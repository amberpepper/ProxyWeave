package monitor

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	mathrand "math/rand"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/proxy"
	"golang.org/x/sync/semaphore"
	"proxyweave/internal/config"
	"proxyweave/internal/geoip"
	storepkg "proxyweave/internal/store"
)

//go:embed assets/index.html
var embeddedFS embed.FS

// Session represents a user session with expiration.
type Session struct {
	Token     string
	CreatedAt time.Time
	ExpiresAt time.Time
}

// NodeManager exposes config node CRUD and reload operations.
type NodeManager interface {
	ListConfigNodes(ctx context.Context) ([]config.NodeConfig, error)
	CreateNode(ctx context.Context, node config.NodeConfig) (config.NodeConfig, error)
	UpdateNode(ctx context.Context, name string, node config.NodeConfig) (config.NodeConfig, error)
	DeleteNode(ctx context.Context, name string) error
	TriggerReload(ctx context.Context) error
}

// Sentinel errors for node operations.
var (
	ErrNodeNotFound = errors.New("节点不存在")
	ErrNodeConflict = errors.New("节点名称或端口已存在")
	ErrInvalidNode  = errors.New("无效的节点配置")
)

// SubscriptionRefresher interface for subscription manager.
type SubscriptionRefresher interface {
	RefreshNow() error
	RefreshSubscription(ctx context.Context, id int64) error
	Status() SubscriptionStatus
	ListSubscriptions(ctx context.Context) ([]storepkg.Subscription, error)
	ListSubscriptionNodes(ctx context.Context, id int64, page int, pageSize int) (storepkg.SubscriptionNodesPage, error)
	CreateSubscription(ctx context.Context, sub storepkg.Subscription) (storepkg.Subscription, error)
	UpdateSubscription(ctx context.Context, sub storepkg.Subscription) (storepkg.Subscription, error)
	DeleteSubscription(ctx context.Context, id int64) error
}

type ProxyTestFunc func(uri string, skipCertVerify bool, probeTarget string) (success bool, latencyMs int64, errMsg string)

type TrafficStatsFunc func() map[string][2]int64

// SubscriptionStatus represents subscription refresh status.
type SubscriptionStatus struct {
	Enabled       bool      `json:"enabled"`
	LastRefresh   time.Time `json:"last_refresh"`
	NextRefresh   time.Time `json:"next_refresh"`
	NodeCount     int       `json:"node_count"`
	LastError     string    `json:"last_error,omitempty"`
	RefreshCount  int       `json:"refresh_count"`
	IsRefreshing  bool      `json:"is_refreshing"`
	NodesModified bool      `json:"nodes_modified"`
}

// Server exposes HTTP endpoints for monitoring.
type Server struct {
	cfg    Config
	cfgMu  sync.RWMutex   // 保护动态配置字段
	cfgSrc *config.Config // 可持久化的配置对象
	mgr    *Manager
	srv    *http.Server
	logger *log.Logger

	// Session management
	sessionMu  sync.RWMutex
	sessions   map[string]*Session
	sessionTTL time.Duration

	// Concurrency control
	probeSem *semaphore.Weighted

	subRefresher  SubscriptionRefresher
	nodeMgr       NodeManager
	proxyTestFn   ProxyTestFunc
	trafficFn     TrafficStatsFunc
	settingsStore storepkg.SettingsStore
}

// NewServer constructs a server; it can be nil when disabled.
func NewServer(cfg Config, mgr *Manager, logger *log.Logger) *Server {
	if !cfg.Enabled || mgr == nil {
		return nil
	}
	if logger == nil {
		logger = log.Default()
	}

	// Calculate max concurrent probes
	maxConcurrentProbes := int64(runtime.NumCPU() * 4)
	if maxConcurrentProbes < 10 {
		maxConcurrentProbes = 10
	}

	s := &Server{
		cfg:        cfg,
		mgr:        mgr,
		logger:     logger,
		sessions:   make(map[string]*Session),
		sessionTTL: 24 * time.Hour,
		probeSem:   semaphore.NewWeighted(maxConcurrentProbes),
	}

	// Start session cleanup goroutine
	go s.cleanupExpiredSessions()

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/auth", s.handleAuth)
	mux.HandleFunc("/api/settings", s.withAuth(s.handleSettings))
	mux.HandleFunc("/api/nodes", s.withAuth(s.handleNodes))
	mux.HandleFunc("/api/nodes/stream", s.withAuth(s.handleNodesStream))
	mux.HandleFunc("/api/nodes/config", s.withAuth(s.handleConfigNodes))
	mux.HandleFunc("/api/nodes/config/", s.withAuth(s.handleConfigNodeItem))
	mux.HandleFunc("/api/nodes/test", s.withAuth(s.handleNodeTest))
	mux.HandleFunc("/api/nodes/probe-all", s.withAuth(s.handleProbeAll))
	mux.HandleFunc("/api/nodes/quality-all", s.withAuth(s.handleQualityAll))
	mux.HandleFunc("/api/nodes/", s.withAuth(s.handleNodeAction))
	mux.HandleFunc("/api/proxy", s.withAuth(s.handleProxyAPI))
	mux.HandleFunc("/api/proxy/", s.withAuth(s.handleProxyAPI))
	mux.HandleFunc("/api/debug", s.withAuth(s.handleDebug))
	mux.HandleFunc("/api/export", s.withAuth(s.handleExport))
	mux.HandleFunc("/api/subscription/status", s.withAuth(s.handleSubscriptionStatus))
	mux.HandleFunc("/api/subscription/refresh", s.withAuth(s.handleSubscriptionRefresh))
	mux.HandleFunc("/api/subscriptions", s.withAuth(s.handleSubscriptions))
	mux.HandleFunc("/api/subscriptions/", s.withAuth(s.handleSubscriptionItem))
	mux.HandleFunc("/api/reload", s.withAuth(s.handleReload))
	mux.HandleFunc("/api/traffic", s.withAuth(s.handleTraffic))
	mux.HandleFunc("/api/connections/status", s.withAuth(s.handleConnectionStatus))
	mux.HandleFunc("/api/connections/stream", s.withAuth(s.handleConnectionStatusStream))
	mux.HandleFunc("/api/logs", s.withAuth(s.handleLogs))
	mux.HandleFunc("/api/logs/stream", s.withAuth(s.handleLogsStream))
	s.srv = &http.Server{Addr: cfg.Listen, Handler: mux}
	return s
}

// SetSubscriptionRefresher sets the subscription refresher for API endpoints.
func (s *Server) SetSubscriptionRefresher(sr SubscriptionRefresher) {
	if s != nil {
		s.subRefresher = sr
	}
}

// SetNodeManager enables config-node CRUD endpoints.
func (s *Server) SetNodeManager(nm NodeManager) {
	if s != nil {
		s.nodeMgr = nm
	}
}

func (s *Server) SetProxyTestFn(fn ProxyTestFunc) {
	if s != nil {
		s.proxyTestFn = fn
	}
}

func (s *Server) SetTrafficStatsFn(fn TrafficStatsFunc) {
	if s != nil {
		s.trafficFn = fn
	}
}

// SetSettingsStore binds the persistent settings store for settings API.
func (s *Server) SetSettingsStore(store storepkg.SettingsStore) {
	if s != nil {
		s.settingsStore = store
	}
}

// SetConfig binds the persistable config object for settings API.
func (s *Server) SetConfig(cfg *config.Config) {
	if s == nil {
		return
	}
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	if cfg != nil && s.cfgSrc != nil {
		if len(cfg.Subscriptions) == 0 && len(s.cfgSrc.Subscriptions) > 0 {
			cfg.Subscriptions = s.cfgSrc.Subscriptions
		}
		if cfg.SubscriptionRefresh.Interval == 0 && s.cfgSrc.SubscriptionRefresh.Interval > 0 {
			cfg.SubscriptionRefresh = s.cfgSrc.SubscriptionRefresh
		}
	}
	s.cfgSrc = cfg
	if cfg != nil {
		s.cfg.ExternalIP = cfg.ExternalIP
		s.cfg.ProbeTarget = cfg.Management.ProbeTarget
		s.cfg.Password = cfg.Management.Password
		s.cfg.APIKey = cfg.Management.APIKey
		s.cfg.SkipCertVerify = cfg.SkipCertVerify
		// Sync proxy credentials based on mode
		if cfg.Mode == "multi-port" || cfg.Mode == "hybrid" {
			s.cfg.ProxyUsername = cfg.MultiPort.Username
			s.cfg.ProxyPassword = cfg.MultiPort.Password
		} else {
			s.cfg.ProxyUsername = cfg.Listener.Username
			s.cfg.ProxyPassword = cfg.Listener.Password
		}
	}
}

// getSettings returns current dynamic settings (thread-safe).
func (s *Server) getSettings() (externalIP, probeTarget string, skipCertVerify bool, logCfg config.LogConfig) {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	logCfg = config.LogConfig{}
	if s.cfgSrc != nil {
		logCfg = s.cfgSrc.Log
	}
	return s.cfg.ExternalIP, s.cfg.ProbeTarget, s.cfg.SkipCertVerify, logCfg
}

// updateSettings updates dynamic settings and persists to config file.
func (s *Server) updateSettings(externalIP, probeTarget string, skipCertVerify bool, logCfg *config.LogConfig, geoipEnabled bool) error {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()

	s.cfg.ExternalIP = externalIP
	s.cfg.ProbeTarget = probeTarget
	s.cfg.SkipCertVerify = skipCertVerify

	if s.cfgSrc == nil {
		return errors.New("配置存储未初始化")
	}

	s.cfgSrc.ExternalIP = externalIP
	s.cfgSrc.Management.ProbeTarget = probeTarget
	s.cfgSrc.SkipCertVerify = skipCertVerify

	// GeoIP settings
	s.cfgSrc.GeoIP.Enabled = geoipEnabled
	if geoipEnabled && s.cfgSrc.GeoIP.DatabasePath == "" {
		s.cfgSrc.GeoIP.DatabasePath = "./GeoLite2-Country.mmdb"
		s.cfgSrc.GeoIP.AutoUpdateEnabled = true
		s.cfgSrc.GeoIP.AutoUpdateInterval = 24 * time.Hour
	}

	if logCfg != nil {
		s.cfgSrc.Log.Output = logCfg.Output
		if logCfg.MaxSize > 0 {
			s.cfgSrc.Log.MaxSize = logCfg.MaxSize
		}
		if logCfg.MaxBackups > 0 {
			s.cfgSrc.Log.MaxBackups = logCfg.MaxBackups
		}
		if logCfg.MaxAge > 0 {
			s.cfgSrc.Log.MaxAge = logCfg.MaxAge
		}
		s.cfgSrc.Log.Compress = logCfg.Compress
	}

	return s.persistSettingsLocked()
}

func (s *Server) persistSettingsLocked() error {
	if s.cfgSrc == nil {
		return errors.New("配置存储未初始化")
	}
	if s.settingsStore != nil {
		if err := s.settingsStore.SaveSettings(context.Background(), s.cfgSrc); err != nil {
			return fmt.Errorf("保存配置失败: %w", err)
		}
		return nil
	}
	if err := s.cfgSrc.SaveSettings(); err != nil {
		return fmt.Errorf("保存配置失败: %w", err)
	}
	return nil
}

// Start launches the HTTP server.
func (s *Server) Start(ctx context.Context) {
	if s == nil || s.srv == nil {
		return
	}
	s.logger.Printf("Starting monitor server on %s", s.cfg.Listen)
	go func() {
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Printf("❌ Monitor server error: %v", err)
		}
	}()
	// Give server a moment to start and check for immediate errors
	time.Sleep(100 * time.Millisecond)
	s.logger.Printf("✅ Monitor server started on http://%s", s.cfg.Listen)

	go func() {
		<-ctx.Done()
		s.Shutdown(context.Background())
	}()
}

// Shutdown stops the server gracefully.
func (s *Server) Shutdown(ctx context.Context) {
	if s == nil || s.srv == nil {
		return
	}
	_ = s.srv.Shutdown(ctx)
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	data, err := embeddedFS.ReadFile("assets/index.html")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, s.buildNodesPayload(r.URL.Query()))
}

func (s *Server) handleNodesStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	send := func() error {
		payload := s.buildNodesPayload(r.URL.Query())
		b, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "event: nodes\ndata: %s\n\n", b); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	if err := send(); err != nil {
		return
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if err := send(); err != nil {
				return
			}
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func healthRate(total, healthy int) float64 {
	if total <= 0 {
		return 0
	}
	return float64(healthy) * 100 / float64(total)
}

func (s *Server) buildNodesPayload(q url.Values) map[string]any {
	page := 1
	if v, err := strconv.Atoi(strings.TrimSpace(q.Get("page"))); err == nil && v > 0 {
		page = v
	}
	pageSize := 20
	if v, err := strconv.Atoi(strings.TrimSpace(q.Get("page_size"))); err == nil && v > 0 {
		if v > 200 {
			v = 200
		}
		pageSize = v
	}
	regionFilter := strings.TrimSpace(strings.ToLower(q.Get("region")))
	if regionFilter == "" {
		regionFilter = "all"
	}

	// 管理面板统计按全部节点计算；表格按分页/筛选返回。
	allNodes := s.mgr.Snapshot()
	totalNodes := len(allNodes)

	// Calculate dashboard statistics.
	regionStats := make(map[string]int)
	regionHealthy := make(map[string]int)
	healthyNodes := 0
	blacklistedNodes := 0
	activeConnections := 0
	uniqueExitIPs := make(map[string]struct{})
	type latencyBucket struct {
		Label string `json:"label"`
		Count int    `json:"count"`
	}
	latencyBuckets := []latencyBucket{
		{Label: "<200ms"},
		{Label: "200-500ms"},
		{Label: "500ms-1s"},
		{Label: "1-2s"},
		{Label: ">2s"},
		{Label: "未测试"},
	}
	for _, snap := range allNodes {
		region := snap.Region
		if region == "" {
			region = "other"
		}
		regionStats[region]++
		activeConnections += int(snap.ActiveConnections)
		if ip := strings.TrimSpace(snap.ExitIP); ip != "" {
			uniqueExitIPs[ip] = struct{}{}
		}
		latency := snap.LastLatencyMs
		switch {
		case latency <= 0:
			latencyBuckets[5].Count++
		case latency < 200:
			latencyBuckets[0].Count++
		case latency < 500:
			latencyBuckets[1].Count++
		case latency < 1000:
			latencyBuckets[2].Count++
		case latency < 2000:
			latencyBuckets[3].Count++
		default:
			latencyBuckets[4].Count++
		}
		// Count healthy nodes per region
		if snap.InitialCheckDone && snap.Available && !snap.Blacklisted {
			regionHealthy[region]++
			healthyNodes++
		}
		if snap.Blacklisted || (snap.InitialCheckDone && !snap.Available) {
			blacklistedNodes++
		}
	}

	filteredNodes := make([]Snapshot, 0, len(allNodes))
	for _, snap := range allNodes {
		region := snap.Region
		if region == "" {
			region = "other"
		}
		if regionFilter != "all" && region != regionFilter {
			continue
		}
		filteredNodes = append(filteredNodes, snap)
	}

	filteredTotal := len(filteredNodes)
	totalPages := 0
	if filteredTotal > 0 {
		totalPages = (filteredTotal + pageSize - 1) / pageSize
	}
	if totalPages == 0 {
		page = 1
	} else if page > totalPages {
		page = totalPages
	}
	start := 0
	end := 0
	if filteredTotal > 0 {
		start = (page - 1) * pageSize
		if start < 0 {
			start = 0
		}
		if start > filteredTotal {
			start = filteredTotal
		}
		end = start + pageSize
		if end > filteredTotal {
			end = filteredTotal
		}
	}
	pageNodes := []Snapshot{}
	if filteredTotal > 0 && start < end {
		pageNodes = filteredNodes[start:end]
	}

	fastestCandidates := make([]Snapshot, 0, len(allNodes))
	for _, snap := range allNodes {
		if snap.LastLatencyMs > 0 && !snap.Blacklisted {
			fastestCandidates = append(fastestCandidates, snap)
		}
	}
	sort.Slice(fastestCandidates, func(i, j int) bool {
		return fastestCandidates[i].LastLatencyMs < fastestCandidates[j].LastLatencyMs
	})
	if len(fastestCandidates) > 10 {
		fastestCandidates = fastestCandidates[:10]
	}

	return map[string]any{
		"nodes":              pageNodes,
		"total_nodes":        totalNodes,
		"filtered_total":     filteredTotal,
		"page":               page,
		"page_size":          pageSize,
		"total_pages":        totalPages,
		"region_filter":      regionFilter,
		"healthy_nodes":      healthyNodes,
		"health_rate":        healthRate(totalNodes, healthyNodes),
		"blacklisted_nodes":  blacklistedNodes,
		"active_connections": activeConnections,
		"unique_exit_ips":    len(uniqueExitIPs),
		"latency_histogram":  latencyBuckets,
		"region_stats":       regionStats,
		"region_healthy":     regionHealthy,
		"fastest_nodes":      fastestCandidates,
	}
}

func (s *Server) handleDebug(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	snapshots := s.mgr.Snapshot()
	var totalCalls, totalSuccess int64
	debugNodes := make([]map[string]any, 0, len(snapshots))
	for _, snap := range snapshots {
		totalCalls += snap.SuccessCount + int64(snap.FailureCount)
		totalSuccess += snap.SuccessCount
		debugNodes = append(debugNodes, map[string]any{
			"tag":                snap.Tag,
			"name":               snap.Name,
			"mode":               snap.Mode,
			"port":               snap.Port,
			"failure_count":      snap.FailureCount,
			"success_count":      snap.SuccessCount,
			"active_connections": snap.ActiveConnections,
			"last_latency_ms":    snap.LastLatencyMs,
			"last_success":       snap.LastSuccess,
			"last_failure":       snap.LastFailure,
			"last_error":         snap.LastError,
			"blacklisted":        snap.Blacklisted,
			"timeline":           snap.Timeline,
		})
	}
	var successRate float64
	if totalCalls > 0 {
		successRate = float64(totalSuccess) / float64(totalCalls) * 100
	}
	writeJSON(w, map[string]any{
		"nodes":         debugNodes,
		"total_calls":   totalCalls,
		"total_success": totalSuccess,
		"success_rate":  successRate,
	})
}

func (s *Server) handleNodeAction(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/nodes/"), "/")
	if len(parts) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	tag := parts[0]
	if tag == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}
	switch action {
	case "probe":
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		latency, err := s.mgr.Probe(ctx, tag)
		if err != nil {
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		latencyMs := latency.Milliseconds()
		if latencyMs == 0 && latency > 0 {
			latencyMs = 1 // Round up sub-millisecond latencies to 1ms
		}
		writeJSON(w, map[string]any{"message": "探测成功", "latency_ms": latencyMs})
	case "quality":
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		info, err := s.mgr.Quality(ctx, tag, true)
		if err != nil {
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"message": "质量检查成功", "quality": info})
	case "release":
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if err := s.mgr.Release(tag); err != nil {
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"message": "已解除拉黑"})
	case "blacklist":
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Duration string `json:"duration"` // e.g. "1h", "24h", "30m"
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Duration == "" {
			req.Duration = "24h"
		}
		duration, err := time.ParseDuration(req.Duration)
		if err != nil || duration <= 0 {
			duration = 24 * time.Hour
		}
		if err := s.mgr.ManualBlacklist(tag, duration); err != nil {
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"message": fmt.Sprintf("已拉黑 %s", duration)})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *Server) handleNodeTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		URI string `json:"uri"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "请求格式错误"})
		return
	}

	uri := strings.TrimSpace(req.URI)
	if uri == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "URI 不能为空"})
		return
	}

	success, latencyMs, errMsg := s.testProxyURI(uri)
	if !success {
		writeJSON(w, map[string]any{"error": errMsg})
		return
	}
	writeJSON(w, map[string]any{
		"message":    "测试成功",
		"latency_ms": latencyMs,
	})
}

func (s *Server) testProxyURI(rawURI string) (bool, int64, string) {
	if s.proxyTestFn != nil {
		_, probeTarget, skipCertVerify, _ := s.getSettings()
		return s.proxyTestFn(rawURI, skipCertVerify, probeTarget)
	}

	proxyURL, err := url.Parse(rawURI)
	if err != nil {
		return false, 0, "代理 URI 格式错误"
	}
	scheme := strings.ToLower(strings.TrimSpace(proxyURL.Scheme))
	if scheme == "socks" {
		scheme = "socks5"
	}
	if scheme != "http" && scheme != "https" && scheme != "socks5" {
		return false, 0, "当前仅支持测试 HTTP/HTTPS/SOCKS5 代理"
	}

	_, probeTarget, skipCertVerify, _ := s.getSettings()
	target := strings.TrimSpace(probeTarget)
	if target == "" {
		target = "http://cp.cloudflare.com/generate_204"
	}
	if !strings.Contains(target, "://") {
		target = "http://" + target
	}
	targetURL, err := url.Parse(target)
	if err != nil {
		return false, 0, "健康检查目标格式错误"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: skipCertVerify, //nolint:gosec
		},
	}

	if scheme == "socks5" {
		dialer, err := proxy.FromURL(proxyURL, proxy.Direct)
		if err != nil {
			return false, 0, err.Error()
		}
		if contextDialer, ok := dialer.(proxy.ContextDialer); ok {
			transport.DialContext = contextDialer.DialContext
		} else {
			transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				type result struct {
					conn net.Conn
					err  error
				}
				ch := make(chan result, 1)
				go func() {
					conn, dialErr := dialer.Dial(network, addr)
					ch <- result{conn: conn, err: dialErr}
				}()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case res := <-ch:
					return res.conn, res.err
				}
			}
		}
	} else {
		transport.Proxy = http.ProxyURL(proxyURL)
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   15 * time.Second,
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL.String(), nil)
	if err != nil {
		return false, 0, err.Error()
	}

	start := time.Now()
	response, err := client.Do(request)
	if err != nil {
		return false, 0, err.Error()
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 400 {
		return false, 0, fmt.Sprintf("探测目标返回状态码 %d", response.StatusCode)
	}

	latencyMs := time.Since(start).Milliseconds()
	if latencyMs <= 0 {
		latencyMs = 1
	}
	return true, latencyMs, ""
}

// handleQualityAll checks exit-IP/ASN quality for all nodes and returns results via SSE.
func (s *Server) handleQualityAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	snapshots := s.mgr.Snapshot()
	total := len(snapshots)
	startData, _ := json.Marshal(map[string]any{"type": "start", "total": total})
	fmt.Fprintf(w, "data: %s\n\n", startData)
	flusher.Flush()
	if total == 0 {
		completeData, _ := json.Marshal(map[string]any{"type": "complete", "total": 0, "success": 0, "failed": 0})
		fmt.Fprintf(w, "data: %s\n\n", completeData)
		flusher.Flush()
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	type qualityResult struct {
		tag  string
		name string
		info QualityInfo
		err  string
	}
	results := make(chan qualityResult, total)
	var wg sync.WaitGroup

	for _, snap := range snapshots {
		wg.Add(1)
		go func(snap Snapshot) {
			defer wg.Done()
			if err := s.probeSem.Acquire(ctx, 1); err != nil {
				results <- qualityResult{tag: snap.Tag, name: snap.Name, err: "quality check cancelled: " + err.Error()}
				return
			}
			defer s.probeSem.Release(1)
			qualityCtx, qualityCancel := context.WithTimeout(ctx, 25*time.Second)
			defer qualityCancel()
			info, err := s.mgr.Quality(qualityCtx, snap.Tag, true)
			if err != nil {
				results <- qualityResult{tag: snap.Tag, name: snap.Name, err: err.Error()}
				return
			}
			results <- qualityResult{tag: snap.Tag, name: snap.Name, info: info}
		}(snap)
	}
	go func() { wg.Wait(); close(results) }()

	successCount, failedCount, count := 0, 0, 0
	for result := range results {
		count++
		if result.err != "" {
			failedCount++
		} else {
			successCount++
		}
		eventPayload := map[string]any{
			"type": "progress", "tag": result.tag, "name": result.name,
			"status": map[bool]string{true: "error", false: "success"}[result.err != ""],
			"error":  result.err, "quality": result.info,
			"current": count, "total": total, "progress": float64(count) / float64(total) * 100,
		}
		data, _ := json.Marshal(eventPayload)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}
	completeData, _ := json.Marshal(map[string]any{"type": "complete", "total": total, "success": successCount, "failed": failedCount})
	fmt.Fprintf(w, "data: %s\n\n", completeData)
	flusher.Flush()
}

// handleProbeAll probes all nodes in batches and returns results via SSE
func (s *Server) handleProbeAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	s.streamProbeSnapshots(w, r, s.mgr.Snapshot())
}

func (s *Server) streamProbeSnapshots(w http.ResponseWriter, r *http.Request, snapshots []Snapshot) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	total := len(snapshots)
	startData, _ := json.Marshal(map[string]any{"type": "start", "total": total})
	fmt.Fprintf(w, "data: %s\n\n", startData)
	flusher.Flush()
	if total == 0 {
		completeData, _ := json.Marshal(map[string]any{"type": "complete", "total": 0, "success": 0, "failed": 0})
		fmt.Fprintf(w, "data: %s\n\n", completeData)
		flusher.Flush()
		return
	}

	ctx := r.Context()
	type probeResult struct {
		tag     string
		name    string
		latency int64
		err     string
		status  string
	}
	workerCount := runtime.NumCPU() * 4
	if workerCount < 10 {
		workerCount = 10
	}
	if workerCount > total {
		workerCount = total
	}
	tasks := make(chan Snapshot)
	results := make(chan probeResult, workerCount*2)
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for snap := range tasks {
				if err := s.probeSem.Acquire(ctx, 1); err != nil {
					results <- probeResult{tag: snap.Tag, name: snap.Name, latency: -1, status: "cancelled", err: "probe cancelled: " + err.Error()}
					continue
				}

				probeCtx, probeCancel := context.WithTimeout(ctx, 10*time.Second)
				latency, err := s.mgr.Probe(probeCtx, snap.Tag)
				probeCancel()
				s.probeSem.Release(1)

				if err != nil {
					status := "error"
					if errors.Is(err, context.Canceled) {
						status = "cancelled"
					}
					results <- probeResult{tag: snap.Tag, name: snap.Name, latency: -1, status: status, err: err.Error()}
					continue
				}
				latencyMs := latency.Milliseconds()
				if latencyMs == 0 && latency > 0 {
					latencyMs = 1
				}
				results <- probeResult{tag: snap.Tag, name: snap.Name, latency: latencyMs, status: "success"}
			}
		}()
	}
	go func() {
		defer close(tasks)
		for _, snap := range snapshots {
			select {
			case <-ctx.Done():
				return
			case tasks <- snap:
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	successCount := 0
	failedCount := 0
	cancelledCount := 0
	count := 0
	for result := range results {
		count++
		switch result.status {
		case "success":
			successCount++
		case "cancelled":
			cancelledCount++
			failedCount++
		default:
			failedCount++
		}
		eventData, _ := json.Marshal(map[string]any{
			"type":     "progress",
			"tag":      result.tag,
			"name":     result.name,
			"latency":  result.latency,
			"status":   result.status,
			"error":    result.err,
			"current":  count,
			"total":    total,
			"progress": float64(count) / float64(total) * 100,
		})
		fmt.Fprintf(w, "data: %s\n\n", eventData)
		flusher.Flush()
	}

	completeData, _ := json.Marshal(map[string]any{
		"type":      "complete",
		"total":     total,
		"success":   successCount,
		"failed":    failedCount,
		"cancelled": cancelledCount,
	})
	fmt.Fprintf(w, "data: %s\n\n", completeData)
	flusher.Flush()
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func (s *Server) hasManagementAuth() bool {
	return s.authKey() != ""
}

func (s *Server) authKey() string {
	if strings.TrimSpace(s.cfg.APIKey) != "" {
		return strings.TrimSpace(s.cfg.APIKey)
	}
	return strings.TrimSpace(s.cfg.Password)
}

func (s *Server) validAPIKey(key string) bool {
	key = strings.TrimSpace(key)
	authKey := s.authKey()
	return key != "" && authKey != "" && secureCompareStrings(key, authKey)
}

func (s *Server) validAPIKeyFromRequest(r *http.Request) bool {
	return s.validAPIKey(r.URL.Query().Get("apikey"))
}

// withAuth 认证中间件，如果配置了 api_key（兼容旧 password）则需要验证
func (s *Server) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 如果没有配置密码和 API key，直接放行
		if !s.hasManagementAuth() {
			next(w, r)
			return
		}

		// 检查 API key（适合程序/API 调用）
		if s.validAPIKeyFromRequest(r) {
			next(w, r)
			return
		}

		// 检查 Cookie 中的 session token
		cookie, err := r.Cookie("session_token")
		if err == nil && s.validateSession(cookie.Value) {
			next(w, r)
			return
		}

		// 未授权
		w.WriteHeader(http.StatusUnauthorized)
		writeJSON(w, map[string]any{"error": "未授权，请先登录"})
	}
}

// handleAuth 处理登录认证
func (s *Server) handleAuth(w http.ResponseWriter, r *http.Request) {
	// 如果没有配置鉴权 key，直接返回成功（不需要 token）
	if !s.hasManagementAuth() {
		writeJSON(w, map[string]any{"message": "无需认证", "no_password": true})
		return
	}

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Password string `json:"password"`
		APIKey   string `json:"api_key"`
		Key      string `json:"key"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "请求格式错误"})
		return
	}

	keyCandidate := req.APIKey
	if keyCandidate == "" {
		keyCandidate = req.Key
	}
	if keyCandidate == "" {
		keyCandidate = req.Password
	}
	keyOK := s.validAPIKey(keyCandidate)

	if !keyOK {
		time.Sleep(time.Duration(100+mathrand.Intn(200)) * time.Millisecond)
		w.WriteHeader(http.StatusUnauthorized)
		writeJSON(w, map[string]any{"error": "API key 错误"})
		return
	}

	// 创建新会话
	session, err := s.createSession()
	if err != nil {
		s.logger.Printf("Failed to create session: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(w, map[string]any{"error": "服务器错误"})
		return
	}

	// 设置 HttpOnly Cookie
	http.SetCookie(w, &http.Cookie{
		Name:     "session_token",
		Value:    session.Token,
		Path:     "/",
		HttpOnly: true,
		Secure:   false, // 生产环境应启用 HTTPS 并设为 true
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(s.sessionTTL.Seconds()),
	})

	writeJSON(w, map[string]any{
		"message": "登录成功",
		"token":   session.Token,
	})
}

// handleExport 导出所有可用代理池节点的代理 URI，每行一个。
// query 参数:
//   - scheme=http   (默认)
//   - scheme=socks5
//   - scheme=all    (同时导出 HTTP 和 SOCKS5)
//
// 在 pool/hybrid 模式下，还会导出 Pool 代理池入口和 GeoIP 分区路由入口。
func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	scheme := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("scheme")))
	if scheme == "" {
		scheme = "http"
	}
	if scheme != "http" && scheme != "socks5" && scheme != "all" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "invalid scheme, use http/socks5/all"})
		return
	}

	// 只导出初始检查通过的可用节点
	snapshots := s.mgr.SnapshotFiltered(true)
	var lines []string

	seen := make(map[string]bool)

	// 读取运行模式和监听配置
	s.cfgMu.RLock()
	mode := ""
	var listenerCfg config.ListenerConfig
	var geoipCfg config.GeoIPConfig
	if s.cfgSrc != nil {
		mode = s.cfgSrc.Mode
		listenerCfg = s.cfgSrc.Listener
		geoipCfg = s.cfgSrc.GeoIP
	}
	s.cfgMu.RUnlock()

	// Pool 代理池入口（pool 或 hybrid 模式）
	if (mode == "pool" || mode == "hybrid") && listenerCfg.Port > 0 {
		poolAddr := listenerCfg.Address
		if poolAddr == "" || poolAddr == "0.0.0.0" || poolAddr == "::" {
			if extIP, _, _, _ := s.getSettings(); extIP != "" {
				poolAddr = extIP
			}
		}
		var poolAuth string
		if listenerCfg.Username != "" && listenerCfg.Password != "" {
			poolAuth = fmt.Sprintf("%s:%s@", listenerCfg.Username, listenerCfg.Password)
		}
		lines = append(lines, "# Pool 代理池入口")
		poolHTTP := fmt.Sprintf("http://%s%s:%d", poolAuth, poolAddr, listenerCfg.Port)
		poolSocks := fmt.Sprintf("socks5://%s%s:%d", poolAuth, poolAddr, listenerCfg.Port)
		switch scheme {
		case "http":
			lines = append(lines, poolHTTP)
			seen[poolHTTP] = true
		case "socks5":
			lines = append(lines, poolSocks)
			seen[poolSocks] = true
		case "all":
			lines = append(lines, poolHTTP)
			seen[poolHTTP] = true
			lines = append(lines, poolSocks)
			seen[poolSocks] = true
		}
	}

	// GeoIP 分区路由入口
	if geoipCfg.Enabled && geoipCfg.Port > 0 {
		geoAddr := geoipCfg.Listen
		if geoAddr == "" || geoAddr == "0.0.0.0" || geoAddr == "::" {
			if extIP, _, _, _ := s.getSettings(); extIP != "" {
				geoAddr = extIP
			}
		}
		var geoAuth string
		if listenerCfg.Username != "" && listenerCfg.Password != "" {
			geoAuth = fmt.Sprintf("%s:%s@", listenerCfg.Username, listenerCfg.Password)
		}
		regionSet := make(map[string]struct{})
		for _, snap := range snapshots {
			region := strings.TrimSpace(strings.ToLower(snap.Region))
			if region == "" || region == geoip.RegionOther {
				continue
			}
			regionSet[region] = struct{}{}
		}
		var pathParts []string
		for _, r := range geoip.SortedRegionCodes(regionSet) {
			pathParts = append(pathParts, fmt.Sprintf("/%s/", r))
		}
		if len(pathParts) == 0 {
			pathParts = append(pathParts, "/other/")
		}
		lines = append(lines, fmt.Sprintf("# GeoIP 分区路由入口 (支持路径: %s)", strings.Join(pathParts, " ")))
		// GeoIP 路由仅支持 HTTP
		geoURI := fmt.Sprintf("http://%s%s:%d", geoAuth, geoAddr, geoipCfg.Port)
		if !seen[geoURI] {
			lines = append(lines, geoURI)
			seen[geoURI] = true
		}
	}

	// Multi-port 独立节点
	if len(snapshots) > 0 && (mode == "hybrid" || mode == "multi-port" || mode == "") {
		lines = append(lines, "# Multi-port 独立节点")
	}
	for _, snap := range snapshots {
		// 只导出有监听地址和端口的节点
		if snap.ListenAddress == "" || snap.Port == 0 {
			continue
		}

		listenAddr := snap.ListenAddress
		if listenAddr == "0.0.0.0" || listenAddr == "::" {
			if extIP, _, _, _ := s.getSettings(); extIP != "" {
				listenAddr = extIP
			}
		}

		var authPart string
		if s.cfg.ProxyUsername != "" && s.cfg.ProxyPassword != "" {
			authPart = fmt.Sprintf("%s:%s@", s.cfg.ProxyUsername, s.cfg.ProxyPassword)
		}
		httpURI := fmt.Sprintf("http://%s%s:%d", authPart, listenAddr, snap.Port)
		socksURI := fmt.Sprintf("socks5://%s%s:%d", authPart, listenAddr, snap.Port)

		switch scheme {
		case "http":
			if !seen[httpURI] {
				lines = append(lines, httpURI)
				seen[httpURI] = true
			}
		case "socks5":
			if !seen[socksURI] {
				lines = append(lines, socksURI)
				seen[socksURI] = true
			}
		case "all":
			if !seen[httpURI] {
				lines = append(lines, httpURI)
				seen[httpURI] = true
			}
			if !seen[socksURI] {
				lines = append(lines, socksURI)
				seen[socksURI] = true
			}
		}
	}

	// 返回纯文本，每行一个 URI
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	filename := "proxy_pool.txt"
	if scheme == "socks5" {
		filename = "proxy_pool_socks5.txt"
	} else if scheme == "all" {
		filename = "proxy_pool_all.txt"
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
	_, _ = w.Write([]byte(strings.Join(lines, "\n")))
}

// handleProxyAPI returns a directly usable proxy URI for automation.
//
// Supported forms:
//
//	GET /api/proxy/random?scheme=http|socks5|all&format=text|json
//	GET /api/proxy/fixed?name=node-1&scheme=http
//	GET /api/proxy?mode=random|fixed|pool&name=...&tag=...&port=...
//
// Filters for random/fixed runtime nodes:
//
//	country=us,jp,cn...    proxy_type=isp|datacenter|mobile|unknown
//	healthy=1              default true for random, false for fixed
func (s *Server) handleProxyAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	mode := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/proxy"), "/")
	if mode == "" {
		mode = strings.ToLower(strings.TrimSpace(q.Get("mode")))
	}
	if mode == "" {
		mode = "random"
	}
	if mode != "random" && mode != "fixed" && mode != "pool" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "invalid mode, use random/fixed/pool"})
		return
	}

	scheme := strings.ToLower(strings.TrimSpace(q.Get("scheme")))
	if scheme == "" {
		scheme = "http"
	}
	if scheme != "http" && scheme != "socks5" && scheme != "all" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "invalid scheme, use http/socks5/all"})
		return
	}

	format := strings.ToLower(strings.TrimSpace(q.Get("format")))
	if format == "" {
		format = "text"
	}
	if format != "text" && format != "json" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "invalid format, use text/json"})
		return
	}

	if mode == "pool" {
		uris, resp, err := s.poolProxyURIs(r, scheme)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		s.writeProxyAPIResponse(w, format, uris, resp)
		return
	}

	healthyDefault := mode == "random"
	healthy := parseBoolDefault(q.Get("healthy"), healthyDefault)
	country := strings.ToLower(strings.TrimSpace(q.Get("country")))
	region := strings.ToLower(strings.TrimSpace(q.Get("region")))
	proxyType := strings.ToLower(strings.TrimSpace(q.Get("proxy_type")))
	name := strings.TrimSpace(q.Get("name"))
	tag := strings.TrimSpace(q.Get("tag"))
	portStr := strings.TrimSpace(q.Get("port"))

	candidates := s.proxyCandidates(healthy, country, region, proxyType, name, tag, portStr)
	if len(candidates) == 0 {
		w.WriteHeader(http.StatusNotFound)
		writeJSON(w, map[string]any{"error": "no matching runtime proxy, reload core or relax filters"})
		return
	}

	var snap Snapshot
	if mode == "random" {
		idx := secureRandIndex(len(candidates))
		snap = candidates[idx]
	} else {
		if name == "" && tag == "" && portStr == "" {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": "fixed mode requires name/tag/port"})
			return
		}
		snap = candidates[0]
	}

	uris := s.snapshotProxyURIs(r, snap, scheme)
	if len(uris) == 0 {
		w.WriteHeader(http.StatusNotFound)
		writeJSON(w, map[string]any{"error": "selected node has no listen address/port, reload core or enable multi-port/hybrid"})
		return
	}
	resp := map[string]any{
		"mode":    mode,
		"scheme":  scheme,
		"proxy":   firstProxyURI(uris),
		"proxies": uris,
		"node":    snap,
	}
	s.writeProxyAPIResponse(w, format, uris, resp)
}

func splitCSVFilter(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.ToLower(strings.TrimSpace(part))
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func matchesAnyFilter(value string, filters []string) bool {
	if len(filters) == 0 {
		return true
	}
	value = strings.ToLower(strings.TrimSpace(value))
	for _, item := range filters {
		if value == item {
			return true
		}
	}
	return false
}

func (s *Server) proxyCandidates(healthy bool, country, region, proxyType, name, tag, portStr string) []Snapshot {
	snaps := s.mgr.SnapshotFiltered(healthy)
	var port uint64
	if portStr != "" {
		port, _ = strconv.ParseUint(portStr, 10, 16)
	}
	countryFilters := splitCSVFilter(country)
	regionFilters := splitCSVFilter(region)
	out := make([]Snapshot, 0, len(snaps))
	for _, snap := range snaps {
		if snap.ListenAddress == "" || snap.Port == 0 {
			continue
		}
		if len(countryFilters) > 0 && !matchesAnyFilter(snap.CountryCode, countryFilters) {
			continue
		}
		if len(regionFilters) > 0 && !matchesAnyFilter(snap.Region, regionFilters) {
			continue
		}
		if proxyType != "" && proxyType != "all" && strings.ToLower(snap.ProxyType) != proxyType {
			continue
		}
		if name != "" && snap.Name != name {
			continue
		}
		if tag != "" && snap.Tag != tag {
			continue
		}
		if portStr != "" && uint16(port) != snap.Port {
			continue
		}
		out = append(out, snap)
	}
	return out
}

func (s *Server) poolProxyURIs(r *http.Request, scheme string) ([]string, map[string]any, error) {
	s.cfgMu.RLock()
	var listenerCfg config.ListenerConfig
	mode := ""
	if s.cfgSrc != nil {
		mode = s.cfgSrc.Mode
		listenerCfg = s.cfgSrc.Listener
	}
	s.cfgMu.RUnlock()
	if mode != "pool" && mode != "hybrid" {
		return nil, nil, fmt.Errorf("pool entry is only available in pool/hybrid mode")
	}
	if listenerCfg.Port == 0 {
		return nil, nil, fmt.Errorf("pool listener port is not configured")
	}
	addr := s.publicProxyAddress(r, listenerCfg.Address)
	uris := buildProxyURIs(scheme, addr, listenerCfg.Port, listenerCfg.Username, listenerCfg.Password)
	resp := map[string]any{
		"mode":    "pool",
		"scheme":  scheme,
		"proxy":   firstProxyURI(uris),
		"proxies": uris,
		"pool": map[string]any{
			"address": addr,
			"port":    listenerCfg.Port,
		},
	}
	return uris, resp, nil
}

func (s *Server) snapshotProxyURIs(r *http.Request, snap Snapshot, scheme string) []string {
	addr := s.publicProxyAddress(r, snap.ListenAddress)
	return buildProxyURIs(scheme, addr, snap.Port, s.cfg.ProxyUsername, s.cfg.ProxyPassword)
}

func buildProxyURIs(scheme, addr string, port uint16, username, password string) []string {
	if addr == "" || port == 0 {
		return nil
	}
	mk := func(sc string) string {
		u := url.URL{Scheme: sc, Host: net.JoinHostPort(addr, strconv.Itoa(int(port)))}
		if username != "" || password != "" {
			u.User = url.UserPassword(username, password)
		}
		return u.String()
	}
	switch scheme {
	case "all":
		return []string{mk("http"), mk("socks5")}
	case "socks5":
		return []string{mk("socks5")}
	default:
		return []string{mk("http")}
	}
}

func (s *Server) publicProxyAddress(r *http.Request, addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" || addr == "0.0.0.0" || addr == "::" || addr == "[::]" {
		if extIP, _, _, _ := s.getSettings(); strings.TrimSpace(extIP) != "" {
			return strings.Trim(strings.TrimSpace(extIP), "[]")
		}
		host := strings.TrimSpace(r.Host)
		if h, _, err := net.SplitHostPort(host); err == nil && h != "" {
			return strings.Trim(h, "[]")
		}
		if i := strings.LastIndex(host, ":"); i > 0 && !strings.Contains(host[:i], ":") {
			host = host[:i]
		}
		host = strings.Trim(host, "[]")
		if host != "" {
			return host
		}
	}
	return strings.Trim(addr, "[]")
}

func (s *Server) writeProxyAPIResponse(w http.ResponseWriter, format string, uris []string, resp map[string]any) {
	if format == "json" {
		writeJSON(w, resp)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(strings.Join(uris, "\n")))
}

func firstProxyURI(uris []string) string {
	if len(uris) == 0 {
		return ""
	}
	return uris[0]
}

func parseBoolDefault(raw string, def bool) bool {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return def
	}
	switch raw {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

func secureRandIndex(n int) int {
	if n <= 1 {
		return 0
	}
	v, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		return int(time.Now().UnixNano() % int64(n))
	}
	return int(v.Int64())
}

// handleSettings handles GET/PUT for dynamic settings (external_ip, probe_target, skip_cert_verify, log).
func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		extIP, probeTarget, skipCertVerify, logCfg := s.getSettings()

		// Read full config for extended fields
		s.cfgMu.RLock()
		cfg := s.cfgSrc
		s.cfgMu.RUnlock()

		resp := map[string]any{
			"external_ip":      extIP,
			"probe_target":     probeTarget,
			"skip_cert_verify": skipCertVerify,
			"log": map[string]any{
				"output":      logCfg.Output,
				"file":        logCfg.File,
				"max_size":    logCfg.MaxSize,
				"max_backups": logCfg.MaxBackups,
				"max_age":     logCfg.MaxAge,
				"compress":    logCfg.Compress,
			},
			"geoip": map[string]any{
				"enabled":              false,
				"database_path":        "",
				"listen":               "",
				"port":                 0,
				"auto_update_enabled":  false,
				"auto_update_interval": "",
			},
		}
		if cfg != nil {
			resp["mode"] = cfg.Mode
			resp["listener"] = map[string]any{
				"address":  cfg.Listener.Address,
				"port":     cfg.Listener.Port,
				"username": cfg.Listener.Username,
				"password": cfg.Listener.Password,
			}
			resp["multi_port"] = map[string]any{
				"address":   cfg.MultiPort.Address,
				"base_port": cfg.MultiPort.BasePort,
				"username":  cfg.MultiPort.Username,
				"password":  cfg.MultiPort.Password,
			}
			resp["pool"] = map[string]any{
				"mode":               cfg.Pool.Mode,
				"failure_threshold":  cfg.Pool.FailureThreshold,
				"blacklist_duration": cfg.Pool.BlacklistDuration.String(),
			}
			qualityEnabled := true
			if cfg.Management.QualityEnabled != nil {
				qualityEnabled = *cfg.Management.QualityEnabled
			}
			resp["management"] = map[string]any{
				"listen":                cfg.Management.Listen,
				"health_check_interval": cfg.Management.HealthCheckInterval.String(),
				"api_key":               cfg.Management.APIKey,
				"quality_enabled":       qualityEnabled,
				"quality_provider":      cfg.Management.QualityProvider,
				"quality_api_key":       cfg.Management.QualityAPIKey,
				"quality_cache_ttl":     cfg.Management.QualityCacheTTL.String(),
			}
			resp["geoip"] = map[string]any{
				"enabled":              cfg.GeoIP.Enabled,
				"database_path":        cfg.GeoIP.DatabasePath,
				"listen":               cfg.GeoIP.Listen,
				"port":                 cfg.GeoIP.Port,
				"auto_update_enabled":  cfg.GeoIP.AutoUpdateEnabled,
				"auto_update_interval": cfg.GeoIP.AutoUpdateInterval.String(),
			}
		}
		writeJSON(w, resp)
	case http.MethodPut:
		var req struct {
			ExternalIP     string `json:"external_ip"`
			ProbeTarget    string `json:"probe_target"`
			SkipCertVerify bool   `json:"skip_cert_verify"`
			Mode           string `json:"mode,omitempty"`
			Listener       *struct {
				Address  string `json:"address"`
				Port     uint16 `json:"port"`
				Username string `json:"username"`
				Password string `json:"password"`
			} `json:"listener,omitempty"`
			MultiPort *struct {
				Address  string `json:"address"`
				BasePort uint16 `json:"base_port"`
				Username string `json:"username"`
				Password string `json:"password"`
			} `json:"multi_port,omitempty"`
			Pool *struct {
				Mode              string `json:"mode"`
				FailureThreshold  int    `json:"failure_threshold"`
				BlacklistDuration string `json:"blacklist_duration"`
			} `json:"pool,omitempty"`
			Management *struct {
				Listen              string `json:"listen"`
				HealthCheckInterval string `json:"health_check_interval"`
				APIKey              string `json:"api_key"`
				QualityEnabled      bool   `json:"quality_enabled"`
				QualityProvider     string `json:"quality_provider"`
				QualityAPIKey       string `json:"quality_api_key"`
				QualityCacheTTL     string `json:"quality_cache_ttl"`
			} `json:"management,omitempty"`
			Log *struct {
				Output     string `json:"output"`
				MaxSize    int    `json:"max_size"`
				MaxBackups int    `json:"max_backups"`
				MaxAge     int    `json:"max_age"`
				Compress   bool   `json:"compress"`
			} `json:"log"`
			GeoIP *struct {
				Enabled            bool   `json:"enabled"`
				DatabasePath       string `json:"database_path"`
				Listen             string `json:"listen"`
				Port               uint16 `json:"port"`
				AutoUpdateEnabled  bool   `json:"auto_update_enabled"`
				AutoUpdateInterval string `json:"auto_update_interval"`
			} `json:"geoip"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": "请求格式错误"})
			return
		}

		extIP := strings.TrimSpace(req.ExternalIP)
		probeTarget := strings.TrimSpace(req.ProbeTarget)

		var logCfg *config.LogConfig
		if req.Log != nil {
			logCfg = &config.LogConfig{
				Output:     req.Log.Output,
				MaxSize:    req.Log.MaxSize,
				MaxBackups: req.Log.MaxBackups,
				MaxAge:     req.Log.MaxAge,
				Compress:   req.Log.Compress,
			}
		}

		if err := s.updateSettings(extIP, probeTarget, req.SkipCertVerify, logCfg, req.GeoIP != nil && req.GeoIP.Enabled); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}

		// Update extended settings
		s.cfgMu.Lock()
		if s.cfgSrc != nil {
			if req.Mode != "" {
				s.cfgSrc.Mode = req.Mode
			}
			if req.Listener != nil {
				s.cfgSrc.Listener.Address = req.Listener.Address
				s.cfgSrc.Listener.Port = req.Listener.Port
				s.cfgSrc.Listener.Username = req.Listener.Username
				s.cfgSrc.Listener.Password = req.Listener.Password
			}
			if req.MultiPort != nil {
				s.cfgSrc.MultiPort.Address = req.MultiPort.Address
				s.cfgSrc.MultiPort.BasePort = req.MultiPort.BasePort
				s.cfgSrc.MultiPort.Username = req.MultiPort.Username
				s.cfgSrc.MultiPort.Password = req.MultiPort.Password
			}
			if req.Pool != nil {
				mode := strings.ToLower(strings.TrimSpace(req.Pool.Mode))
				switch mode {
				case "random", "sequential", "balance":
					s.cfgSrc.Pool.Mode = mode
				case "latency":
					// 已废弃：保存时自动回退到 random。
					s.cfgSrc.Pool.Mode = "random"
				case "":
					// keep current
				default:
					s.cfgSrc.Pool.Mode = "sequential"
				}
				s.cfgSrc.Pool.FailureThreshold = req.Pool.FailureThreshold
				if req.Pool.BlacklistDuration != "" {
					if d, err := time.ParseDuration(req.Pool.BlacklistDuration); err == nil {
						s.cfgSrc.Pool.BlacklistDuration = d
					}
				}
			}
			if req.Management != nil {
				s.cfgSrc.Management.Listen = req.Management.Listen
				if req.Management.HealthCheckInterval != "" {
					if d, err := time.ParseDuration(req.Management.HealthCheckInterval); err == nil && d > 0 {
						s.cfgSrc.Management.HealthCheckInterval = d
						s.cfg.HealthCheckInterval = d
					}
				}
				keyValue := strings.TrimSpace(req.Management.APIKey)
				s.cfgSrc.Management.APIKey = keyValue
				s.cfgSrc.Management.Password = ""
				s.cfgSrc.Management.QualityEnabled = &req.Management.QualityEnabled
				s.cfgSrc.Management.QualityProvider = strings.TrimSpace(req.Management.QualityProvider)
				s.cfgSrc.Management.QualityAPIKey = strings.TrimSpace(req.Management.QualityAPIKey)
				qualityCacheTTL := s.cfgSrc.Management.QualityCacheTTL
				if req.Management.QualityCacheTTL != "" {
					if d, err := time.ParseDuration(req.Management.QualityCacheTTL); err == nil && d > 0 {
						qualityCacheTTL = d
						s.cfgSrc.Management.QualityCacheTTL = d
					}
				}
				s.cfg.Password = ""
				s.cfg.APIKey = keyValue
				s.cfg.QualityEnabled = req.Management.QualityEnabled
				s.cfg.QualityProvider = s.cfgSrc.Management.QualityProvider
				s.cfg.QualityAPIKey = s.cfgSrc.Management.QualityAPIKey
				s.cfg.QualityCacheTTL = qualityCacheTTL
				if s.mgr != nil {
					s.mgr.SetQualityConfig(req.Management.QualityEnabled, s.cfgSrc.Management.QualityProvider, s.cfgSrc.Management.QualityAPIKey, qualityCacheTTL)
				}
			}
			if req.GeoIP != nil {
				s.cfgSrc.GeoIP.DatabasePath = req.GeoIP.DatabasePath
				s.cfgSrc.GeoIP.Listen = req.GeoIP.Listen
				s.cfgSrc.GeoIP.Port = req.GeoIP.Port
				s.cfgSrc.GeoIP.AutoUpdateEnabled = req.GeoIP.AutoUpdateEnabled
				if req.GeoIP.AutoUpdateInterval != "" {
					if d, err := time.ParseDuration(req.GeoIP.AutoUpdateInterval); err == nil {
						s.cfgSrc.GeoIP.AutoUpdateInterval = d
					}
				}
			}
			_ = s.persistSettingsLocked()
		}
		s.cfgMu.Unlock()

		writeJSON(w, map[string]any{
			"message":          "设置已保存",
			"external_ip":      extIP,
			"probe_target":     probeTarget,
			"skip_cert_verify": req.SkipCertVerify,
			"need_reload":      true,
		})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleSubscriptionStatus returns the current subscription refresh status.
func (s *Server) handleSubscriptionStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if s.subRefresher == nil {
		writeJSON(w, map[string]any{
			"enabled": false,
			"message": "订阅刷新未启用",
		})
		return
	}

	status := s.subRefresher.Status()
	writeJSON(w, map[string]any{
		"enabled":        status.Enabled,
		"last_refresh":   status.LastRefresh,
		"next_refresh":   status.NextRefresh,
		"node_count":     status.NodeCount,
		"last_error":     status.LastError,
		"refresh_count":  status.RefreshCount,
		"is_refreshing":  status.IsRefreshing,
		"nodes_modified": status.NodesModified,
	})
}

// handleSubscriptionRefresh triggers an immediate subscription refresh.
func (s *Server) handleSubscriptionRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if s.subRefresher == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, map[string]any{"error": "订阅刷新未启用"})
		return
	}

	if err := s.subRefresher.RefreshNow(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(w, map[string]any{"error": err.Error()})
		return
	}

	status := s.subRefresher.Status()
	writeJSON(w, map[string]any{
		"message":    "刷新成功",
		"node_count": status.NodeCount,
	})
}

// handleSubscriptions handles GET/POST for subscription records.
func (s *Server) handleSubscriptions(w http.ResponseWriter, r *http.Request) {
	if s.subRefresher == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, map[string]any{"error": "订阅管理未启用"})
		return
	}

	switch r.Method {
	case http.MethodGet:
		subs, err := s.subRefresher.ListSubscriptions(r.Context())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"subscriptions": subs})
	case http.MethodPost:
		sub, ok := s.decodeSubscriptionPayload(w, r)
		if !ok {
			return
		}
		created, err := s.subRefresher.CreateSubscription(r.Context(), sub)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"message": "订阅已创建", "subscription": created})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleSubscriptionItem handles PUT/DELETE and one-shot refresh for a subscription.
func (s *Server) handleSubscriptionItem(w http.ResponseWriter, r *http.Request) {
	if s.subRefresher == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, map[string]any{"error": "订阅管理未启用"})
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/subscriptions/")
	path = strings.Trim(path, "/")
	if path == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	parts := strings.Split(path, "/")
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || id <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "订阅 ID 无效"})
		return
	}

	if len(parts) == 2 && parts[1] == "refresh" {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if err := s.subRefresher.RefreshSubscription(r.Context(), id); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"message": "订阅刷新成功"})
		return
	}
	if len(parts) == 2 && parts[1] == "probe" {
		s.handleSubscriptionProbe(w, r, id)
		return
	}
	if len(parts) == 2 && parts[1] == "nodes" {
		s.handleSubscriptionNodes(w, r, id)
		return
	}
	if len(parts) > 1 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodPut:
		sub, ok := s.decodeSubscriptionPayload(w, r)
		if !ok {
			return
		}
		sub.ID = id
		updated, err := s.subRefresher.UpdateSubscription(r.Context(), sub)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"message": "订阅已更新", "subscription": updated})
	case http.MethodDelete:
		if err := s.subRefresher.DeleteSubscription(r.Context(), id); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"message": "订阅已删除"})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleSubscriptionProbe(w http.ResponseWriter, r *http.Request, id int64) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	allNodes, err := s.listAllSubscriptionNodes(r.Context(), id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(w, map[string]any{"error": err.Error()})
		return
	}
	uris := make(map[string]struct{}, len(allNodes))
	for _, node := range allNodes {
		uri := strings.TrimSpace(node.URI)
		if uri != "" {
			uris[uri] = struct{}{}
		}
	}
	if len(uris) == 0 {
		s.streamProbeSnapshots(w, r, nil)
		return
	}
	snapshots := make([]Snapshot, 0, len(uris))
	for _, snap := range s.mgr.Snapshot() {
		if _, ok := uris[strings.TrimSpace(snap.URI)]; ok {
			snapshots = append(snapshots, snap)
		}
	}
	s.streamProbeSnapshots(w, r, snapshots)
}

func (s *Server) handleSubscriptionNodes(w http.ResponseWriter, r *http.Request, id int64) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	page := 1
	if v, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("page"))); err == nil && v > 0 {
		page = v
	}
	pageSize := 20
	if v, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("page_size"))); err == nil && v > 0 {
		pageSize = v
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 200 {
		pageSize = 200
	}
	keyword := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("keyword")))
	regionFilter := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("region")))
	statusFilter := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("status")))
	if statusFilter == "" {
		statusFilter = "all"
	}

	if keyword == "" && regionFilter == "" && statusFilter == "all" {
		nodePage, err := s.subRefresher.ListSubscriptionNodes(r.Context(), id, page, pageSize)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{
			"nodes":       s.buildNodeViews(nodePage.Nodes),
			"total":       nodePage.Total,
			"page":        nodePage.Page,
			"page_size":   nodePage.PageSize,
			"total_pages": nodePage.TotalPages,
		})
		return
	}

	allNodes, err := s.listAllSubscriptionNodes(r.Context(), id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(w, map[string]any{"error": err.Error()})
		return
	}
	views := s.buildNodeViews(allNodes)
	filtered := make([]configNodeView, 0, len(views))
	for _, view := range views {
		if matchesSubscriptionNodeFilters(view, keyword, regionFilter, statusFilter) {
			filtered = append(filtered, view)
		}
	}
	total := len(filtered)
	totalPages := 0
	if total > 0 {
		totalPages = (total + pageSize - 1) / pageSize
	}
	if totalPages == 0 {
		page = 1
	} else if page > totalPages {
		page = totalPages
	}
	start, end := 0, 0
	if total > 0 {
		start = (page - 1) * pageSize
		if start < 0 {
			start = 0
		}
		if start > total {
			start = total
		}
		end = start + pageSize
		if end > total {
			end = total
		}
	}
	pageViews := []configNodeView{}
	if total > 0 && start < end {
		pageViews = filtered[start:end]
	}
	writeJSON(w, map[string]any{
		"nodes":       pageViews,
		"total":       total,
		"page":        page,
		"page_size":   pageSize,
		"total_pages": totalPages,
	})
}

func (s *Server) listAllSubscriptionNodes(ctx context.Context, id int64) ([]config.NodeConfig, error) {
	const pageSize = 200
	page := 1
	var out []config.NodeConfig
	for {
		nodePage, err := s.subRefresher.ListSubscriptionNodes(ctx, id, page, pageSize)
		if err != nil {
			return nil, err
		}
		out = append(out, nodePage.Nodes...)
		if nodePage.TotalPages <= 0 || page >= nodePage.TotalPages {
			break
		}
		page++
	}
	return out, nil
}

func matchesSubscriptionNodeFilters(view configNodeView, keyword, regionFilter, statusFilter string) bool {
	if keyword != "" {
		parts := []string{view.Name, view.URI, string(view.Source)}
		if view.Runtime != nil {
			parts = append(parts, view.Runtime.Tag, view.Runtime.ExitIP, view.Runtime.Country, view.Runtime.CountryCode, view.Runtime.ISP, view.Runtime.ASN, view.Runtime.ASName)
		}
		if !strings.Contains(strings.ToLower(strings.Join(parts, " ")), keyword) {
			return false
		}
	}
	if regionFilter != "" {
		parts := []string{}
		if view.Runtime != nil {
			parts = append(parts, view.Runtime.Region, view.Runtime.Country, view.Runtime.CountryCode, view.Runtime.ExitIP)
		}
		if !strings.Contains(strings.ToLower(strings.Join(parts, " ")), regionFilter) {
			return false
		}
	}
	switch statusFilter {
	case "healthy":
		return view.RuntimeState == "healthy"
	case "unhealthy":
		return view.RuntimeState == "unhealthy" || view.RuntimeState == "blacklisted"
	case "unknown":
		return view.RuntimeState == "unknown"
	default:
		return true
	}
}

func (s *Server) buildNodeViews(nodes []config.NodeConfig) []configNodeView {
	snapshots := s.mgr.Snapshot()
	byURI := make(map[string]Snapshot, len(snapshots))
	byName := make(map[string]Snapshot, len(snapshots))
	for _, snap := range snapshots {
		if snap.URI != "" {
			byURI[snap.URI] = snap
		}
		if snap.Name != "" {
			byName[snap.Name] = snap
		}
	}
	trafficMap := make(map[string][2]int64)
	if s.trafficFn != nil {
		trafficMap = s.trafficFn()
	}
	out := make([]configNodeView, 0, len(nodes))
	for _, node := range nodes {
		view := configNodeView{NodeConfig: node, RuntimeState: "unknown"}
		var snap Snapshot
		var ok bool
		if node.URI != "" {
			snap, ok = byURI[node.URI]
		}
		if !ok && node.Name != "" {
			snap, ok = byName[node.Name]
		}
		if ok {
			view.Runtime = &configNodeRuntimeSummary{
				Tag:              snap.Tag,
				Available:        snap.Available,
				InitialCheckDone: snap.InitialCheckDone,
				Blacklisted:      snap.Blacklisted,
				FailureCount:     snap.FailureCount,
				LastLatencyMs:    snap.LastLatencyMs,
				Region:           snap.NodeInfo.Region,
				CountryCode:      snap.QualityInfo.CountryCode,
				Country:          snap.QualityInfo.Country,
				ExitIP:           snap.ExitIP,
				ProxyType:        snap.ProxyType,
				IPValid:          snap.IPValid,
				QualityError:     snap.Error,
				ActiveConns:      snap.ActiveConnections,
				ISP:              snap.ISP,
				ASN:              snap.ASN,
				ASName:           snap.ASName,
				IPVersion:        snap.IPVersion,
				IPType:           snap.IPType,
				IPInvalidReason:  snap.IPInvalidReason,
				Mobile:           snap.Mobile,
				Hosting:          snap.Hosting,
				Proxy:            snap.Proxy,
			}
			if tag := snap.Tag; tag != "" {
				if td, found := trafficMap[tag]; found {
					view.Runtime.UploadBytes = td[0]
					view.Runtime.DownloadBytes = td[1]
				}
			}
			switch {
			case snap.Blacklisted:
				view.RuntimeState = "blacklisted"
			case snap.InitialCheckDone && !snap.Available:
				view.RuntimeState = "unhealthy"
			case snap.InitialCheckDone && snap.Available:
				view.RuntimeState = "healthy"
			default:
				view.RuntimeState = "unknown"
			}
		}
		out = append(out, view)
	}
	return out
}

func (s *Server) decodeSubscriptionPayload(w http.ResponseWriter, r *http.Request) (storepkg.Subscription, bool) {
	var req struct {
		Name            string `json:"name"`
		URL             string `json:"url"`
		Enabled         bool   `json:"enabled"`
		RefreshInterval string `json:"refresh_interval"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "请求格式错误"})
		return storepkg.Subscription{}, false
	}

	interval, err := time.ParseDuration(strings.TrimSpace(req.RefreshInterval))
	if err != nil || interval < 5*time.Minute {
		interval = time.Hour
	}

	return storepkg.Subscription{
		Name:            strings.TrimSpace(req.Name),
		URL:             strings.TrimSpace(req.URL),
		Enabled:         req.Enabled,
		RefreshInterval: interval,
	}, true
}

// nodePayload is the JSON request body for node CRUD operations.
type nodePayload struct {
	Name     string `json:"name"`
	URI      string `json:"uri"`
	Port     uint16 `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type configNodeRuntimeSummary struct {
	Tag              string `json:"tag,omitempty"`
	Available        bool   `json:"available"`
	InitialCheckDone bool   `json:"initial_check_done"`
	Blacklisted      bool   `json:"blacklisted"`
	FailureCount     int    `json:"failure_count"`
	LastLatencyMs    int64  `json:"last_latency_ms"`
	Region           string `json:"region,omitempty"`
	CountryCode      string `json:"country_code,omitempty"`
	Country          string `json:"country,omitempty"`
	ExitIP           string `json:"exit_ip,omitempty"`
	ProxyType        string `json:"proxy_type,omitempty"`
	IPValid          bool   `json:"ip_valid"`
	QualityError     string `json:"quality_error,omitempty"`
	ActiveConns      int32  `json:"active_connections"`
	UploadBytes      int64  `json:"upload_bytes"`
	DownloadBytes    int64  `json:"download_bytes"`
	ISP              string `json:"isp,omitempty"`
	ASN              string `json:"asn,omitempty"`
	ASName           string `json:"as_name,omitempty"`
	IPVersion        string `json:"ip_version,omitempty"`
	IPType           string `json:"ip_type,omitempty"`
	IPInvalidReason  string `json:"ip_invalid_reason,omitempty"`
	Mobile           bool   `json:"mobile,omitempty"`
	Hosting          bool   `json:"hosting,omitempty"`
	Proxy            bool   `json:"proxy,omitempty"`
}

type configNodeView struct {
	config.NodeConfig
	Runtime      *configNodeRuntimeSummary `json:"runtime,omitempty"`
	RuntimeState string                    `json:"runtime_state,omitempty"` // healthy / unhealthy / blacklisted / unknown
	ConnectHTTP  string                    `json:"connect_http,omitempty"`
	ConnectSocks string                    `json:"connect_socks,omitempty"`
}

func (p nodePayload) toConfig() config.NodeConfig {
	return config.NodeConfig{
		Name:     p.Name,
		URI:      p.URI,
		Port:     p.Port,
		Username: p.Username,
		Password: p.Password,
	}
}

// handleConfigNodes handles GET (list) and POST (create) for config nodes.
func (s *Server) handleConfigNodes(w http.ResponseWriter, r *http.Request) {
	if !s.ensureNodeManager(w) {
		return
	}

	switch r.Method {
	case http.MethodGet:
		nodes, err := s.nodeMgr.ListConfigNodes(r.Context())
		if err != nil {
			s.respondNodeError(w, err)
			return
		}
		page := 1
		if v, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("page"))); err == nil && v > 0 {
			page = v
		}
		pageSize := 20
		if v, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("page_size"))); err == nil && v > 0 {
			if v > 200 {
				v = 200
			}
			pageSize = v
		}
		keyword := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("keyword")))
		source := strings.TrimSpace(r.URL.Query().Get("source"))
		statusFilter := strings.TrimSpace(r.URL.Query().Get("status"))
		if statusFilter == "" {
			statusFilter = "all"
		}

		s.cfgMu.RLock()
		var cfgCopy *config.Config
		if s.cfgSrc != nil {
			tmp := *s.cfgSrc
			cfgCopy = &tmp
		}
		s.cfgMu.RUnlock()
		externalIP, _, _, _ := s.getSettings()

		snapshots := s.mgr.Snapshot()
		byURI := make(map[string]Snapshot, len(snapshots))
		byName := make(map[string]Snapshot, len(snapshots))
		for _, snap := range snapshots {
			if snap.URI != "" {
				byURI[snap.URI] = snap
			}
			if snap.Name != "" {
				byName[snap.Name] = snap
			}
		}

		trafficMap := make(map[string][2]int64)
		if s.trafficFn != nil {
			trafficMap = s.trafficFn()
		}

		filtered := make([]configNodeView, 0, len(nodes))
		hasSubscriptionNodes := false
		for _, node := range nodes {
			if node.Source == config.NodeSourceSubscription {
				hasSubscriptionNodes = true
			}
			if source != "" && source != "all" && string(node.Source) != source {
				continue
			}
			if keyword != "" {
				text := strings.ToLower(strings.Join([]string{node.Name, node.URI, string(node.Source)}, " "))
				if !strings.Contains(text, keyword) {
					continue
				}
			}
			view := configNodeView{NodeConfig: node, RuntimeState: "unknown"}
			var snap Snapshot
			var ok bool
			if node.URI != "" {
				snap, ok = byURI[node.URI]
			}
			if !ok && node.Name != "" {
				snap, ok = byName[node.Name]
			}
			if cfgCopy != nil && node.Port > 0 && (cfgCopy.Mode == "hybrid" || cfgCopy.Mode == "multi-port" || cfgCopy.Mode == "") {
				listenAddr := cfgCopy.MultiPort.Address
				if listenAddr == "" || listenAddr == "0.0.0.0" || listenAddr == "::" {
					if externalIP != "" {
						listenAddr = externalIP
					}
				}
				if listenAddr != "" {
					var authPart string
					if cfgCopy.MultiPort.Username != "" && cfgCopy.MultiPort.Password != "" {
						authPart = fmt.Sprintf("%s:%s@", cfgCopy.MultiPort.Username, cfgCopy.MultiPort.Password)
					}
					view.ConnectHTTP = fmt.Sprintf("http://%s%s:%d", authPart, listenAddr, node.Port)
					view.ConnectSocks = fmt.Sprintf("socks5://%s%s:%d", authPart, listenAddr, node.Port)
				}
			}
			if ok {
				view.Runtime = &configNodeRuntimeSummary{
					Tag:              snap.Tag,
					Available:        snap.Available,
					InitialCheckDone: snap.InitialCheckDone,
					Blacklisted:      snap.Blacklisted,
					FailureCount:     snap.FailureCount,
					LastLatencyMs:    snap.LastLatencyMs,
					ExitIP:           snap.ExitIP,
					ProxyType:        snap.ProxyType,
					IPValid:          snap.IPValid,
					QualityError:     snap.Error,
					Region:           snap.Region,
					ActiveConns:      snap.ActiveConnections,
					ISP:              snap.ISP,
					ASN:              snap.ASN,
					ASName:           snap.ASName,
					IPVersion:        snap.IPVersion,
					IPType:           snap.IPType,
					IPInvalidReason:  snap.IPInvalidReason,
					Mobile:           snap.Mobile,
					Hosting:          snap.Hosting,
					Proxy:            snap.Proxy,
				}
				if tag := snap.Tag; tag != "" {
					if td, found := trafficMap[tag]; found {
						view.Runtime.UploadBytes = td[0]
						view.Runtime.DownloadBytes = td[1]
					}
				}
				view.Runtime = &configNodeRuntimeSummary{
					Tag:              snap.Tag,
					Available:        snap.Available,
					InitialCheckDone: snap.InitialCheckDone,
					Blacklisted:      snap.Blacklisted,
					FailureCount:     snap.FailureCount,
					LastLatencyMs:    snap.LastLatencyMs,
					Region:           snap.NodeInfo.Region,
					CountryCode:      snap.QualityInfo.CountryCode,
					Country:          snap.QualityInfo.Country,
					ExitIP:           snap.ExitIP,
					ProxyType:        snap.ProxyType,
					IPValid:          snap.IPValid,
					QualityError:     snap.Error,
					ActiveConns:      snap.ActiveConnections,
					ISP:              snap.ISP,
					ASN:              snap.ASN,
					ASName:           snap.ASName,
					IPVersion:        snap.IPVersion,
					IPType:           snap.IPType,
					IPInvalidReason:  snap.IPInvalidReason,
					Mobile:           snap.Mobile,
					Hosting:          snap.Hosting,
					Proxy:            snap.Proxy,
				}
				if tag := snap.Tag; tag != "" {
					if td, found := trafficMap[tag]; found {
						view.Runtime.UploadBytes = td[0]
						view.Runtime.DownloadBytes = td[1]
					}
				}
				switch {
				case snap.Blacklisted:
					view.RuntimeState = "blacklisted"
				case snap.InitialCheckDone && !snap.Available:
					view.RuntimeState = "unhealthy"
				case snap.InitialCheckDone && snap.Available:
					view.RuntimeState = "healthy"
				default:
					view.RuntimeState = "unknown"
				}
			}
			switch statusFilter {
			case "healthy":
				if view.RuntimeState != "healthy" {
					continue
				}
			case "unhealthy":
				if view.RuntimeState != "unhealthy" && view.RuntimeState != "blacklisted" {
					continue
				}
			case "unknown":
				if view.RuntimeState != "unknown" {
					continue
				}
			}
			filtered = append(filtered, view)
		}

		total := len(filtered)
		totalPages := 0
		if total > 0 {
			totalPages = (total + pageSize - 1) / pageSize
		}
		if totalPages == 0 {
			page = 1
		} else if page > totalPages {
			page = totalPages
		}

		start := 0
		end := 0
		if total > 0 {
			start = (page - 1) * pageSize
			if start < 0 {
				start = 0
			}
			if start > total {
				start = total
			}
			end = start + pageSize
			if end > total {
				end = total
			}
		}

		pageNodes := []configNodeView{}
		if total > 0 && start < end {
			pageNodes = filtered[start:end]
		}

		writeJSON(w, map[string]any{
			"nodes":                  pageNodes,
			"total":                  total,
			"page":                   page,
			"page_size":              pageSize,
			"total_pages":            totalPages,
			"keyword":                keyword,
			"source":                 source,
			"status":                 statusFilter,
			"has_subscription_nodes": hasSubscriptionNodes,
		})
	case http.MethodPost:
		var payload nodePayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": "请求格式错误"})
			return
		}
		node, err := s.nodeMgr.CreateNode(r.Context(), payload.toConfig())
		if err != nil {
			s.respondNodeError(w, err)
			return
		}
		writeJSON(w, map[string]any{"node": node, "message": "节点已添加，请点击重载使配置生效"})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleConfigNodeItem handles PUT (update) and DELETE for a specific config node.
func (s *Server) handleConfigNodeItem(w http.ResponseWriter, r *http.Request) {
	if !s.ensureNodeManager(w) {
		return
	}

	namePart := strings.TrimPrefix(r.URL.Path, "/api/nodes/config/")
	nodeName, err := url.PathUnescape(namePart)
	if err != nil || nodeName == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "节点名称无效"})
		return
	}

	switch r.Method {
	case http.MethodPut:
		var payload nodePayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": "请求格式错误"})
			return
		}
		node, err := s.nodeMgr.UpdateNode(r.Context(), nodeName, payload.toConfig())
		if err != nil {
			s.respondNodeError(w, err)
			return
		}
		writeJSON(w, map[string]any{"node": node, "message": "节点已更新，请点击重载使配置生效"})
	case http.MethodDelete:
		if err := s.nodeMgr.DeleteNode(r.Context(), nodeName); err != nil {
			s.respondNodeError(w, err)
			return
		}
		writeJSON(w, map[string]any{"message": "节点已删除，请点击重载使配置生效"})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleReload triggers a configuration reload.
func (s *Server) handleReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !s.ensureNodeManager(w) {
		return
	}

	if err := s.nodeMgr.TriggerReload(r.Context()); err != nil {
		s.respondNodeError(w, err)
		return
	}
	writeJSON(w, map[string]any{
		"message": "重载成功，现有连接已被中断",
	})
}

func (s *Server) ensureNodeManager(w http.ResponseWriter) bool {
	if s.nodeMgr == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, map[string]any{"error": "节点管理未启用"})
		return false
	}
	return true
}

func (s *Server) respondNodeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrNodeNotFound):
		status = http.StatusNotFound
	case errors.Is(err, ErrNodeConflict), errors.Is(err, ErrInvalidNode):
		status = http.StatusBadRequest
	}
	w.WriteHeader(status)
	writeJSON(w, map[string]any{"error": err.Error()})
}

func (s *Server) handleConnectionStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, s.getConnectionStatusPayload())
}

func (s *Server) handleConnectionStatusStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	send := func() error {
		payload := s.getConnectionStatusPayload()
		b, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "event: connections\ndata: %s\n\n", b); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	if err := send(); err != nil {
		return
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if err := send(); err != nil {
				return
			}
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) getConnectionStatusPayload() map[string]any {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:9092/connections")
	if err != nil {
		return map[string]any{
			"active":          0,
			"inbound":         0,
			"outbound_routes": 0,
			"error":           err.Error(),
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return map[string]any{
			"active":          0,
			"inbound":         0,
			"outbound_routes": 0,
			"error":           fmt.Sprintf("clash api status: %d", resp.StatusCode),
		}
	}
	var payload struct {
		DownloadTotal int64 `json:"downloadTotal"`
		UploadTotal   int64 `json:"uploadTotal"`
		Connections   []struct {
			Chains   []string `json:"chains"`
			Upload   int64    `json:"upload"`
			Download int64    `json:"download"`
			Metadata struct {
				Type string `json:"type"`
			} `json:"metadata"`
		} `json:"connections"`
		Memory uint64 `json:"memory"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return map[string]any{
			"active":          0,
			"inbound":         0,
			"outbound_routes": 0,
			"error":           err.Error(),
		}
	}
	routes := make(map[string]struct{})
	for _, conn := range payload.Connections {
		for i := len(conn.Chains) - 1; i >= 0; i-- {
			chain := strings.TrimSpace(conn.Chains[i])
			if chain != "" {
				routes[chain] = struct{}{}
				break
			}
		}
	}
	return map[string]any{
		"active":          len(payload.Connections),
		"inbound":         len(payload.Connections),
		"outbound_routes": len(routes),
		"upload_total":    payload.UploadTotal,
		"download_total":  payload.DownloadTotal,
		"memory":          payload.Memory,
	}
}

// handleTraffic streams real-time traffic from sing-box Clash API as SSE.
// Clash API /traffic returns newline-delimited JSON; we convert to SSE for browser EventSource.
func (s *Server) handleTraffic(w http.ResponseWriter, r *http.Request) {
	// Connect to sing-box Clash API
	resp, err := http.Get("http://127.0.0.1:9092/traffic")
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		writeJSON(w, map[string]any{"error": "无法连接到流量统计接口", "details": err.Error()})
		return
	}
	defer resp.Body.Close()

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Read NDJSON lines from Clash API and forward as SSE
	buf := make([]byte, 4096)
	for {
		select {
		case <-r.Context().Done():
			return
		default:
		}
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			// Each chunk may contain one or more JSON lines; forward as-is in SSE data frames
			lines := strings.Split(strings.TrimSpace(string(buf[:n])), "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				fmt.Fprintf(w, "data: %s\n\n", line)
			}
			flusher.Flush()
		}
		if readErr != nil {
			return
		}
	}
}

// handleLogs returns recent console log content from the in-memory ring buffer.
func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	content := SharedLogBuffer.Content()
	writeJSON(w, map[string]any{"logs": content})
}

func (s *Server) handleLogsStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	sendLogs := func() error {
		content, _ := SharedLogBuffer.Snapshot()
		b, err := json.Marshal(map[string]any{"logs": content})
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "event: logs\ndata: %s\n\n", b); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	updates, _, cancel := SharedLogBuffer.Subscribe()
	defer cancel()

	if err := sendLogs(); err != nil {
		return
	}

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case _, ok := <-updates:
			if !ok {
				return
			}
			if err := sendLogs(); err != nil {
				return
			}
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// Session management functions

// generateSessionToken creates a cryptographically secure random token.
func (s *Server) generateSessionToken() (string, error) {
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", fmt.Errorf("failed to generate session token: %w", err)
	}
	return hex.EncodeToString(tokenBytes), nil
}

// createSession creates a new session with expiration.
func (s *Server) createSession() (*Session, error) {
	token, err := s.generateSessionToken()
	if err != nil {
		return nil, err
	}

	now := time.Now()
	session := &Session{
		Token:     token,
		CreatedAt: now,
		ExpiresAt: now.Add(s.sessionTTL),
	}

	s.sessionMu.Lock()
	s.sessions[token] = session
	s.sessionMu.Unlock()

	return session, nil
}

// validateSession checks if a session token is valid and not expired.
func (s *Server) validateSession(token string) bool {
	s.sessionMu.RLock()
	session, exists := s.sessions[token]
	s.sessionMu.RUnlock()

	if !exists {
		return false
	}

	// Check if expired
	if time.Now().After(session.ExpiresAt) {
		s.sessionMu.Lock()
		delete(s.sessions, token)
		s.sessionMu.Unlock()
		return false
	}

	return true
}

// cleanupExpiredSessions periodically removes expired sessions.
func (s *Server) cleanupExpiredSessions() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		s.sessionMu.Lock()
		for token, session := range s.sessions {
			if now.After(session.ExpiresAt) {
				delete(s.sessions, token)
			}
		}
		s.sessionMu.Unlock()
	}
}

// secureCompareStrings performs constant-time string comparison to prevent timing attacks.
func secureCompareStrings(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
