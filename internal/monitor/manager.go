package monitor

import (
	"context"
	"errors"
	"fmt"
	"net"
	neturl "net/url"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

// Config mirrors user settings needed by the monitoring server.
type Config struct {
	Enabled             bool
	Listen              string
	ProbeTarget         string
	HealthCheckInterval time.Duration
	Password            string
	APIKey              string // WebUI/API key（用于 apikey 查询参数鉴权）
	ProxyUsername       string // 代理池的用户名（用于导出）
	ProxyPassword       string // 代理池的密码（用于导出）
	ExternalIP          string // 外部 IP 地址，用于导出时替换 0.0.0.0
	SkipCertVerify      bool   // 全局跳过 SSL 证书验证
	QualityEnabled      bool
	QualityProvider     string
	QualityAPIKey       string
	QualityCacheTTL     time.Duration
}

// NodeInfo is static metadata about a proxy entry.
type NodeInfo struct {
	Tag           string `json:"tag"`
	Name          string `json:"name"`
	URI           string `json:"uri"`
	Mode          string `json:"mode"`
	ListenAddress string `json:"listen_address,omitempty"`
	Port          uint16 `json:"port,omitempty"`
	Region        string `json:"region,omitempty"`  // GeoIP region code: lowercase ISO country code (e.g. "jp", "us", "de"), fallback "other"
	Country       string `json:"country,omitempty"` // Full country name from GeoIP
}

// QualityInfo describes the observed exit IP and coarse network type of a node.
// The type is heuristic (based on ASN/ISP/provider flags), not a guaranteed truth.
type QualityInfo struct {
	ExitIP          string    `json:"exit_ip,omitempty"`
	IPValid         bool      `json:"ip_valid"`
	IPVersion       string    `json:"ip_version,omitempty"` // ipv4 / ipv6
	IPType          string    `json:"ip_type,omitempty"`    // public/private/loopback/link_local/multicast/reserved/invalid/unknown
	IPInvalidReason string    `json:"ip_invalid_reason,omitempty"`
	CountryCode     string    `json:"country_code,omitempty"`
	Country         string    `json:"country,omitempty"`
	ASN             string    `json:"asn,omitempty"`
	ASName          string    `json:"as_name,omitempty"`
	ISP             string    `json:"isp,omitempty"`
	Org             string    `json:"org,omitempty"`
	ProxyType       string    `json:"proxy_type,omitempty"` // isp/datacenter/mobile/unknown
	QualitySource   string    `json:"quality_source,omitempty"`
	Mobile          bool      `json:"mobile,omitempty"`
	Hosting         bool      `json:"hosting,omitempty"`
	Proxy           bool      `json:"proxy,omitempty"`
	CheckedAt       time.Time `json:"quality_checked_at,omitempty"`
	Error           string    `json:"quality_error,omitempty"`
}

// PersistedState is the DB-backed subset of runtime state that survives restarts.
type PersistedState struct {
	Available        bool
	InitialCheckDone bool
	FailureCount     int
	Blacklisted      bool
	BlacklistedUntil time.Time
	LastError        string
	LastLatencyMs    int64
	LastSuccess      time.Time
	LastFailure      time.Time
	CountryCode      string
	Country          string
	ExitIP           string
	ProxyType        string
	QualitySource    string
	QualityError     string
	QualityCheckedAt time.Time
	IPValid          bool
	IPVersion        string
	IPType           string
	IPInvalidReason  string
	ASN              string
	ASName           string
	ISP              string
	Org              string
	Mobile           bool
	Hosting          bool
	Proxy            bool
}

// StateStore persists runtime node state.
type StateStore interface {
	LoadRuntimeStates(ctx context.Context) (map[string]PersistedState, error)
	SaveRuntimeState(ctx context.Context, nodeURI string, st PersistedState) error
	DeleteRuntimeState(ctx context.Context, nodeURI string) error
}

// TimelineEvent represents a single usage event for debug tracking.
type TimelineEvent struct {
	Time      time.Time `json:"time"`
	Success   bool      `json:"success"`
	LatencyMs int64     `json:"latency_ms"`
	Error     string    `json:"error,omitempty"`
}

const maxTimelineSize = 20

// Snapshot is a runtime view of a proxy node.
type Snapshot struct {
	NodeInfo
	FailureCount      int             `json:"failure_count"`
	SuccessCount      int64           `json:"success_count"`
	Blacklisted       bool            `json:"blacklisted"`
	BlacklistedUntil  time.Time       `json:"blacklisted_until"`
	ActiveConnections int32           `json:"active_connections"`
	LastError         string          `json:"last_error,omitempty"`
	LastFailure       time.Time       `json:"last_failure,omitempty"`
	LastSuccess       time.Time       `json:"last_success,omitempty"`
	LastProbeLatency  time.Duration   `json:"last_probe_latency,omitempty"`
	LastLatencyMs     int64           `json:"last_latency_ms"`
	Available         bool            `json:"available"`
	InitialCheckDone  bool            `json:"initial_check_done"`
	Timeline          []TimelineEvent `json:"timeline,omitempty"`
	QualityInfo
}

type probeFunc func(ctx context.Context) (time.Duration, error)
type qualityFunc func(ctx context.Context) (QualityInfo, error)
type releaseFunc func()

type EntryHandle struct {
	ref *entry
	mgr *Manager
}

type entry struct {
	info             NodeInfo
	failure          int
	success          int64
	timeline         []TimelineEvent
	blacklist        bool
	until            time.Time
	lastError        string
	lastFail         time.Time
	lastOK           time.Time
	lastProbe        time.Duration
	active           atomic.Int32
	probe            probeFunc
	qualityProbe     qualityFunc
	release          releaseFunc
	blacklistFn      func(time.Duration)
	initialCheckDone bool
	available        bool
	quality          QualityInfo
	qualityChecking  bool
	qualityAttempt   time.Time
	mu               sync.RWMutex
}

// Manager aggregates all node states for the UI/API.
type Manager struct {
	cfg                Config
	probeDst           M.Socksaddr
	probeReady         bool
	probePath          string
	probeTLS           bool
	mu                 sync.RWMutex
	nodes              map[string]*entry
	ctx                context.Context
	cancel             context.CancelFunc
	logger             Logger
	stateStore         StateStore
	persisted          map[string]PersistedState
	periodicProbeInRun atomic.Bool
	periodicProbeStop  context.CancelFunc
	periodicProbeTO    time.Duration
}

// Logger interface for logging
type Logger interface {
	Info(args ...any)
	Warn(args ...any)
}

// NewManager constructs a manager and pre-validates the probe target.
func NewManager(cfg Config) (*Manager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		cfg:       cfg,
		nodes:     make(map[string]*entry),
		ctx:       ctx,
		cancel:    cancel,
		persisted: make(map[string]PersistedState),
		probePath: "/generate_204",
	}
	if cfg.ProbeTarget != "" {
		targetRaw := strings.TrimSpace(cfg.ProbeTarget)
		parseRaw := targetRaw
		if !strings.Contains(parseRaw, "://") {
			parseRaw = "http://" + parseRaw
		}
		u, err := neturl.Parse(parseRaw)
		if err != nil {
			u = &neturl.URL{Scheme: "http", Host: targetRaw}
		}

		m.probeTLS = strings.EqualFold(strings.TrimSpace(u.Scheme), "https")
		if p := strings.TrimSpace(u.EscapedPath()); p != "" {
			m.probePath = p
		}
		if u.RawQuery != "" {
			m.probePath += "?" + u.RawQuery
		}
		if !strings.HasPrefix(m.probePath, "/") {
			m.probePath = "/" + m.probePath
		}

		targetHost := strings.TrimSpace(u.Host)
		host, port, err := net.SplitHostPort(targetHost)
		if err != nil {
			host = targetHost
			if m.probeTLS {
				port = "443"
			} else {
				port = "80"
			}
		}
		parsed := M.ParseSocksaddrHostPort(host, parsePort(port))
		m.probeDst = parsed
		m.probeReady = true
	}
	return m, nil
}

// QualityConfig returns exit-IP quality lookup settings.
func (m *Manager) QualityConfig() (enabled bool, provider string, apiKey string, cacheTTL time.Duration) {
	if m == nil {
		return false, "off", "", 0
	}
	m.mu.RLock()
	cfg := m.cfg
	m.mu.RUnlock()
	provider = strings.ToLower(strings.TrimSpace(cfg.QualityProvider))
	if provider == "" {
		provider = "auto"
	}
	if provider == "off" || provider == "none" || provider == "disabled" {
		return false, provider, cfg.QualityAPIKey, cfg.QualityCacheTTL
	}
	if cfg.QualityCacheTTL <= 0 {
		cfg.QualityCacheTTL = 7 * 24 * time.Hour
	}
	return cfg.QualityEnabled, provider, cfg.QualityAPIKey, cfg.QualityCacheTTL
}

// SetQualityConfig updates exit-IP quality lookup settings at runtime.
func (m *Manager) SetQualityConfig(enabled bool, provider, apiKey string, cacheTTL time.Duration) {
	if m == nil {
		return
	}
	if strings.TrimSpace(provider) == "" {
		provider = "auto"
	}
	if cacheTTL <= 0 {
		cacheTTL = 7 * 24 * time.Hour
	}
	m.mu.Lock()
	m.cfg.QualityEnabled = enabled
	m.cfg.QualityProvider = strings.ToLower(strings.TrimSpace(provider))
	m.cfg.QualityAPIKey = apiKey
	m.cfg.QualityCacheTTL = cacheTTL
	m.mu.Unlock()
}

// SetLogger sets the logger for the manager.
func (m *Manager) SetLogger(logger Logger) {
	m.logger = logger
}

// SetStateStore binds persistent runtime-state storage and restores known state.
func (m *Manager) SetStateStore(store StateStore) error {
	if m == nil {
		return nil
	}

	m.mu.Lock()
	m.stateStore = store
	m.persisted = make(map[string]PersistedState)
	m.mu.Unlock()

	if store == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	states, err := store.LoadRuntimeStates(ctx)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.persisted = states
	for _, e := range m.nodes {
		if uri := strings.TrimSpace(e.info.URI); uri != "" {
			if st, ok := m.persisted[uri]; ok {
				e.restorePersistedState(st)
				delete(m.persisted, uri)
			}
		}
	}
	return nil
}

// StartPeriodicHealthCheck starts a background goroutine that periodically checks all nodes.
// interval: how often to check (e.g., 30 * time.Second)
// timeout: timeout for each probe (e.g., 10 * time.Second)
func (m *Manager) StartPeriodicHealthCheck(interval, timeout time.Duration) {
	m.mu.Lock()
	if m.periodicProbeStop != nil {
		m.periodicProbeStop()
		m.periodicProbeStop = nil
	}
	logger := m.logger
	probeReady := m.probeReady
	if timeout <= 0 {
		timeout = m.periodicProbeTO
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	m.periodicProbeTO = timeout
	if !probeReady {
		m.mu.Unlock()
		if logger != nil {
			logger.Warn("probe target not configured, periodic health check disabled")
		}
		return
	}
	if interval <= 0 {
		m.mu.Unlock()
		if logger != nil {
			logger.Info("periodic health check disabled")
		}
		return
	}
	loopCtx, cancel := context.WithCancel(m.ctx)
	m.periodicProbeStop = cancel
	m.mu.Unlock()

	go func(ctx context.Context, interval, timeout time.Duration) {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !m.periodicProbeInRun.CompareAndSwap(false, true) {
					if logger != nil {
						logger.Warn("skip periodic health check: previous round still running")
					}
					continue
				}
				m.probeAllNodes(timeout)
				m.periodicProbeInRun.Store(false)
			}
		}
	}(loopCtx, interval, timeout)

	if logger != nil {
		logger.Info("periodic health check started, interval: ", interval)
	}
}

func (m *Manager) UpdatePeriodicHealthCheckInterval(interval time.Duration) {
	if m == nil {
		return
	}
	m.StartPeriodicHealthCheck(interval, 0)
}

// ProbeAllNow triggers a one-time health check on all nodes (e.g. after reload).
func (m *Manager) ProbeAllNow(timeout time.Duration) {
	if !m.probeReady {
		if m.logger != nil {
			m.logger.Warn("probe target not configured, skipping health check")
		}
		return
	}
	if timeout <= 0 {
		m.mu.RLock()
		timeout = m.periodicProbeTO
		m.mu.RUnlock()
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
	}
	m.probeAllNodes(timeout)
}

// probeAllNodes checks all registered nodes concurrently.
func (m *Manager) probeAllNodes(timeout time.Duration) {
	m.mu.RLock()
	entries := make([]*entry, 0, len(m.nodes))
	for _, e := range m.nodes {
		entries = append(entries, e)
	}
	m.mu.RUnlock()

	if len(entries) == 0 {
		return
	}

	if m.logger != nil {
		m.logger.Info("starting health check for ", len(entries), " nodes")
	}

	workerLimit := runtime.NumCPU() * 2
	if workerLimit < 8 {
		workerLimit = 8
	}
	sem := make(chan struct{}, workerLimit)
	var wg sync.WaitGroup
	var availableCount atomic.Int32
	var failedCount atomic.Int32

	for _, e := range entries {
		e.mu.RLock()
		probeFn := e.probe
		tag := e.info.Tag
		e.mu.RUnlock()

		if probeFn == nil {
			continue
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(entry *entry, probe probeFunc, tag string) {
			defer wg.Done()
			defer func() { <-sem }()

			ctx, cancel := context.WithTimeout(m.ctx, timeout)
			latency, err := probe(ctx)
			cancel()

			entry.mu.Lock()
			if err != nil {
				failedCount.Add(1)
				entry.lastError = err.Error()
				entry.lastFail = time.Now()
				entry.available = false
				entry.initialCheckDone = true
			} else {
				availableCount.Add(1)
				entry.lastOK = time.Now()
				entry.lastProbe = latency
				entry.available = true
				entry.initialCheckDone = true
			}
			entry.mu.Unlock()
			m.persistEntry(entry)

			if err != nil && m.logger != nil {
				m.logger.Warn("probe failed for ", tag, ": ", err)
			}
		}(e, probeFn, tag)
	}
	wg.Wait()

	if m.logger != nil {
		m.logger.Info("health check completed: ", availableCount.Load(), " available, ", failedCount.Load(), " failed")
	}
}

// Stop stops the periodic health check.
func (m *Manager) Stop() {
	m.mu.Lock()
	if m.periodicProbeStop != nil {
		m.periodicProbeStop()
		m.periodicProbeStop = nil
	}
	m.mu.Unlock()
	if m.cancel != nil {
		m.cancel()
	}
}

func parsePort(value string) uint16 {
	p, err := strconv.Atoi(value)
	if err != nil || p <= 0 || p > 65535 {
		return 80
	}
	return uint16(p)
}

// Register ensures a node is tracked and returns its entry.
func (m *Manager) Register(info NodeInfo) *EntryHandle {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.nodes[info.Tag]
	if !ok {
		e = &entry{
			info:     info,
			timeline: make([]TimelineEvent, 0, maxTimelineSize),
		}
		m.nodes[info.Tag] = e
	} else {
		if e.info.URI != "" && info.URI != "" && e.info.URI != info.URI {
			e.resetRuntimeLocked()
		}
		e.info = info
	}
	if uri := strings.TrimSpace(info.URI); uri != "" {
		if st, found := m.persisted[uri]; found {
			e.restorePersistedState(st)
			delete(m.persisted, uri)
		}
	}
	return &EntryHandle{ref: e, mgr: m}
}

// ClearNodes removes all registered nodes. Call before re-registering
// during a config reload so stale entries don't persist in the dashboard.
func (m *Manager) ClearNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes = make(map[string]*entry)
}

// DestinationForProbe exposes the configured destination for health checks.
func (m *Manager) DestinationForProbe() (M.Socksaddr, bool) {
	if !m.probeReady {
		return M.Socksaddr{}, false
	}
	return m.probeDst, true
}

// ProbeRequestOptions returns probe request path and whether TLS should be used.
func (m *Manager) ProbeRequestOptions() (path string, useTLS bool) {
	if m == nil {
		return "/generate_204", false
	}
	path = strings.TrimSpace(m.probePath)
	if path == "" {
		path = "/generate_204"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path, m.probeTLS
}

// Snapshot returns a sorted copy of current node states.
// If onlyAvailable is true, only returns nodes that passed initial health check.
func (m *Manager) Snapshot() []Snapshot {
	return m.SnapshotFiltered(false)
}

// SnapshotFiltered returns a sorted copy of current node states.
// If onlyAvailable is true, only returns nodes that passed initial health check.
// Nodes that haven't been checked yet are also included (they will be checked on first use).
func (m *Manager) SnapshotFiltered(onlyAvailable bool) []Snapshot {
	m.mu.RLock()
	list := make([]*entry, 0, len(m.nodes))
	for _, e := range m.nodes {
		list = append(list, e)
	}
	m.mu.RUnlock()
	snapshots := make([]Snapshot, 0, len(list))
	for _, e := range list {
		snap := e.snapshot()
		// 如果只要可用节点：
		// - 跳过已完成检查但不可用的节点
		// - 保留未完成检查的节点（它们会在首次使用时被检查）
		if onlyAvailable && ((snap.InitialCheckDone && !snap.Available) || snap.Blacklisted) {
			continue
		}
		snapshots = append(snapshots, snap)
	}
	// 按延迟排序（延迟小的在前面，未测试的排在最后）
	sort.Slice(snapshots, func(i, j int) bool {
		latencyI := snapshots[i].LastLatencyMs
		latencyJ := snapshots[j].LastLatencyMs
		// -1 表示未测试，排在最后
		if latencyI < 0 && latencyJ < 0 {
			return snapshots[i].Name < snapshots[j].Name // 都未测试时按名称排序
		}
		if latencyI < 0 {
			return false // i 未测试，排在后面
		}
		if latencyJ < 0 {
			return true // j 未测试，i 排在前面
		}
		if latencyI == latencyJ {
			return snapshots[i].Name < snapshots[j].Name // 延迟相同时按名称排序
		}
		return latencyI < latencyJ
	})
	return snapshots
}

// Probe triggers a manual health check.
func (m *Manager) Probe(ctx context.Context, tag string) (time.Duration, error) {
	e, err := m.entry(tag)
	if err != nil {
		return 0, err
	}
	if e.probe == nil {
		return 0, errors.New("probe not available for this node")
	}
	latency, err := e.probe(ctx)
	if err != nil {
		return 0, err
	}
	e.recordProbeLatency(latency)
	m.persistEntry(e)
	return latency, nil
}

// Quality triggers an exit-IP/ASN quality lookup for a node.
func (m *Manager) Quality(ctx context.Context, tag string, force bool) (QualityInfo, error) {
	e, err := m.entry(tag)
	if err != nil {
		return QualityInfo{}, err
	}
	e.mu.RLock()
	fn := e.qualityProbe
	cached := e.quality
	e.mu.RUnlock()
	if fn == nil {
		return QualityInfo{}, errors.New("quality probe not available for this node")
	}
	enabled, _, _, cacheTTL := m.QualityConfig()
	if !enabled {
		return QualityInfo{}, errors.New("quality probe is disabled")
	}
	h := &EntryHandle{ref: e}
	if !h.BeginQualityProbe(cacheTTL, force) {
		if !force && !cached.CheckedAt.IsZero() {
			return cached, nil
		}
		return cached, errors.New("quality probe is already running or cached")
	}
	info, err := fn(ctx)
	h.FinishQualityProbe(info, err)
	if err != nil {
		return QualityInfo{}, err
	}
	return info, nil
}

// Release clears blacklist state for the given node.
func (m *Manager) Release(tag string) error {
	e, err := m.entry(tag)
	if err != nil {
		return err
	}
	if e.release == nil {
		return errors.New("release not available for this node")
	}
	e.release()
	m.persistEntry(e)
	return nil
}

// ManualBlacklist manually blacklists a node for the given duration.
func (m *Manager) ManualBlacklist(tag string, duration time.Duration) error {
	e, err := m.entry(tag)
	if err != nil {
		return err
	}
	e.mu.RLock()
	fn := e.blacklistFn
	e.mu.RUnlock()

	if fn != nil {
		// Blacklist in pool shared state (affects routing)
		fn(duration)
	}
	// Also mark in monitor state (affects UI display)
	e.blacklistUntil(time.Now().Add(duration))
	m.persistEntry(e)
	return nil
}

func (m *Manager) entry(tag string) (*entry, error) {
	m.mu.RLock()
	e, ok := m.nodes[tag]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("node %s not found", tag)
	}
	return e, nil
}

func (e *entry) snapshot() Snapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()

	latencyMs := int64(-1)
	if e.lastProbe > 0 {
		latencyMs = e.lastProbe.Milliseconds()
		if latencyMs == 0 {
			latencyMs = 1
		}
	}

	var timelineCopy []TimelineEvent
	if len(e.timeline) > 0 {
		timelineCopy = make([]TimelineEvent, len(e.timeline))
		copy(timelineCopy, e.timeline)
	}

	return Snapshot{
		NodeInfo:          e.info,
		FailureCount:      e.failure,
		SuccessCount:      e.success,
		Blacklisted:       e.blacklist,
		BlacklistedUntil:  e.until,
		ActiveConnections: e.active.Load(),
		LastError:         e.lastError,
		LastFailure:       e.lastFail,
		LastSuccess:       e.lastOK,
		LastProbeLatency:  e.lastProbe,
		LastLatencyMs:     latencyMs,
		Available:         e.available,
		InitialCheckDone:  e.initialCheckDone,
		Timeline:          timelineCopy,
		QualityInfo:       e.quality,
	}
}

func (e *entry) resetRuntimeLocked() {
	e.failure = 0
	e.success = 0
	e.timeline = make([]TimelineEvent, 0, maxTimelineSize)
	e.blacklist = false
	e.until = time.Time{}
	e.lastError = ""
	e.lastFail = time.Time{}
	e.lastOK = time.Time{}
	e.lastProbe = 0
	e.initialCheckDone = false
	e.available = false
	e.quality = QualityInfo{}
	e.qualityChecking = false
	e.qualityAttempt = time.Time{}
}

func (e *entry) restorePersistedState(st PersistedState) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.failure = st.FailureCount
	e.blacklist = st.Blacklisted
	e.until = st.BlacklistedUntil
	e.lastError = st.LastError
	e.lastFail = st.LastFailure
	e.lastOK = st.LastSuccess
	e.initialCheckDone = st.InitialCheckDone
	e.available = st.Available
	if st.LastLatencyMs > 0 {
		e.lastProbe = time.Duration(st.LastLatencyMs) * time.Millisecond
	} else {
		e.lastProbe = 0
	}
	e.quality = QualityInfo{
		ExitIP:          st.ExitIP,
		IPValid:         st.IPValid,
		IPVersion:       st.IPVersion,
		IPType:          st.IPType,
		IPInvalidReason: st.IPInvalidReason,
		CountryCode:     st.CountryCode,
		Country:         st.Country,
		ASN:             st.ASN,
		ASName:          st.ASName,
		ISP:             st.ISP,
		Org:             st.Org,
		ProxyType:       st.ProxyType,
		QualitySource:   st.QualitySource,
		Mobile:          st.Mobile,
		Hosting:         st.Hosting,
		Proxy:           st.Proxy,
		CheckedAt:       st.QualityCheckedAt,
		Error:           st.QualityError,
	}
}

func (e *entry) persistedState() PersistedState {
	e.mu.RLock()
	defer e.mu.RUnlock()

	latencyMs := int64(-1)
	if e.lastProbe > 0 {
		latencyMs = e.lastProbe.Milliseconds()
		if latencyMs == 0 {
			latencyMs = 1
		}
	}

	return PersistedState{
		Available:        e.available,
		InitialCheckDone: e.initialCheckDone,
		FailureCount:     e.failure,
		Blacklisted:      e.blacklist,
		BlacklistedUntil: e.until,
		LastError:        e.lastError,
		LastLatencyMs:    latencyMs,
		LastSuccess:      e.lastOK,
		LastFailure:      e.lastFail,
		CountryCode:      e.quality.CountryCode,
		Country:          e.quality.Country,
		ExitIP:           e.quality.ExitIP,
		ProxyType:        e.quality.ProxyType,
		QualitySource:    e.quality.QualitySource,
		QualityError:     e.quality.Error,
		QualityCheckedAt: e.quality.CheckedAt,
		IPValid:          e.quality.IPValid,
		IPVersion:        e.quality.IPVersion,
		IPType:           e.quality.IPType,
		IPInvalidReason:  e.quality.IPInvalidReason,
		ASN:              e.quality.ASN,
		ASName:           e.quality.ASName,
		ISP:              e.quality.ISP,
		Org:              e.quality.Org,
		Mobile:           e.quality.Mobile,
		Hosting:          e.quality.Hosting,
		Proxy:            e.quality.Proxy,
	}
}

func (m *Manager) persistEntry(e *entry) {
	if m == nil || e == nil {
		return
	}
	m.mu.RLock()
	store := m.stateStore
	m.mu.RUnlock()
	if store == nil {
		return
	}
	uri := strings.TrimSpace(e.info.URI)
	if uri == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := store.SaveRuntimeState(ctx, uri, e.persistedState()); err != nil && m.logger != nil {
		m.logger.Warn("persist runtime state failed for ", uri, ": ", err)
	}
}

func (e *entry) recordFailure(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	errStr := err.Error()
	e.failure++
	e.lastError = errStr
	e.lastFail = time.Now()
	e.appendTimelineLocked(false, 0, errStr)
}

func (e *entry) recordSuccess() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.success++
	e.lastOK = time.Now()
	e.appendTimelineLocked(true, 0, "")
}

func (e *entry) recordSuccessWithLatency(latency time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.success++
	e.lastOK = time.Now()
	e.lastProbe = latency
	latencyMs := latency.Milliseconds()
	if latencyMs == 0 && latency > 0 {
		latencyMs = 1
	}
	e.appendTimelineLocked(true, latencyMs, "")
}

func (e *entry) appendTimelineLocked(success bool, latencyMs int64, errStr string) {
	evt := TimelineEvent{
		Time:      time.Now(),
		Success:   success,
		LatencyMs: latencyMs,
		Error:     errStr,
	}
	if len(e.timeline) >= maxTimelineSize {
		copy(e.timeline, e.timeline[1:])
		e.timeline[len(e.timeline)-1] = evt
	} else {
		e.timeline = append(e.timeline, evt)
	}
}

func (e *entry) blacklistUntil(until time.Time) {
	e.mu.Lock()
	e.blacklist = true
	e.until = until
	e.mu.Unlock()
}

func (e *entry) clearBlacklist() {
	e.mu.Lock()
	e.blacklist = false
	e.until = time.Time{}
	e.mu.Unlock()
}

func (e *entry) incActive() {
	e.active.Add(1)
}

func (e *entry) decActive() {
	e.active.Add(-1)
}

func (e *entry) setProbe(fn probeFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.probe = fn
}

func (e *entry) setQualityProbe(fn qualityFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.qualityProbe = fn
}

func (e *entry) setRelease(fn releaseFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.release = fn
}

func (e *entry) recordProbeLatency(d time.Duration) {
	e.mu.Lock()
	e.lastProbe = d
	e.mu.Unlock()
}

// BeginQualityProbe reserves a quality lookup slot for this node.
// It returns false when a lookup is already running or the cached result is still fresh.
func (h *EntryHandle) BeginQualityProbe(interval time.Duration, force bool) bool {
	if h == nil || h.ref == nil {
		return false
	}
	if interval <= 0 {
		interval = 7 * 24 * time.Hour
	}
	now := time.Now()
	h.ref.mu.Lock()
	defer h.ref.mu.Unlock()
	if h.ref.qualityChecking {
		return false
	}
	last := h.ref.qualityAttempt
	if h.ref.quality.CheckedAt.After(last) {
		last = h.ref.quality.CheckedAt
	}
	if !force && !last.IsZero() && now.Sub(last) < interval {
		return false
	}
	h.ref.qualityChecking = true
	h.ref.qualityAttempt = now
	return true
}

// FinishQualityProbe stores the result of an exit-IP/ASN quality lookup.
func (h *EntryHandle) FinishQualityProbe(info QualityInfo, err error) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.mu.Lock()
	defer h.ref.mu.Unlock()
	h.ref.qualityChecking = false
	if err != nil {
		h.ref.quality.Error = err.Error()
		if h.mgr != nil {
			h.mgr.persistEntry(h.ref)
		}
		return
	}
	if info.CheckedAt.IsZero() {
		info.CheckedAt = time.Now()
	}
	if info.ProxyType == "" {
		info.ProxyType = "unknown"
	}
	h.ref.quality = info
	if h.mgr != nil {
		h.mgr.persistEntry(h.ref)
	}
}

func (h *EntryHandle) LastLatencyMs() int64 {
	if h == nil || h.ref == nil {
		return -1
	}
	h.ref.mu.RLock()
	defer h.ref.mu.RUnlock()
	if h.ref.lastProbe <= 0 {
		return -1
	}
	latencyMs := h.ref.lastProbe.Milliseconds()
	if latencyMs == 0 {
		return 1
	}
	return latencyMs
}

// RecordFailure updates failure counters.
func (h *EntryHandle) RecordFailure(err error) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.recordFailure(err)
	if h.mgr != nil {
		h.mgr.persistEntry(h.ref)
	}
}

// RecordSuccess updates the last success timestamp.
func (h *EntryHandle) RecordSuccess() {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.recordSuccess()
	if h.mgr != nil {
		h.mgr.persistEntry(h.ref)
	}
}

// RecordSuccessWithLatency updates the last success timestamp and latency.
func (h *EntryHandle) RecordSuccessWithLatency(latency time.Duration) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.recordSuccessWithLatency(latency)
	if h.mgr != nil {
		h.mgr.persistEntry(h.ref)
	}
}

// Blacklist marks the node unavailable until the given deadline.
func (h *EntryHandle) Blacklist(until time.Time) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.blacklistUntil(until)
	if h.mgr != nil {
		h.mgr.persistEntry(h.ref)
	}
}

// ClearBlacklist removes the blacklist flag.
func (h *EntryHandle) ClearBlacklist() {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.clearBlacklist()
	if h.mgr != nil {
		h.mgr.persistEntry(h.ref)
	}
}

// IncActive increments the active connection counter.
func (h *EntryHandle) IncActive() {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.incActive()
}

// DecActive decrements the active connection counter.
func (h *EntryHandle) DecActive() {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.decActive()
}

// SetProbe assigns a probe function.
func (h *EntryHandle) SetProbe(fn func(ctx context.Context) (time.Duration, error)) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.setProbe(fn)
}

// SetQualityProbe assigns an exit-IP/ASN quality probe function.
func (h *EntryHandle) SetQualityProbe(fn func(ctx context.Context) (QualityInfo, error)) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.setQualityProbe(fn)
}

// SetRelease assigns a release function.
func (h *EntryHandle) SetRelease(fn func()) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.setRelease(fn)
}

// SetBlacklistFn assigns a manual blacklist function.
func (h *EntryHandle) SetBlacklistFn(fn func(time.Duration)) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.mu.Lock()
	h.ref.blacklistFn = fn
	h.ref.mu.Unlock()
}

// MarkInitialCheckDone marks the initial health check as completed.
func (h *EntryHandle) MarkInitialCheckDone(available bool) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.mu.Lock()
	h.ref.initialCheckDone = true
	h.ref.available = available
	h.ref.mu.Unlock()
	if h.mgr != nil {
		h.mgr.persistEntry(h.ref)
	}
}

// MarkAvailable updates the availability status.
func (h *EntryHandle) MarkAvailable(available bool) {
	if h == nil || h.ref == nil {
		return
	}
	h.ref.mu.Lock()
	h.ref.available = available
	h.ref.mu.Unlock()
	if h.mgr != nil {
		h.mgr.persistEntry(h.ref)
	}
}
