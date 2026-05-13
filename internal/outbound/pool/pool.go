package pool

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"proxyweave/internal/config"
	"proxyweave/internal/monitor"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	singlog "github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

const (
	// Type is the outbound type name exposed to sing-box.
	Type = "pool"
	// Tag is the default outbound tag used by builder.
	Tag = "proxy-pool"

	modeSequential = "sequential"
	modeRandom     = "random"
	modeBalance    = "balance"

	qualityProbeHost = "ip-api.com"
)

var qualityProbeSem = make(chan struct{}, 4)

// Options controls pool outbound behaviour.
type Options struct {
	Mode               string
	Members            []string
	FailureThreshold   int
	BlacklistDuration  time.Duration
	StartupHealthCheck string
	MinAvailable       int
	Metadata           map[string]MemberMeta
}

// MemberMeta carries optional descriptive information for monitoring UI.
type MemberMeta struct {
	Name          string
	URI           string
	Mode          string
	ListenAddress string
	Port          uint16
	Region        string // GeoIP region code: lowercase ISO country code (e.g. "jp", "us", "de"), fallback "other"
	Country       string // Full country name from GeoIP
}

// Register wires the pool outbound into the registry.
func Register(registry *outbound.Registry) {
	outbound.Register[Options](registry, Type, newPool)
}

type memberState struct {
	outbound adapter.Outbound
	tag      string
	entry    *monitor.EntryHandle
	shared   *sharedMemberState
}

type poolOutbound struct {
	outbound.Adapter
	ctx            context.Context
	logger         singlog.ContextLogger
	manager        adapter.OutboundManager
	options        Options
	mode           string
	members        []*memberState
	mu             sync.Mutex
	rrCounter      atomic.Uint32
	rng            *rand.Rand
	rngMu          sync.Mutex // protects rng for random mode
	monitor        *monitor.Manager
	candidatesPool sync.Pool
}

func newPool(ctx context.Context, _ adapter.Router, logger singlog.ContextLogger, tag string, options Options) (adapter.Outbound, error) {
	if len(options.Members) == 0 {
		return nil, E.New("pool requires at least one member")
	}
	manager := service.FromContext[adapter.OutboundManager](ctx)
	if manager == nil {
		return nil, E.New("missing outbound manager in context")
	}
	monitorMgr := monitor.FromContext(ctx)
	normalized := normalizeOptions(options)
	memberCount := len(normalized.Members)
	p := &poolOutbound{
		Adapter: outbound.NewAdapter(Type, tag, []string{N.NetworkTCP, N.NetworkUDP}, normalized.Members),
		ctx:     ctx,
		logger:  logger,
		manager: manager,
		options: normalized,
		mode:    normalized.Mode,
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
		monitor: monitorMgr,
		candidatesPool: sync.Pool{
			New: func() any {
				return make([]*memberState, 0, memberCount)
			},
		},
	}

	// Register nodes immediately if monitor is available
	if monitorMgr != nil {
		logger.Info("registering ", len(normalized.Members), " nodes to monitor")
		for _, memberTag := range normalized.Members {
			// Acquire shared state for this tag (creates if not exists)
			state := acquireSharedState(memberTag)

			meta := normalized.Metadata[memberTag]
			info := monitor.NodeInfo{
				Tag:           memberTag,
				Name:          meta.Name,
				URI:           meta.URI,
				Mode:          meta.Mode,
				ListenAddress: meta.ListenAddress,
				Port:          meta.Port,
				Region:        meta.Region,
				Country:       meta.Country,
			}
			entry := monitorMgr.Register(info)
			if entry != nil {
				// Attach entry to shared state so all pool instances share it
				state.attachEntry(entry)
				logger.Info("registered node: ", memberTag)
				// Set probe, release, and blacklist functions immediately
				entry.SetRelease(p.makeReleaseByTagFunc(memberTag))
				entry.SetBlacklistFn(p.makeBlacklistByTagFunc(memberTag))
				if probeFn := p.makeProbeByTagFunc(memberTag); probeFn != nil {
					entry.SetProbe(probeFn)
				}
				if qualityFn := p.makeQualityByTagFunc(memberTag); qualityFn != nil {
					entry.SetQualityProbe(qualityFn)
				}
			} else {
				logger.Warn("failed to register node: ", memberTag)
			}
		}
	} else {
		logger.Warn("monitor manager is nil, skipping node registration")
	}

	// Register this pool outbound in the dialer registry for GeoIP router
	registerDialer(tag, p)

	return p, nil
}

func normalizeOptions(options Options) Options {
	if options.FailureThreshold <= 0 {
		options.FailureThreshold = 3
	}
	if options.BlacklistDuration <= 0 {
		options.BlacklistDuration = 24 * time.Hour
	}
	if options.Metadata == nil {
		options.Metadata = make(map[string]MemberMeta)
	}
	switch strings.ToLower(options.Mode) {
	case modeRandom:
		options.Mode = modeRandom
	case modeBalance:
		options.Mode = modeBalance
	case "latency":
		// 已废弃：自动回退到随机，避免继续命中坏节点。
		options.Mode = modeRandom
	default:
		options.Mode = modeSequential
	}
	return options
}

func (p *poolOutbound) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	p.mu.Lock()
	err := p.initializeMembersLocked()
	p.mu.Unlock()
	if err != nil {
		return err
	}
	// 在初始化完成后，立即在后台触发健康检查
	if p.monitor != nil && config.ShouldRunStartupHealthCheck(
		p.options.StartupHealthCheck,
		p.options.MinAvailable,
	) {
		go p.probeAllMembersOnStartup()
	}
	return nil
}

// initializeMembersLocked must be called with p.mu held
func (p *poolOutbound) initializeMembersLocked() error {
	if len(p.members) > 0 {
		return nil // Already initialized
	}

	members := make([]*memberState, 0, len(p.options.Members))
	for _, tag := range p.options.Members {
		detour, loaded := p.manager.Outbound(tag)
		if !loaded {
			return E.New("pool member not found: ", tag)
		}

		// Acquire shared state (creates if not exists, reuses if already created)
		state := acquireSharedState(tag)

		member := &memberState{
			outbound: detour,
			tag:      tag,
			shared:   state,
			entry:    state.entryHandle(),
		}

		// Connect to existing monitor entry if available
		if p.monitor != nil {
			meta := p.options.Metadata[tag]
			info := monitor.NodeInfo{
				Tag:           tag,
				Name:          meta.Name,
				URI:           meta.URI,
				Mode:          meta.Mode,
				ListenAddress: meta.ListenAddress,
				Port:          meta.Port,
				Region:        meta.Region,
				Country:       meta.Country,
			}
			entry := p.monitor.Register(info)
			if entry != nil {
				state.attachEntry(entry)
				member.entry = entry
				entry.SetRelease(p.makeReleaseFunc(member))
				entry.SetBlacklistFn(p.makeBlacklistByTagFunc(member.tag))
				if probe := p.makeProbeFunc(member); probe != nil {
					entry.SetProbe(probe)
				}
				if qualityFn := p.makeQualityFunc(member); qualityFn != nil {
					entry.SetQualityProbe(qualityFn)
				}
			}
		}
		members = append(members, member)
	}
	p.members = members
	p.logger.Info("pool initialized with ", len(members), " members")

	return nil
}

// probeAllMembersOnStartup performs initial health checks on all members
func (p *poolOutbound) probeAllMembersOnStartup() {
	destination, ok := p.monitor.DestinationForProbe()
	if !ok {
		p.logger.Warn("probe target not configured, skipping initial health check")
		// 没有配置探测目标时，标记所有节点为可用
		p.mu.Lock()
		for _, member := range p.members {
			if member.entry != nil {
				member.entry.MarkInitialCheckDone(true)
			}
		}
		p.mu.Unlock()
		return
	}

	p.logger.Info("starting initial health check for all nodes")

	p.mu.Lock()
	members := make([]*memberState, len(p.members))
	copy(members, p.members)
	p.mu.Unlock()
	probePath, probeTLS := p.monitor.ProbeRequestOptions()

	// Concurrent probing with bounded workers
	const maxWorkers = 20
	type probeResult struct {
		member  *memberState
		success bool
		latency time.Duration
		err     error
	}

	results := make(chan probeResult, len(members))
	sem := make(chan struct{}, maxWorkers)

	for _, member := range members {
		sem <- struct{}{} // acquire worker slot
		go func(m *memberState) {
			defer func() { <-sem }() // release worker slot

			ctx, cancel := context.WithTimeout(p.ctx, 15*time.Second)
			defer cancel()

			start := time.Now()
			conn, err := m.outbound.DialContext(ctx, N.NetworkTCP, destination)
			if err != nil {
				results <- probeResult{member: m, err: err}
				return
			}

			_, err = httpProbe(conn, destination.AddrString(), probePath, probeTLS)
			conn.Close()
			if err != nil {
				results <- probeResult{member: m, err: err}
				return
			}

			results <- probeResult{member: m, success: true, latency: time.Since(start)}
		}(member)
	}

	// Collect results
	availableCount := 0
	failedCount := 0
	for i := 0; i < len(members); i++ {
		res := <-results
		if res.err != nil {
			p.logger.Warn("initial probe failed for ", res.member.tag, ": ", res.err)
			failedCount++
			if res.member.shared != nil {
				res.member.shared.recordFailure(res.err, 1, p.options.BlacklistDuration)
			} else if res.member.entry != nil {
				res.member.entry.RecordFailure(res.err)
			}
			if res.member.entry != nil {
				res.member.entry.MarkInitialCheckDone(false)
			}
		} else {
			latencyMs := res.latency.Milliseconds()
			p.logger.Info("initial probe success for ", res.member.tag, ", latency: ", latencyMs, "ms")
			availableCount++
			if res.member.entry != nil {
				res.member.entry.RecordSuccessWithLatency(res.latency)
				res.member.entry.MarkInitialCheckDone(true)
			}
			p.maybeProbeQuality(res.member)
		}
	}

	p.logger.Info("initial health check completed: ", availableCount, " available, ", failedCount, " failed")
}

func (p *poolOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	member, err := p.pickMember(network)
	if err != nil {
		return nil, err
	}
	p.incActive(member)
	conn, err := member.outbound.DialContext(ctx, network, destination)
	if err != nil {
		p.decActive(member)
		p.recordFailure(member, err)
		return nil, err
	}
	p.recordSuccess(member)
	return p.wrapConn(conn, member), nil
}

func (p *poolOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	member, err := p.pickMember(N.NetworkUDP)
	if err != nil {
		return nil, err
	}
	p.incActive(member)
	conn, err := member.outbound.ListenPacket(ctx, destination)
	if err != nil {
		p.decActive(member)
		p.recordFailure(member, err)
		return nil, err
	}
	p.recordSuccess(member)
	return p.wrapPacketConn(conn, member), nil
}

func (p *poolOutbound) pickMember(network string) (*memberState, error) {
	now := time.Now()
	candidates := p.getCandidateBuffer()

	p.mu.Lock()
	if len(p.members) == 0 {
		if err := p.initializeMembersLocked(); err != nil {
			p.mu.Unlock()
			p.putCandidateBuffer(candidates)
			return nil, err
		}
	}
	candidates = p.availableMembersLocked(now, network, candidates)
	p.mu.Unlock()

	if len(candidates) == 0 {
		p.mu.Lock()
		if p.releaseIfAllBlacklistedLocked(now) {
			candidates = p.availableMembersLocked(now, network, candidates)
		}
		p.mu.Unlock()
	}

	if len(candidates) == 0 {
		p.putCandidateBuffer(candidates)
		return nil, E.New("no healthy proxy available")
	}

	member := p.selectMember(candidates)
	p.putCandidateBuffer(candidates)
	return member, nil
}

func (p *poolOutbound) availableMembersLocked(now time.Time, network string, buf []*memberState) []*memberState {
	result := buf[:0]
	for _, member := range p.members {
		// Check blacklist via shared state (auto-clears if expired)
		if member.shared != nil && member.shared.isBlacklisted(now) {
			continue
		}
		if network != "" && !common.Contains(member.outbound.Network(), network) {
			continue
		}
		result = append(result, member)
	}
	return result
}

func (p *poolOutbound) releaseIfAllBlacklistedLocked(now time.Time) bool {
	if len(p.members) == 0 {
		return false
	}
	// Check if all members are blacklisted
	for _, member := range p.members {
		if member.shared == nil || !member.shared.isBlacklisted(now) {
			return false
		}
	}
	// All blacklisted, force release all
	for _, member := range p.members {
		if member.shared != nil {
			member.shared.forceRelease()
		}
	}
	p.logger.Warn("all upstream proxies were blacklisted, releasing them for retry")
	return true
}

func (p *poolOutbound) selectMember(candidates []*memberState) *memberState {
	switch p.mode {
	case modeRandom:
		p.rngMu.Lock()
		idx := p.rng.Intn(len(candidates))
		p.rngMu.Unlock()
		return candidates[idx]
	case modeBalance:
		return selectLeastActive(candidates)
	default:
		idx := int(p.rrCounter.Add(1)-1) % len(candidates)
		return candidates[idx]
	}
}

func selectLeastActive(candidates []*memberState) *memberState {
	var selected *memberState
	var minActive int32
	for _, member := range candidates {
		active := memberActiveCount(member)
		if selected == nil || active < minActive {
			selected = member
			minActive = active
		}
	}
	return selected
}

func selectLowestLatency(candidates []*memberState) *memberState {
	var selected *memberState
	selectedLatency := int64(-1)
	selectedActive := int32(0)
	for _, member := range candidates {
		latency := memberLatencyMs(member)
		active := memberActiveCount(member)
		if selected == nil || betterLatencyCandidate(latency, active, selectedLatency, selectedActive) {
			selected = member
			selectedLatency = latency
			selectedActive = active
		}
	}
	return selected
}

func betterLatencyCandidate(latency int64, active int32, selectedLatency int64, selectedActive int32) bool {
	if latency >= 0 && selectedLatency < 0 {
		return true
	}
	if latency < 0 && selectedLatency >= 0 {
		return false
	}
	if latency >= 0 && selectedLatency >= 0 && latency != selectedLatency {
		return latency < selectedLatency
	}
	return active < selectedActive
}

func memberActiveCount(member *memberState) int32 {
	if member == nil || member.shared == nil {
		return 0
	}
	return member.shared.activeCount()
}

func memberLatencyMs(member *memberState) int64 {
	if member == nil || member.entry == nil {
		return -1
	}
	return member.entry.LastLatencyMs()
}

func (p *poolOutbound) recordFailure(member *memberState, cause error) {
	if member.shared == nil {
		p.logger.Warn("proxy ", member.tag, " failure (no shared state): ", cause)
		return
	}
	failures, blacklisted, _ := member.shared.recordFailure(cause, p.options.FailureThreshold, p.options.BlacklistDuration)
	if blacklisted {
		p.logger.Warn("proxy ", member.tag, " blacklisted for ", p.options.BlacklistDuration, ": ", cause)
		log.Printf("[pool] %s blacklisted for %s: %v", member.tag, p.options.BlacklistDuration, cause)
	} else {
		p.logger.Warn("proxy ", member.tag, " failure ", failures, "/", p.options.FailureThreshold, ": ", cause)
		log.Printf("[pool] %s failure %d/%d: %v", member.tag, failures, p.options.FailureThreshold, cause)
	}
}

func (p *poolOutbound) recordSuccess(member *memberState) {
	if member.shared != nil {
		member.shared.recordSuccess()
	}
}

func (p *poolOutbound) wrapConn(conn net.Conn, member *memberState) net.Conn {
	var onWrite, onRead func(int64)
	if member.shared != nil {
		onWrite = func(n int64) { member.shared.addUpload(n) }
		onRead = func(n int64) { member.shared.addDownload(n) }
	}
	return &trackedConn{Conn: conn, release: func() {
		p.decActive(member)
	}, onWrite: onWrite, onRead: onRead}
}

func (p *poolOutbound) wrapPacketConn(conn net.PacketConn, member *memberState) net.PacketConn {
	return &trackedPacketConn{PacketConn: conn, release: func() {
		p.decActive(member)
	}}
}

func (p *poolOutbound) makeReleaseFunc(member *memberState) func() {
	return func() {
		if member.shared != nil {
			member.shared.forceRelease()
		}
	}
}

// httpProbe performs an HTTP(S) probe and measures TTFB.
// It validates the response status line to avoid false-positive "success".
func httpProbe(conn net.Conn, host, path string, useTLS bool) (time.Duration, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		path = "/generate_204"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	hostHeader := host
	serverName := host
	if h, _, err := net.SplitHostPort(host); err == nil && strings.TrimSpace(h) != "" {
		serverName = h
	}

	// For HTTPS probe targets, establish TLS over the already-proxied TCP stream.
	stream := conn
	if useTLS {
		tlsConn := tls.Client(conn, &tls.Config{
			ServerName:         serverName,
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true, // health check should focus on connectivity, not certificate trust
		})
		_ = tlsConn.SetDeadline(time.Now().Add(10 * time.Second))
		if err := tlsConn.Handshake(); err != nil {
			return 0, fmt.Errorf("tls handshake: %w", err)
		}
		stream = tlsConn
	}

	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\nUser-Agent: ProxyWeave-HealthCheck/1.0\r\nAccept: */*\r\n\r\n", path, hostHeader)

	_ = stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
	start := time.Now()
	if _, err := stream.Write([]byte(req)); err != nil {
		return 0, fmt.Errorf("write request: %w", err)
	}

	_ = stream.SetReadDeadline(time.Now().Add(10 * time.Second))
	reader := bufio.NewReader(stream)
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		return 0, fmt.Errorf("read response: %w", err)
	}
	ttfb := time.Since(start)

	parts := strings.Fields(strings.TrimSpace(statusLine))
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid response status line: %q", strings.TrimSpace(statusLine))
	}
	code, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, fmt.Errorf("invalid status code in response line: %q", strings.TrimSpace(statusLine))
	}
	if isNoContentProbePath(path) && code != http.StatusNoContent {
		return 0, fmt.Errorf("probe target expected status 204, got %d", code)
	}
	if code < 200 || code >= 400 {
		return 0, fmt.Errorf("probe target returned status %d", code)
	}
	return ttfb, nil
}

func isNoContentProbePath(path string) bool {
	path = strings.TrimSpace(path)
	if path == "" {
		path = "/generate_204"
	}
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	return strings.HasSuffix(strings.TrimRight(path, "/"), "/generate_204")
}

type ipAPIResponse struct {
	Status      string `json:"status"`
	Message     string `json:"message"`
	Query       string `json:"query"`
	Country     string `json:"country"`
	CountryCode string `json:"countryCode"`
	AS          string `json:"as"`
	ASName      string `json:"asname"`
	ISP         string `json:"isp"`
	Org         string `json:"org"`
	Mobile      bool   `json:"mobile"`
	Hosting     bool   `json:"hosting"`
	Proxy       bool   `json:"proxy"`
}

func (p *poolOutbound) maybeProbeQuality(member *memberState) {
	if member == nil || member.entry == nil || member.outbound == nil || p.monitor == nil {
		return
	}
	enabled, provider, apiKey, cacheTTL := p.monitor.QualityConfig()
	if !enabled {
		return
	}
	if !member.entry.BeginQualityProbe(cacheTTL, false) {
		return
	}
	go func() {
		select {
		case qualityProbeSem <- struct{}{}:
			defer func() { <-qualityProbeSem }()
		case <-p.ctx.Done():
			member.entry.FinishQualityProbe(monitor.QualityInfo{}, p.ctx.Err())
			return
		}

		ctx, cancel := context.WithTimeout(p.ctx, 15*time.Second)
		defer cancel()

		info, err := probeOutboundQuality(ctx, member.outbound, provider, apiKey)
		member.entry.FinishQualityProbe(info, err)
		if err != nil {
			p.logger.Warn("quality probe failed for ", member.tag, ": ", err)
			return
		}
		p.logger.Info("quality probe success for ", member.tag, ": ", info.ExitIP, " ", info.ProxyType, " ", info.ISP)
	}()
}

func probeOutboundQuality(ctx context.Context, ob adapter.Outbound, provider, apiKey string) (monitor.QualityInfo, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "" {
		provider = "auto"
	}
	switch provider {
	case "ipinfo_dkly", "ipinfo-dkly", "dkly":
		return probeIPInfoDKLYQuality(ctx, ob, apiKey)
	case "ip-api", "ipapi":
		return probeIPAPIQuality(ctx, ob)
	case "ipinfo", "ipinfo_lite", "ipinfo-lite":
		return probeIPInfoLiteQuality(ctx, ob, apiKey)
	case "auto":
		if strings.TrimSpace(apiKey) != "" {
			info, err := probeIPInfoDKLYQuality(ctx, ob, apiKey)
			if err == nil {
				return info, nil
			}
			info2, fallbackErr := probeIPAPIQuality(ctx, ob)
			if fallbackErr == nil {
				return info2, nil
			}
			info3, fallbackErr2 := probeIPInfoLiteQuality(ctx, ob, apiKey)
			if fallbackErr2 == nil {
				return info3, nil
			}
			return monitor.QualityInfo{}, fmt.Errorf("ipinfo_dkly: %v; ip-api: %v; ipinfo_lite: %v", err, fallbackErr, fallbackErr2)
		}
		return probeIPAPIQuality(ctx, ob)
	default:
		return probeIPInfoDKLYQuality(ctx, ob, apiKey)
	}
}

func probeIPAPIQuality(ctx context.Context, ob adapter.Outbound) (monitor.QualityInfo, error) {
	req := "GET /json/?fields=status,message,query,country,countryCode,as,asname,isp,org,mobile,hosting,proxy HTTP/1.1\r\n" +
		"Host: " + qualityProbeHost + "\r\n" +
		"Connection: close\r\n" +
		"User-Agent: proxyweave/quality-probe\r\n\r\n"
	body, status, err := doOutboundHTTPRequest(ctx, ob, qualityProbeHost, 80, false, req)
	if err != nil {
		return monitor.QualityInfo{}, err
	}
	if status < 200 || status >= 300 {
		return monitor.QualityInfo{}, fmt.Errorf("quality api status: %d", status)
	}

	var api ipAPIResponse
	if err := json.Unmarshal(body, &api); err != nil {
		return monitor.QualityInfo{}, fmt.Errorf("decode quality response: %w", err)
	}
	if api.Status != "" && api.Status != "success" {
		if api.Message == "" {
			api.Message = api.Status
		}
		return monitor.QualityInfo{}, fmt.Errorf("quality api error: %s", api.Message)
	}

	asn := extractASN(api.AS)
	ipValid, ipVersion, ipType, ipReason := validateExitIP(api.Query)
	return monitor.QualityInfo{
		ExitIP:          api.Query,
		IPValid:         ipValid,
		IPVersion:       ipVersion,
		IPType:          ipType,
		IPInvalidReason: ipReason,
		CountryCode:     api.CountryCode,
		Country:         api.Country,
		ASN:             asn,
		ASName:          api.ASName,
		ISP:             api.ISP,
		Org:             api.Org,
		ProxyType:       classifyProxyType(api),
		QualitySource:   qualityProbeHost,
		Mobile:          api.Mobile,
		Hosting:         api.Hosting,
		Proxy:           api.Proxy,
		CheckedAt:       time.Now(),
	}, nil
}

type ipInfoLiteResponse struct {
	IP          string `json:"ip"`
	ASN         string `json:"asn"`
	ASName      string `json:"as_name"`
	ASDomain    string `json:"as_domain"`
	CountryCode string `json:"country_code"`
	Country     string `json:"country"`
}

type ipInfoDKLYResponse struct {
	IP      string `json:"ip"`
	Type    string `json:"type"`
	Company struct {
		Domain string `json:"domain"`
		Name   string `json:"name"`
		Type   string `json:"type"`
	} `json:"company"`
	Connection struct {
		ASN          int    `json:"asn"`
		Domain       string `json:"domain"`
		Organization string `json:"organization"`
		Route        string `json:"route"`
		Type         string `json:"type"`
	} `json:"connection"`
	Location struct {
		Country struct {
			Code string `json:"code"`
			Name string `json:"name"`
		} `json:"country"`
		Region struct {
			Code string `json:"code"`
			Name string `json:"name"`
		} `json:"region"`
		City string `json:"city"`
	} `json:"location"`
	Security struct {
		IsAbuser        bool `json:"is_abuser"`
		IsAttacker      bool `json:"is_attacker"`
		IsBogon         bool `json:"is_bogon"`
		IsCloudProvider bool `json:"is_cloud_provider"`
		IsProxy         bool `json:"is_proxy"`
		IsRelay         bool `json:"is_relay"`
		IsTor           bool `json:"is_tor"`
		IsTorExit       bool `json:"is_tor_exit"`
		IsVPN           bool `json:"is_vpn"`
		IsAnonymous     bool `json:"is_anonymous"`
		IsThreat        bool `json:"is_threat"`
	} `json:"security"`
}

func probeIPInfoLiteQuality(ctx context.Context, ob adapter.Outbound, apiKey string) (monitor.QualityInfo, error) {
	exitIP, err := probeExitIP(ctx, ob)
	if err != nil {
		return monitor.QualityInfo{}, err
	}
	path := "/lite/" + url.PathEscape(exitIP)
	if strings.TrimSpace(apiKey) != "" {
		path += "?token=" + url.QueryEscape(strings.TrimSpace(apiKey))
	}
	req := "GET " + path + " HTTP/1.1\r\n" +
		"Host: api.ipinfo.io\r\n" +
		"Connection: close\r\n" +
		"User-Agent: proxyweave/quality-probe\r\n\r\n"
	body, status, err := doOutboundHTTPRequest(ctx, ob, "api.ipinfo.io", 443, true, req)
	if err != nil {
		return monitor.QualityInfo{}, err
	}
	if status < 200 || status >= 300 {
		return monitor.QualityInfo{}, fmt.Errorf("ipinfo lite status: %d", status)
	}
	var api ipInfoLiteResponse
	if err := json.Unmarshal(body, &api); err != nil {
		return monitor.QualityInfo{}, fmt.Errorf("decode ipinfo lite response: %w", err)
	}
	if api.IP == "" {
		api.IP = exitIP
	}
	text := strings.ToLower(strings.Join([]string{api.ASN, api.ASName, api.ASDomain}, " "))
	ipValid, ipVersion, ipType, ipReason := validateExitIP(api.IP)
	return monitor.QualityInfo{
		ExitIP:          api.IP,
		IPValid:         ipValid,
		IPVersion:       ipVersion,
		IPType:          ipType,
		IPInvalidReason: ipReason,
		CountryCode:     api.CountryCode,
		Country:         api.Country,
		ASN:             api.ASN,
		ASName:          api.ASName,
		ISP:             api.ASName,
		Org:             api.ASDomain,
		ProxyType:       classifyTextProxyType(text),
		QualitySource:   "ipinfo_lite",
		CheckedAt:       time.Now(),
	}, nil
}

func probeIPInfoDKLYQuality(ctx context.Context, ob adapter.Outbound, apiKey string) (monitor.QualityInfo, error) {
	if strings.TrimSpace(apiKey) == "" {
		return monitor.QualityInfo{}, fmt.Errorf("ipinfo dkly api key is required")
	}
	exitIP, err := probeExitIP(ctx, ob)
	if err != nil {
		return monitor.QualityInfo{}, err
	}
	path := "/api/?key=" + url.QueryEscape(strings.TrimSpace(apiKey)) + "&ip=" + url.QueryEscape(exitIP)
	req := "GET " + path + " HTTP/1.1\r\n" +
		"Host: ipinfo.dkly.net\r\n" +
		"Connection: close\r\n" +
		"User-Agent: proxyweave/quality-probe\r\n\r\n"
	body, status, err := doOutboundHTTPRequest(ctx, ob, "ipinfo.dkly.net", 443, true, req)
	if err != nil {
		return monitor.QualityInfo{}, err
	}
	if status < 200 || status >= 300 {
		return monitor.QualityInfo{}, fmt.Errorf("ipinfo dkly status: %d", status)
	}
	var api ipInfoDKLYResponse
	if err := json.Unmarshal(body, &api); err != nil {
		return monitor.QualityInfo{}, fmt.Errorf("decode ipinfo dkly response: %w", err)
	}
	if strings.TrimSpace(api.IP) == "" {
		api.IP = exitIP
	}
	ipValid, ipVersion, ipType, ipReason := validateExitIP(api.IP)
	info := monitor.QualityInfo{
		ExitIP:          api.IP,
		IPValid:         ipValid,
		IPVersion:       ipVersion,
		IPType:          ipType,
		IPInvalidReason: ipReason,
		CountryCode:     strings.ToUpper(strings.TrimSpace(api.Location.Country.Code)),
		Country:         strings.TrimSpace(api.Location.Country.Name),
		ASN:             formatASN(api.Connection.ASN),
		ASName:          firstNonEmpty(api.Connection.Organization, api.Company.Name),
		ISP:             api.Company.Name,
		Org:             firstNonEmpty(api.Company.Domain, api.Connection.Domain),
		ProxyType:       classifyDKLYProxyType(api),
		QualitySource:   "ipinfo_dkly",
		Hosting:         api.Security.IsCloudProvider || strings.EqualFold(strings.TrimSpace(api.Connection.Type), "hosting"),
		Proxy:           api.Security.IsProxy || api.Security.IsVPN || api.Security.IsTor || api.Security.IsRelay || api.Security.IsAnonymous,
		CheckedAt:       time.Now(),
	}
	if api.Security.IsThreat || api.Security.IsAbuser || api.Security.IsAttacker || api.Security.IsBogon {
		info.Error = "threat"
	}
	return info, nil
}

func probeExitIP(ctx context.Context, ob adapter.Outbound) (string, error) {
	req := "GET / HTTP/1.1\r\nHost: api.ipify.org\r\nConnection: close\r\nUser-Agent: proxyweave/quality-probe\r\n\r\n"
	body, status, err := doOutboundHTTPRequest(ctx, ob, "api.ipify.org", 443, true, req)
	if err != nil {
		return "", err
	}
	if status < 200 || status >= 300 {
		return "", fmt.Errorf("ipify status: %d", status)
	}
	ip := strings.TrimSpace(string(body))
	if ip == "" {
		return "", fmt.Errorf("empty exit ip")
	}
	return ip, nil
}

func doOutboundHTTPRequest(ctx context.Context, ob adapter.Outbound, host string, port uint16, useTLS bool, req string) ([]byte, int, error) {
	destination := M.ParseSocksaddrHostPort(host, port)
	conn, err := ob.DialContext(ctx, N.NetworkTCP, destination)
	if err != nil {
		return nil, 0, fmt.Errorf("dial %s: %w", host, err)
	}
	defer conn.Close()

	if useTLS {
		tlsConn := tls.Client(conn, &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			return nil, 0, fmt.Errorf("tls handshake %s: %w", host, err)
		}
		conn = tlsConn
	}

	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte(req)); err != nil {
		return nil, 0, fmt.Errorf("write request %s: %w", host, err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		return nil, 0, fmt.Errorf("read response %s: %w", host, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read body %s: %w", host, err)
	}
	return body, resp.StatusCode, nil
}

func validateExitIP(raw string) (valid bool, version string, ipType string, reason string) {
	ipText := strings.TrimSpace(raw)
	if ipText == "" {
		return false, "", "invalid", "empty ip"
	}
	addr, err := netip.ParseAddr(ipText)
	if err != nil {
		return false, "", "invalid", "invalid ip format"
	}
	if addr.Is4() {
		version = "ipv4"
	} else if addr.Is6() {
		version = "ipv6"
	} else {
		version = "unknown"
	}

	switch {
	case addr.IsUnspecified():
		return false, version, "unspecified", "unspecified address"
	case addr.IsLoopback():
		return false, version, "loopback", "loopback address"
	case addr.IsPrivate():
		return false, version, "private", "private address"
	case addr.IsLinkLocalUnicast():
		return false, version, "link_local", "link-local address"
	case addr.IsMulticast():
		return false, version, "multicast", "multicast address"
	case isSpecialUseIP(addr):
		return false, version, "reserved", "reserved/special-use address"
	case !addr.IsGlobalUnicast():
		return false, version, "invalid", "not global unicast"
	default:
		return true, version, "public", ""
	}
}

func isSpecialUseIP(addr netip.Addr) bool {
	for _, cidr := range []string{
		"0.0.0.0/8",       // current network
		"100.64.0.0/10",   // carrier-grade NAT
		"169.254.0.0/16",  // link local (kept for clarity)
		"192.0.0.0/24",    // IETF protocol assignments
		"192.0.2.0/24",    // documentation
		"198.18.0.0/15",   // benchmark tests
		"198.51.100.0/24", // documentation
		"203.0.113.0/24",  // documentation
		"224.0.0.0/4",     // multicast
		"240.0.0.0/4",     // reserved
		"::/128",          // unspecified
		"::1/128",         // loopback
		"64:ff9b::/96",    // IPv4/IPv6 translation
		"100::/64",        // discard-only
		"2001:db8::/32",   // documentation
		"fc00::/7",        // unique local
		"fe80::/10",       // link local
		"ff00::/8",        // multicast
	} {
		prefix := netip.MustParsePrefix(cidr)
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

func extractASN(as string) string {
	for _, part := range strings.Fields(as) {
		upper := strings.ToUpper(strings.TrimSpace(part))
		if strings.HasPrefix(upper, "AS") && len(upper) > 2 {
			return upper
		}
	}
	return as
}

func formatASN(asn int) string {
	if asn <= 0 {
		return ""
	}
	return fmt.Sprintf("AS%d", asn)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func classifyProxyType(api ipAPIResponse) string {
	if api.Mobile {
		return "mobile"
	}
	text := strings.ToLower(strings.Join([]string{api.AS, api.ASName, api.ISP, api.Org}, " "))
	if api.Hosting || api.Proxy || containsAny(text, []string{
		"amazon", "aws", "google cloud", "microsoft", "azure", "cloudflare", "digitalocean",
		"linode", "akamai", "ovh", "hetzner", "contabo", "vultr", "choopa", "leaseweb",
		"alibaba", "aliyun", "tencent", "huawei cloud", "oracle", "rackspace", "colo", "datacenter",
		"data center", "hosting", "server", "cloud", "vps", "dedicated",
	}) {
		return "datacenter"
	}
	if containsAny(text, []string{
		"broadband", "fiber", "fibre", "cable", "telecom", "communications", "comcast", "verizon",
		"at&t", "charter", "spectrum", "cox", "bt", "orange", "vodafone", "telefonica", "deutsche telekom",
		"china telecom", "china unicom", "china mobile", "chinanet", "kddi", "ntt", "softbank", "sk broadband",
		"korea telecom", "hinet", "singtel", "telstra", "pldt", "converge", "isp", "internet service",
	}) || api.ISP != "" {
		return "isp"
	}
	return "unknown"
}

func classifyTextProxyType(text string) string {
	if containsAny(text, []string{
		"amazon", "aws", "google cloud", "microsoft", "azure", "cloudflare", "digitalocean",
		"linode", "akamai", "ovh", "hetzner", "contabo", "vultr", "choopa", "leaseweb",
		"alibaba", "aliyun", "tencent", "huawei cloud", "oracle", "rackspace", "colo", "datacenter",
		"data center", "hosting", "server", "cloud", "vps", "dedicated",
	}) {
		return "datacenter"
	}
	if containsAny(text, []string{
		"broadband", "fiber", "fibre", "cable", "telecom", "communications", "comcast", "verizon",
		"at&t", "charter", "spectrum", "cox", "bt", "orange", "vodafone", "telefonica", "deutsche telekom",
		"china telecom", "china unicom", "china mobile", "chinanet", "kddi", "ntt", "softbank", "sk broadband",
		"korea telecom", "hinet", "singtel", "telstra", "pldt", "converge", "isp", "internet service",
	}) {
		return "isp"
	}
	return "unknown"
}

func classifyDKLYProxyType(api ipInfoDKLYResponse) string {
	switch {
	case api.Security.IsProxy || api.Security.IsVPN || api.Security.IsTor || api.Security.IsRelay || api.Security.IsAnonymous:
		return "proxy"
	case api.Security.IsCloudProvider || strings.EqualFold(strings.TrimSpace(api.Connection.Type), "hosting"):
		return "datacenter"
	case strings.EqualFold(strings.TrimSpace(api.Company.Type), "isp"):
		return "isp"
	}
	text := strings.ToLower(strings.Join([]string{
		api.Company.Name,
		api.Company.Domain,
		api.Connection.Organization,
		api.Connection.Domain,
		api.Connection.Type,
		api.Type,
	}, " "))
	return classifyTextProxyType(text)
}

func containsAny(s string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(s, needle) {
			return true
		}
	}
	return false
}

func (p *poolOutbound) makeQualityFunc(member *memberState) func(ctx context.Context) (monitor.QualityInfo, error) {
	if p.monitor == nil || member == nil || member.outbound == nil {
		return nil
	}
	return func(ctx context.Context) (monitor.QualityInfo, error) {
		enabled, provider, apiKey, _ := p.monitor.QualityConfig()
		if !enabled {
			return monitor.QualityInfo{}, E.New("quality probe is disabled")
		}
		return probeOutboundQuality(ctx, member.outbound, provider, apiKey)
	}
}

func (p *poolOutbound) makeQualityByTagFunc(tag string) func(ctx context.Context) (monitor.QualityInfo, error) {
	if p.monitor == nil {
		return nil
	}
	return func(ctx context.Context) (monitor.QualityInfo, error) {
		p.mu.Lock()
		if len(p.members) == 0 {
			if err := p.initializeMembersLocked(); err != nil {
				p.mu.Unlock()
				return monitor.QualityInfo{}, err
			}
		}
		var member *memberState
		for _, m := range p.members {
			if m.tag == tag {
				member = m
				break
			}
		}
		p.mu.Unlock()
		if member == nil {
			return monitor.QualityInfo{}, E.New("member not found: ", tag)
		}
		qualityFn := p.makeQualityFunc(member)
		if qualityFn == nil {
			return monitor.QualityInfo{}, E.New("quality probe not available")
		}
		return qualityFn(ctx)
	}
}

func (p *poolOutbound) makeProbeFunc(member *memberState) func(ctx context.Context) (time.Duration, error) {
	if p.monitor == nil {
		return nil
	}
	destination, ok := p.monitor.DestinationForProbe()
	if !ok {
		return nil
	}
	probePath, probeTLS := p.monitor.ProbeRequestOptions()
	return func(ctx context.Context) (time.Duration, error) {
		start := time.Now()
		conn, err := member.outbound.DialContext(ctx, N.NetworkTCP, destination)
		if err != nil {
			if member.entry != nil {
				member.entry.RecordFailure(err)
				member.entry.MarkInitialCheckDone(false)
			}
			return 0, err
		}
		defer conn.Close()

		// Perform HTTP probe to measure actual latency (TTFB)
		_, err = httpProbe(conn, destination.AddrString(), probePath, probeTLS)
		if err != nil {
			if member.entry != nil {
				member.entry.RecordFailure(err)
				member.entry.MarkInitialCheckDone(false)
			}
			return 0, err
		}

		// Total duration = dial time + HTTP probe
		duration := time.Since(start)
		if member.entry != nil {
			member.entry.RecordSuccessWithLatency(duration)
			member.entry.MarkInitialCheckDone(true)
		}
		p.maybeProbeQuality(member)
		// Clear pool blacklist on successful probe — a node that passes
		// health check should be available for selection immediately,
		// not remain blacklisted for the full duration (fixes #8, #9).
		if member.shared != nil {
			member.shared.forceRelease()
		}
		return duration, nil
	}
}

// makeProbeByTagFunc creates a probe function that works before member initialization
func (p *poolOutbound) makeProbeByTagFunc(tag string) func(ctx context.Context) (time.Duration, error) {
	if p.monitor == nil {
		return nil
	}
	destination, ok := p.monitor.DestinationForProbe()
	if !ok {
		return nil
	}
	probePath, probeTLS := p.monitor.ProbeRequestOptions()
	return func(ctx context.Context) (time.Duration, error) {
		// Ensure members are initialized
		p.mu.Lock()
		if len(p.members) == 0 {
			if err := p.initializeMembersLocked(); err != nil {
				p.mu.Unlock()
				return 0, err
			}
		}

		// Find the member by tag
		var member *memberState
		for _, m := range p.members {
			if m.tag == tag {
				member = m
				break
			}
		}
		p.mu.Unlock()

		if member == nil {
			return 0, E.New("member not found: ", tag)
		}

		start := time.Now()
		conn, err := member.outbound.DialContext(ctx, N.NetworkTCP, destination)
		if err != nil {
			if member.entry != nil {
				member.entry.RecordFailure(err)
				member.entry.MarkInitialCheckDone(false)
			}
			return 0, err
		}
		defer conn.Close()

		// Perform HTTP probe to measure actual latency (TTFB)
		_, err = httpProbe(conn, destination.AddrString(), probePath, probeTLS)
		if err != nil {
			if member.entry != nil {
				member.entry.RecordFailure(err)
				member.entry.MarkInitialCheckDone(false)
			}
			return 0, err
		}

		// Total duration = dial time + TTFB
		duration := time.Since(start)
		if member.entry != nil {
			member.entry.RecordSuccessWithLatency(duration)
			member.entry.MarkInitialCheckDone(true)
		}
		p.maybeProbeQuality(member)
		// Clear pool blacklist on successful probe (fixes #8, #9)
		if member.shared != nil {
			member.shared.forceRelease()
		}
		return duration, nil
	}
}

// makeReleaseByTagFunc creates a release function that works before member initialization
func (p *poolOutbound) makeReleaseByTagFunc(tag string) func() {
	return func() {
		releaseSharedMember(tag)
	}
}

// makeBlacklistByTagFunc creates a blacklist function for manual ban via API
func (p *poolOutbound) makeBlacklistByTagFunc(tag string) func(time.Duration) {
	return func(duration time.Duration) {
		blacklistSharedMember(tag, duration)
	}
}

type trackedConn struct {
	net.Conn
	once    sync.Once
	release func()
	onWrite func(n int64)
	onRead  func(n int64)
}

func (c *trackedConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if n > 0 && c.onWrite != nil {
		c.onWrite(int64(n))
	}
	return n, err
}

func (c *trackedConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 && c.onRead != nil {
		c.onRead(int64(n))
	}
	return n, err
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

type trackedPacketConn struct {
	net.PacketConn
	once    sync.Once
	release func()
}

func (c *trackedPacketConn) Close() error {
	err := c.PacketConn.Close()
	c.once.Do(c.release)
	return err
}

func (p *poolOutbound) incActive(member *memberState) {
	if member.shared != nil {
		member.shared.incActive()
	}
}

func (p *poolOutbound) decActive(member *memberState) {
	if member.shared != nil {
		member.shared.decActive()
	}
}

func (p *poolOutbound) getCandidateBuffer() []*memberState {
	if buf := p.candidatesPool.Get(); buf != nil {
		return buf.([]*memberState)
	}
	return make([]*memberState, 0, len(p.options.Members))
}

func (p *poolOutbound) putCandidateBuffer(buf []*memberState) {
	if buf == nil {
		return
	}
	const maxCached = 4096
	if cap(buf) > maxCached {
		return
	}
	p.candidatesPool.Put(buf[:0])
}
