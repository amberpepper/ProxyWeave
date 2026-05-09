package subscription

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"proxyweave/internal/boxmgr"
	"proxyweave/internal/config"
	"proxyweave/internal/monitor"
	storepkg "proxyweave/internal/store"
)

// Logger defines logging interface.
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// Option configures the Manager.
type Option func(*Manager)

// WithLogger sets a custom logger.
func WithLogger(l Logger) Option {
	return func(m *Manager) { m.logger = l }
}

// Manager handles periodic subscription refresh.
type Manager struct {
	mu sync.RWMutex

	baseCfg    *config.Config
	boxMgr     *boxmgr.Manager
	store      storepkg.SubscriptionStore
	logger     Logger
	httpClient *http.Client

	status      monitor.SubscriptionStatus
	ctx         context.Context
	cancel      context.CancelFunc
	loopCancels map[int64]context.CancelFunc
	nextRefresh map[int64]time.Time
	refreshing  map[int64]bool
	manualMu    sync.Mutex
}

// New creates a SubscriptionManager.
func New(cfg *config.Config, boxMgr *boxmgr.Manager, store storepkg.SubscriptionStore, opts ...Option) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   60 * time.Second,
	}

	m := &Manager{
		baseCfg:     cfg,
		boxMgr:      boxMgr,
		store:       store,
		ctx:         ctx,
		cancel:      cancel,
		loopCancels: make(map[int64]context.CancelFunc),
		nextRefresh: make(map[int64]time.Time),
		refreshing:  make(map[int64]bool),
		httpClient:  httpClient,
	}
	for _, opt := range opts {
		opt(m)
	}
	if m.logger == nil {
		m.logger = defaultLogger{}
	}
	return m
}

// Start begins the periodic refresh loops.
func (m *Manager) Start() {
	if m.store == nil {
		m.logger.Warnf("subscription store is not configured, refresh disabled")
		return
	}
	if err := m.resyncLoops(); err != nil {
		m.logger.Errorf("failed to start subscription loops: %v", err)
	}
}

// Stop stops the periodic refresh.
func (m *Manager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}

	m.mu.Lock()
	for id, cancel := range m.loopCancels {
		cancel()
		delete(m.loopCancels, id)
	}
	m.nextRefresh = make(map[int64]time.Time)
	m.refreshing = make(map[int64]bool)
	m.mu.Unlock()

	if m.httpClient != nil {
		m.httpClient.CloseIdleConnections()
	}
}

// RefreshNow refreshes all enabled subscriptions immediately.
func (m *Manager) RefreshNow() error {
	m.manualMu.Lock()
	defer m.manualMu.Unlock()

	ctx, cancel := context.WithTimeout(m.ctx, m.refreshDeadline())
	defer cancel()

	subs, err := m.store.ListSubscriptions(ctx)
	if err != nil {
		return fmt.Errorf("list subscriptions: %w", err)
	}
	var enabled []storepkg.Subscription
	for _, sub := range subs {
		if sub.Enabled {
			enabled = append(enabled, sub)
		}
	}
	if len(enabled) == 0 {
		return fmt.Errorf("没有启用的订阅")
	}
	return m.refreshBatch(ctx, enabled)
}

// RefreshSubscription refreshes one subscription immediately.
func (m *Manager) RefreshSubscription(ctx context.Context, id int64) error {
	if id <= 0 {
		return fmt.Errorf("订阅 ID 无效")
	}
	return m.refreshOne(ctx, id, true)
}

// Status returns aggregate subscription status.
func (m *Manager) Status() monitor.SubscriptionStatus {
	status := monitor.SubscriptionStatus{}
	subs, err := m.ListSubscriptions(context.Background())
	status.Enabled = len(subs) > 0
	if err == nil {
		for _, sub := range subs {
			if sub.LastRefresh.After(status.LastRefresh) {
				status.LastRefresh = sub.LastRefresh
			}
			if sub.LastError != "" && (status.LastError == "" || sub.LastRefresh.After(status.LastRefresh)) {
				status.LastError = sub.LastError
			}
		}
	}
	if nodes, err := m.boxMgr.ListConfigNodes(context.Background()); err == nil {
		for _, node := range nodes {
			if node.Source == config.NodeSourceSubscription {
				status.NodeCount++
			}
		}
	}

	m.mu.RLock()
	status.RefreshCount = m.status.RefreshCount
	status.IsRefreshing = len(m.refreshing) > 0
	for _, next := range m.nextRefresh {
		if next.IsZero() {
			continue
		}
		if status.NextRefresh.IsZero() || next.Before(status.NextRefresh) {
			status.NextRefresh = next
		}
	}
	m.mu.RUnlock()
	status.NodesModified = false
	return status
}

// ListSubscriptions returns all subscription records.
func (m *Manager) ListSubscriptions(ctx context.Context) ([]storepkg.Subscription, error) {
	if m.store == nil {
		return nil, fmt.Errorf("subscription store is not configured")
	}
	return m.store.ListSubscriptions(ctx)
}

// ListSubscriptionNodes returns paginated nodes imported by a subscription.
func (m *Manager) ListSubscriptionNodes(ctx context.Context, id int64, page int, pageSize int) (storepkg.SubscriptionNodesPage, error) {
	if m.store == nil {
		return storepkg.SubscriptionNodesPage{}, fmt.Errorf("subscription store is not configured")
	}
	return m.store.ListSubscriptionNodes(ctx, id, page, pageSize)
}

// CreateSubscription creates a subscription and optionally performs an initial refresh.
func (m *Manager) CreateSubscription(ctx context.Context, sub storepkg.Subscription) (storepkg.Subscription, error) {
	sub = normalizeSubscription(sub)
	created, err := m.store.CreateSubscription(ctx, sub)
	if err != nil {
		return storepkg.Subscription{}, err
	}
	if err := m.resyncLoops(); err != nil {
		return created, err
	}
	if created.Enabled {
		if err := m.refreshOne(ctx, created.ID, true); err != nil {
			return created, err
		}
		return m.store.GetSubscription(ctx, created.ID)
	}
	return created, nil
}

// UpdateSubscription updates a subscription and applies runtime changes immediately.
func (m *Manager) UpdateSubscription(ctx context.Context, sub storepkg.Subscription) (storepkg.Subscription, error) {
	sub = normalizeSubscription(sub)
	updated, err := m.store.UpdateSubscription(ctx, sub)
	if err != nil {
		return storepkg.Subscription{}, err
	}
	if !updated.Enabled {
		if err := m.store.ReplaceSubscriptionNodes(ctx, updated, nil); err != nil {
			return updated, err
		}
		if err := m.boxMgr.TriggerReload(ctx); err != nil {
			return updated, err
		}
	} else {
		if err := m.refreshOne(ctx, updated.ID, true); err != nil {
			return updated, err
		}
	}
	if err := m.resyncLoops(); err != nil {
		return updated, err
	}
	return m.store.GetSubscription(ctx, updated.ID)
}

// DeleteSubscription deletes a subscription and reloads the runtime.
func (m *Manager) DeleteSubscription(ctx context.Context, id int64) error {
	if err := m.store.DeleteSubscription(ctx, id); err != nil {
		return err
	}
	if err := m.resyncLoops(); err != nil {
		return err
	}
	return m.boxMgr.TriggerReload(ctx)
}

func (m *Manager) refreshDeadline() time.Duration {
	deadline := m.baseCfg.SubscriptionRefresh.Timeout
	if deadline <= 0 {
		deadline = 30 * time.Second
	}
	if hc := m.baseCfg.SubscriptionRefresh.HealthCheckTimeout; hc > 0 {
		deadline += hc
	}
	return deadline
}

func normalizeSubscription(sub storepkg.Subscription) storepkg.Subscription {
	sub.Name = strings.TrimSpace(sub.Name)
	sub.URL = strings.TrimSpace(sub.URL)
	if sub.RefreshInterval <= 0 {
		sub.RefreshInterval = time.Hour
	}
	return sub
}

func (m *Manager) resyncLoops() error {
	if m.store == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
	defer cancel()
	subs, err := m.store.ListSubscriptions(ctx)
	if err != nil {
		return err
	}

	enabled := make(map[int64]storepkg.Subscription)
	for _, sub := range subs {
		if sub.Enabled {
			enabled[sub.ID] = normalizeSubscription(sub)
		}
	}

	m.mu.Lock()
	for id, cancelLoop := range m.loopCancels {
		cancelLoop()
		delete(m.loopCancels, id)
	}
	m.nextRefresh = make(map[int64]time.Time)
	m.mu.Unlock()

	for _, sub := range enabled {
		loopCtx, loopCancel := context.WithCancel(m.ctx)
		m.mu.Lock()
		m.loopCancels[sub.ID] = loopCancel
		m.nextRefresh[sub.ID] = time.Now().Add(sub.RefreshInterval)
		m.mu.Unlock()
		go m.runLoop(loopCtx, sub)
	}

	if len(enabled) == 0 {
		m.logger.Infof("no enabled subscriptions, refresh loops stopped")
	} else {
		m.logger.Infof("subscription loops started: %d enabled", len(enabled))
	}
	return nil
}

func (m *Manager) runLoop(ctx context.Context, sub storepkg.Subscription) {
	interval := sub.RefreshInterval
	if interval <= 0 {
		interval = time.Hour
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			m.mu.Lock()
			delete(m.nextRefresh, sub.ID)
			delete(m.loopCancels, sub.ID)
			m.mu.Unlock()
			return
		case <-timer.C:
			if err := m.refreshOne(ctx, sub.ID, true); err != nil {
				m.logger.Warnf("subscription %d refresh failed: %v", sub.ID, err)
			}
			next := time.Now().Add(interval)
			m.mu.Lock()
			m.nextRefresh[sub.ID] = next
			m.mu.Unlock()
			timer.Reset(interval)
		}
	}
}

func (m *Manager) refreshBatch(ctx context.Context, subs []storepkg.Subscription) error {
	if len(subs) == 0 {
		return nil
	}
	sort.Slice(subs, func(i, j int) bool { return subs[i].ID < subs[j].ID })

	var firstErr error
	refreshed := false
	for _, sub := range subs {
		if err := m.refreshOne(ctx, sub.ID, false); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		refreshed = true
	}
	if refreshed {
		if err := m.boxMgr.TriggerReload(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (m *Manager) refreshOne(parent context.Context, id int64, reload bool) error {
	if m.store == nil {
		return fmt.Errorf("subscription store is not configured")
	}
	if id <= 0 {
		return fmt.Errorf("subscription id is required")
	}

	m.mu.Lock()
	if m.refreshing[id] {
		m.mu.Unlock()
		return fmt.Errorf("订阅 %d 正在刷新", id)
	}
	m.refreshing[id] = true
	m.status.IsRefreshing = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.refreshing, id)
		m.status.IsRefreshing = len(m.refreshing) > 0
		m.status.RefreshCount++
		m.mu.Unlock()
	}()

	ctx := parent
	if ctx == nil {
		ctx = m.ctx
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, m.refreshDeadline())
	defer cancel()

	sub, err := m.store.GetSubscription(timeoutCtx, id)
	if err != nil {
		if errorsIsNoRows(err) {
			return fmt.Errorf("订阅不存在")
		}
		return err
	}

	nodes, err := m.fetchSubscription(timeoutCtx, sub.URL)
	if err != nil {
		_ = m.store.MarkSubscriptionRefreshError(context.Background(), id, err.Error())
		return fmt.Errorf("fetch subscription %d: %w", id, err)
	}
	for i := range nodes {
		nodes[i].Source = config.NodeSourceSubscription
	}
	if err := m.store.ReplaceSubscriptionNodes(timeoutCtx, sub, nodes); err != nil {
		_ = m.store.MarkSubscriptionRefreshError(context.Background(), id, err.Error())
		return err
	}
	if reload {
		if err := m.boxMgr.TriggerReload(timeoutCtx); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) fetchSubscription(ctx context.Context, subURL string) ([]config.NodeConfig, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, subURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("User-Agent", "clash-verge/v2.2.3")
	req.Header.Set("Accept", "*/*")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	const maxBodySize = 10 * 1024 * 1024
	limitedReader := io.LimitReader(resp.Body, maxBodySize)
	body, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return config.ParseSubscriptionContent(string(body))
}

func errorsIsNoRows(err error) bool {
	return err == sql.ErrNoRows || (err != nil && strings.Contains(err.Error(), sql.ErrNoRows.Error()))
}

type defaultLogger struct{}

func (defaultLogger) Infof(format string, args ...any) {
	log.Printf("[subscription] "+format, args...)
}

func (defaultLogger) Warnf(format string, args ...any) {
	log.Printf("[subscription] WARN: "+format, args...)
}

func (defaultLogger) Errorf(format string, args ...any) {
	log.Printf("[subscription] ERROR: "+format, args...)
}
