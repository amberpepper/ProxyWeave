package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"proxyweave/internal/config"
	"proxyweave/internal/monitor"
	storepkg "proxyweave/internal/store"

	sqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const schemaSQL = `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS app_settings (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  config_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  uri TEXT NOT NULL UNIQUE,
  port INTEGER NOT NULL DEFAULT 0,
  username TEXT NOT NULL DEFAULT '',
  password TEXT NOT NULL DEFAULT '',
  source TEXT NOT NULL DEFAULT 'inline',
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS subscriptions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL DEFAULT '',
  url TEXT NOT NULL UNIQUE,
  enabled INTEGER NOT NULL DEFAULT 1,
  refresh_interval_seconds INTEGER NOT NULL DEFAULT 3600,
  last_refresh TEXT,
  last_error TEXT,
  last_node_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS subscription_nodes (
  subscription_id INTEGER NOT NULL,
  node_uri TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (subscription_id, node_uri),
  FOREIGN KEY (subscription_id) REFERENCES subscriptions(id) ON DELETE CASCADE,
  FOREIGN KEY (node_uri) REFERENCES nodes(uri) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS runtime_state (
  node_uri TEXT PRIMARY KEY,
  state_key TEXT NOT NULL DEFAULT '',
  available INTEGER NOT NULL DEFAULT 0,
  initial_check_done INTEGER NOT NULL DEFAULT 0,
  failure_count INTEGER NOT NULL DEFAULT 0,
  blacklisted INTEGER NOT NULL DEFAULT 0,
  blacklisted_until TEXT,
  last_error TEXT,
  last_latency_ms INTEGER NOT NULL DEFAULT -1,
  last_success TEXT,
  last_failure TEXT,
  country_code TEXT,
  country TEXT,
  exit_ip TEXT,
  proxy_type TEXT,
  quality_source TEXT,
  quality_error TEXT,
  quality_checked_at TEXT,
  ip_valid INTEGER NOT NULL DEFAULT 0,
  ip_version TEXT,
  ip_type TEXT,
  ip_invalid_reason TEXT,
  asn TEXT,
  as_name TEXT,
  isp TEXT,
  org TEXT,
  mobile INTEGER NOT NULL DEFAULT 0,
  hosting INTEGER NOT NULL DEFAULT 0,
  proxy INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (node_uri) REFERENCES nodes(uri) ON DELETE CASCADE
);
`

type SettingsPayload struct {
	Mode                string
	Listener            config.ListenerConfig
	MultiPort           config.MultiPortConfig
	Pool                config.PoolConfig
	Management          config.ManagementConfig
	SubscriptionRefresh config.SubscriptionRefreshConfig
	GeoIP               config.GeoIPConfig
	Log                 config.LogConfig
	ExternalIP          string
	LogLevel            string
	SkipCertVerify      bool
}

type NodeRecord struct {
	config.NodeConfig
	Enabled           bool     `json:"enabled"`
	SubscriptionCount int      `json:"subscription_count"`
	Subscriptions     []string `json:"subscriptions,omitempty"`
}

type Store struct {
	db   *sql.DB
	path string
}

func Open(path string) (*Store, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		path = filepath.Join("data", "proxyweave.db")
	}
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create db dir: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable foreign keys: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout = 5000"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set busy timeout: %w", err)
	}
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE runtime_state ADD COLUMN state_key TEXT NOT NULL DEFAULT ''`); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column name") {
		_ = db.Close()
		return nil, fmt.Errorf("ensure runtime_state.state_key: %w", err)
	}

	s := &Store{db: db, path: path}
	if err := s.ensureDefaults(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Path() string { return s.path }

func (s *Store) ensureDefaults(ctx context.Context) error {
	var count int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(1) FROM app_settings").Scan(&count); err != nil {
		return fmt.Errorf("check app_settings: %w", err)
	}
	if count > 0 {
		return nil
	}
	cfg := config.DefaultWebConfig()
	cfg.Nodes = nil
	cfg.NodesFile = ""
	cfg.Subscriptions = nil
	return s.SaveSettings(ctx, &cfg)
}

func payloadFromConfig(cfg *config.Config) SettingsPayload {
	if cfg == nil {
		cfgCopy := config.DefaultWebConfig()
		cfg = &cfgCopy
	}
	return SettingsPayload{
		Mode:                cfg.Mode,
		Listener:            cfg.Listener,
		MultiPort:           cfg.MultiPort,
		Pool:                cfg.Pool,
		Management:          cfg.Management,
		SubscriptionRefresh: cfg.SubscriptionRefresh,
		GeoIP:               cfg.GeoIP,
		Log:                 cfg.Log,
		ExternalIP:          cfg.ExternalIP,
		LogLevel:            cfg.LogLevel,
		SkipCertVerify:      cfg.SkipCertVerify,
	}
}

func (p SettingsPayload) toConfig() *config.Config {
	cfg := &config.Config{
		Mode:                p.Mode,
		Listener:            p.Listener,
		MultiPort:           p.MultiPort,
		Pool:                p.Pool,
		Management:          p.Management,
		SubscriptionRefresh: p.SubscriptionRefresh,
		GeoIP:               p.GeoIP,
		Log:                 p.Log,
		ExternalIP:          p.ExternalIP,
		LogLevel:            p.LogLevel,
		SkipCertVerify:      p.SkipCertVerify,
	}
	cfg.NodesFile = ""
	return cfg
}

func (s *Store) SaveSettings(ctx context.Context, cfg *config.Config) error {
	payload := payloadFromConfig(cfg)
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal settings: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO app_settings (id, config_json, updated_at) VALUES (1, ?, ?) ON CONFLICT(id) DO UPDATE SET config_json=excluded.config_json, updated_at=excluded.updated_at`,
		string(data), now,
	)
	if err != nil {
		return fmt.Errorf("save settings: %w", err)
	}
	return nil
}

func (s *Store) loadSettingsPayload(ctx context.Context) (SettingsPayload, error) {
	var raw string
	if err := s.db.QueryRowContext(ctx, `SELECT config_json FROM app_settings WHERE id = 1`).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cfg := config.DefaultWebConfig()
			cfg.NodesFile = ""
			if err := s.SaveSettings(ctx, &cfg); err != nil {
				return SettingsPayload{}, err
			}
			return payloadFromConfig(&cfg), nil
		}
		return SettingsPayload{}, fmt.Errorf("load settings: %w", err)
	}
	var payload SettingsPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return SettingsPayload{}, fmt.Errorf("decode settings: %w", err)
	}
	return payload, nil
}

func (s *Store) LoadConfig(ctx context.Context) (*config.Config, error) {
	payload, err := s.loadSettingsPayload(ctx)
	if err != nil {
		return nil, err
	}
	cfg := payload.toConfig()
	nodes, err := s.ListNodeConfigs(ctx)
	if err != nil {
		return nil, err
	}
	subs, err := s.ListSubscriptions(ctx)
	if err != nil {
		return nil, err
	}
	cfg.Nodes = nodes
	cfg.Subscriptions = make([]string, 0, len(subs))
	for _, sub := range subs {
		if sub.Enabled {
			cfg.Subscriptions = append(cfg.Subscriptions, sub.URL)
		}
	}
	cfg.SetFilePath(s.path)
	if err := cfg.NormalizeWithPortMap(nil); err != nil {
		return nil, err
	}
	cfg.Subscriptions = make([]string, 0, len(subs))
	for _, sub := range subs {
		if sub.Enabled {
			cfg.Subscriptions = append(cfg.Subscriptions, sub.URL)
		}
	}
	cfg.NodesFile = ""
	return cfg, nil
}

func nodeSourceFromText(v string, subCount int) config.NodeSource {
	if subCount > 0 {
		return config.NodeSourceSubscription
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case string(config.NodeSourceSubscription):
		return config.NodeSourceSubscription
	case string(config.NodeSourceFile):
		return config.NodeSourceFile
	default:
		return config.NodeSourceInline
	}
}

func nodeSourceText(src config.NodeSource) string {
	if src == "" {
		return string(config.NodeSourceInline)
	}
	return string(src)
}

func (s *Store) ListNodes(ctx context.Context) ([]NodeRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT n.name, n.uri, n.port, n.username, n.password, n.source, n.enabled,
       COUNT(sn.subscription_id) AS sub_count,
       GROUP_CONCAT(sub.name, ',') AS sub_names
FROM nodes n
LEFT JOIN subscription_nodes sn ON sn.node_uri = n.uri
LEFT JOIN subscriptions sub ON sub.id = sn.subscription_id
GROUP BY n.uri
ORDER BY n.updated_at DESC, n.name ASC, n.uri ASC`)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	defer rows.Close()
	var out []NodeRecord
	for rows.Next() {
		var rec NodeRecord
		var sourceText string
		var enabledInt int
		var subCount int
		var subNames sql.NullString
		var port int
		if err := rows.Scan(&rec.Name, &rec.URI, &port, &rec.Username, &rec.Password, &sourceText, &enabledInt, &subCount, &subNames); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}
		rec.Port = uint16(port)
		rec.Enabled = enabledInt == 1
		rec.Source = nodeSourceFromText(sourceText, subCount)
		rec.SubscriptionCount = subCount
		if subNames.Valid && strings.TrimSpace(subNames.String) != "" {
			rec.Subscriptions = strings.Split(subNames.String, ",")
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) ListNodeConfigs(ctx context.Context) ([]config.NodeConfig, error) {
	records, err := s.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]config.NodeConfig, 0, len(records))
	for _, rec := range records {
		if !rec.Enabled {
			continue
		}
		out = append(out, rec.NodeConfig)
	}
	return out, nil
}

func (s *Store) CreateNode(ctx context.Context, node config.NodeConfig) (config.NodeConfig, error) {
	node.Name = strings.TrimSpace(node.Name)
	node.URI = strings.TrimSpace(node.URI)
	if node.URI == "" {
		return config.NodeConfig{}, fmt.Errorf("node uri is required")
	}
	if node.Name == "" {
		node.Name = config.ExtractNodeName(node.URI)
	}
	if node.Name == "" {
		node.Name = fmt.Sprintf("node-%d", time.Now().UnixNano())
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `INSERT INTO nodes (name, uri, port, username, password, source, enabled, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)`,
		node.Name, node.URI, int(node.Port), node.Username, node.Password, nodeSourceText(config.NodeSourceInline), now, now)
	if err != nil {
		return config.NodeConfig{}, fmt.Errorf("create node: %w", err)
	}
	node.Source = config.NodeSourceInline
	return node, nil
}

func (s *Store) UpdateNodeByName(ctx context.Context, currentName string, node config.NodeConfig) (config.NodeConfig, error) {
	currentName = strings.TrimSpace(currentName)
	if currentName == "" {
		return config.NodeConfig{}, fmt.Errorf("current node name is required")
	}
	node.Name = strings.TrimSpace(node.Name)
	node.URI = strings.TrimSpace(node.URI)
	if node.URI == "" {
		return config.NodeConfig{}, fmt.Errorf("node uri is required")
	}
	if node.Name == "" {
		node.Name = config.ExtractNodeName(node.URI)
	}
	if node.Name == "" {
		node.Name = currentName
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return config.NodeConfig{}, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var oldURI string
	if err = tx.QueryRowContext(ctx, `SELECT uri FROM nodes WHERE name = ?`, currentName).Scan(&oldURI); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return config.NodeConfig{}, sql.ErrNoRows
		}
		return config.NodeConfig{}, fmt.Errorf("load existing node: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := tx.ExecContext(ctx, `UPDATE nodes SET name = ?, uri = ?, port = ?, username = ?, password = ?, updated_at = ? WHERE name = ?`,
		node.Name, node.URI, int(node.Port), node.Username, node.Password, now, currentName)
	if err != nil {
		return config.NodeConfig{}, fmt.Errorf("update node: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return config.NodeConfig{}, sql.ErrNoRows
	}
	if oldURI != "" && oldURI != node.URI {
		if _, err = tx.ExecContext(ctx, `UPDATE subscription_nodes SET node_uri = ? WHERE node_uri = ?`, node.URI, oldURI); err != nil {
			return config.NodeConfig{}, fmt.Errorf("update subscription node refs: %w", err)
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM runtime_state WHERE node_uri = ?`, oldURI); err != nil {
			return config.NodeConfig{}, fmt.Errorf("cleanup runtime state: %w", err)
		}
	}

	var subCount int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM subscription_nodes WHERE node_uri = ?`, node.URI).Scan(&subCount); err != nil {
		return config.NodeConfig{}, fmt.Errorf("count node subscriptions: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return config.NodeConfig{}, fmt.Errorf("commit node update: %w", err)
	}

	if subCount > 0 {
		node.Source = config.NodeSourceSubscription
	} else {
		node.Source = config.NodeSourceInline
	}
	return node, nil
}

func (s *Store) DeleteNodeByName(ctx context.Context, name string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM nodes WHERE name = ?`, strings.TrimSpace(name))
	if err != nil {
		return fmt.Errorf("delete node: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *Store) ListSubscriptions(ctx context.Context) ([]storepkg.Subscription, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, url, enabled, refresh_interval_seconds, last_refresh, last_error, last_node_count, created_at, updated_at FROM subscriptions ORDER BY created_at ASC, id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list subscriptions: %w", err)
	}
	defer rows.Close()
	var out []storepkg.Subscription
	for rows.Next() {
		var sub storepkg.Subscription
		var enabledInt int
		var intervalSeconds int64
		var lastRefresh, lastError, createdAt, updatedAt sql.NullString
		if err := rows.Scan(&sub.ID, &sub.Name, &sub.URL, &enabledInt, &intervalSeconds, &lastRefresh, &lastError, &sub.LastNodeCount, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan subscription: %w", err)
		}
		sub.Enabled = enabledInt == 1
		sub.RefreshInterval = time.Duration(intervalSeconds) * time.Second
		if lastError.Valid {
			sub.LastError = lastError.String
		}
		if lastRefresh.Valid {
			sub.LastRefresh, _ = time.Parse(time.RFC3339Nano, lastRefresh.String)
		}
		if createdAt.Valid {
			sub.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt.String)
		}
		if updatedAt.Valid {
			sub.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedAt.String)
		}
		out = append(out, sub)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) CreateSubscription(ctx context.Context, sub storepkg.Subscription) (storepkg.Subscription, error) {
	sub.URL = strings.TrimSpace(sub.URL)
	sub.Name = strings.TrimSpace(sub.Name)
	if sub.URL == "" {
		return storepkg.Subscription{}, fmt.Errorf("subscription url is required")
	}
	if sub.RefreshInterval <= 0 {
		sub.RefreshInterval = time.Hour
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	enabled := 0
	if sub.Enabled {
		enabled = 1
	}
	res, err := s.db.ExecContext(ctx, `INSERT INTO subscriptions (name, url, enabled, refresh_interval_seconds, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
		sub.Name, sub.URL, enabled, int64(sub.RefreshInterval/time.Second), now, now)
	if err != nil {
		if isSQLiteUniqueURLConstraint(err) {
			return storepkg.Subscription{}, fmt.Errorf("订阅 URL 已存在")
		}
		return storepkg.Subscription{}, fmt.Errorf("create subscription: %w", err)
	}
	sub.ID, _ = res.LastInsertId()
	sub.CreatedAt = time.Now().UTC()
	sub.UpdatedAt = sub.CreatedAt
	return sub, nil
}

func (s *Store) UpdateSubscription(ctx context.Context, sub storepkg.Subscription) (storepkg.Subscription, error) {
	if sub.ID <= 0 {
		return storepkg.Subscription{}, fmt.Errorf("subscription id is required")
	}
	sub.URL = strings.TrimSpace(sub.URL)
	sub.Name = strings.TrimSpace(sub.Name)
	if sub.URL == "" {
		return storepkg.Subscription{}, fmt.Errorf("subscription url is required")
	}
	if sub.RefreshInterval <= 0 {
		sub.RefreshInterval = time.Hour
	}
	enabled := 0
	if sub.Enabled {
		enabled = 1
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, `UPDATE subscriptions SET name = ?, url = ?, enabled = ?, refresh_interval_seconds = ?, updated_at = ? WHERE id = ?`,
		sub.Name, sub.URL, enabled, int64(sub.RefreshInterval/time.Second), now, sub.ID)
	if err != nil {
		if isSQLiteUniqueURLConstraint(err) {
			return storepkg.Subscription{}, fmt.Errorf("订阅 URL 已存在")
		}
		return storepkg.Subscription{}, fmt.Errorf("update subscription: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return storepkg.Subscription{}, sql.ErrNoRows
	}
	return s.GetSubscription(ctx, sub.ID)
}

func (s *Store) GetSubscription(ctx context.Context, id int64) (storepkg.Subscription, error) {
	var sub storepkg.Subscription
	var enabledInt int
	var intervalSeconds int64
	var lastRefresh, lastError, createdAt, updatedAt sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT id, name, url, enabled, refresh_interval_seconds, last_refresh, last_error, last_node_count, created_at, updated_at FROM subscriptions WHERE id = ?`, id).
		Scan(&sub.ID, &sub.Name, &sub.URL, &enabledInt, &intervalSeconds, &lastRefresh, &lastError, &sub.LastNodeCount, &createdAt, &updatedAt); err != nil {
		return storepkg.Subscription{}, err
	}
	sub.Enabled = enabledInt == 1
	sub.RefreshInterval = time.Duration(intervalSeconds) * time.Second
	if lastError.Valid {
		sub.LastError = lastError.String
	}
	if lastRefresh.Valid {
		sub.LastRefresh, _ = time.Parse(time.RFC3339Nano, lastRefresh.String)
	}
	if createdAt.Valid {
		sub.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt.String)
	}
	if updatedAt.Valid {
		sub.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedAt.String)
	}
	return sub, nil
}

func (s *Store) DeleteSubscription(ctx context.Context, id int64) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM subscriptions WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete subscription: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return sql.ErrNoRows
	}
	_, _ = s.db.ExecContext(ctx, `DELETE FROM nodes WHERE source = ? AND uri NOT IN (SELECT node_uri FROM subscription_nodes)`, string(config.NodeSourceSubscription))
	return nil
}

func (s *Store) ListSubscriptionNodes(ctx context.Context, id int64, page int, pageSize int) (storepkg.SubscriptionNodesPage, error) {
	if id <= 0 {
		return storepkg.SubscriptionNodesPage{}, fmt.Errorf("subscription id is required")
	}
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 200 {
		pageSize = 200
	}
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM subscription_nodes WHERE subscription_id = ?`, id).Scan(&total); err != nil {
		return storepkg.SubscriptionNodesPage{}, fmt.Errorf("count subscription nodes: %w", err)
	}
	totalPages := 0
	if total > 0 {
		totalPages = (total + pageSize - 1) / pageSize
	}
	if totalPages == 0 {
		page = 1
	} else if page > totalPages {
		page = totalPages
	}
	offset := (page - 1) * pageSize
	if offset < 0 {
		offset = 0
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT n.name, n.uri, n.port, n.username, n.password, n.source
FROM subscription_nodes sn
JOIN nodes n ON n.uri = sn.node_uri
WHERE sn.subscription_id = ?
ORDER BY sn.created_at ASC, n.name ASC, n.uri ASC
LIMIT ? OFFSET ?`, id, pageSize, offset)
	if err != nil {
		return storepkg.SubscriptionNodesPage{}, fmt.Errorf("list subscription nodes: %w", err)
	}
	defer rows.Close()
	nodes := make([]config.NodeConfig, 0, pageSize)
	for rows.Next() {
		var node config.NodeConfig
		var sourceText string
		var port int
		if err := rows.Scan(&node.Name, &node.URI, &port, &node.Username, &node.Password, &sourceText); err != nil {
			return storepkg.SubscriptionNodesPage{}, fmt.Errorf("scan subscription node: %w", err)
		}
		node.Port = uint16(port)
		node.Source = config.NodeSourceSubscription
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		return storepkg.SubscriptionNodesPage{}, err
	}
	return storepkg.SubscriptionNodesPage{Nodes: nodes, Total: total, Page: page, PageSize: pageSize, TotalPages: totalPages}, nil
}

func (s *Store) ReplaceSubscriptionNodes(ctx context.Context, sub storepkg.Subscription, nodes []config.NodeConfig) error {
	if sub.ID <= 0 {
		return fmt.Errorf("subscription id is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.ExecContext(ctx, `DELETE FROM subscription_nodes WHERE subscription_id = ?`, sub.ID); err != nil {
		return fmt.Errorf("clear subscription relations: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	seen := make(map[string]struct{})
	for _, node := range nodes {
		node.URI = strings.TrimSpace(node.URI)
		if node.URI == "" {
			continue
		}
		if _, ok := seen[node.URI]; ok {
			continue
		}
		seen[node.URI] = struct{}{}
		if node.Name == "" {
			node.Name = config.ExtractNodeName(node.URI)
		}
		if node.Name == "" {
			node.Name = node.URI
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO nodes (name, uri, port, username, password, source, enabled, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
			ON CONFLICT(uri) DO UPDATE SET name=excluded.name, port=excluded.port, username=excluded.username, password=excluded.password, source=excluded.source, enabled=1, updated_at=excluded.updated_at`,
			node.Name, node.URI, int(node.Port), node.Username, node.Password, string(config.NodeSourceSubscription), now, now); err != nil {
			return fmt.Errorf("upsert subscription node: %w", err)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO subscription_nodes (subscription_id, node_uri, created_at) VALUES (?, ?, ?)`, sub.ID, node.URI, now); err != nil {
			return fmt.Errorf("insert subscription relation: %w", err)
		}
	}

	if _, err = tx.ExecContext(ctx, `DELETE FROM nodes WHERE source = ? AND uri NOT IN (SELECT node_uri FROM subscription_nodes)`, string(config.NodeSourceSubscription)); err != nil {
		return fmt.Errorf("cleanup orphan subscription nodes: %w", err)
	}

	if _, err = tx.ExecContext(ctx, `UPDATE subscriptions SET last_refresh = ?, last_error = '', last_node_count = ?, updated_at = ? WHERE id = ?`, now, len(seen), now, sub.ID); err != nil {
		return fmt.Errorf("update subscription status: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit subscription refresh: %w", err)
	}
	return nil
}

func (s *Store) MarkSubscriptionRefreshError(ctx context.Context, id int64, msg string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `UPDATE subscriptions SET last_refresh = ?, last_error = ?, updated_at = ? WHERE id = ?`, now, msg, now, id)
	if err != nil {
		return fmt.Errorf("mark subscription refresh error: %w", err)
	}
	return nil
}

func (s *Store) LoadRuntimeStates(ctx context.Context) (map[string]monitor.PersistedState, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT node_uri, state_key, available, initial_check_done, failure_count, blacklisted, blacklisted_until, last_error, last_latency_ms, last_success, last_failure, country_code, country, exit_ip, proxy_type, quality_source, quality_error, quality_checked_at, ip_valid, ip_version, ip_type, ip_invalid_reason, asn, as_name, isp, org, mobile, hosting, proxy, updated_at FROM runtime_state`)
	if err != nil {
		return nil, fmt.Errorf("load runtime states: %w", err)
	}
	defer rows.Close()
	out := make(map[string]monitor.PersistedState)
	for rows.Next() {
		var uri, stateKey string
		var st monitor.PersistedState
		var available, initialDone, blacklisted, ipValid, mobile, hosting, proxyFlag int
		var blacklistedUntil, lastSuccess, lastFailure, lastError, checkedAt sql.NullString
		if err := rows.Scan(&uri, &stateKey, &available, &initialDone, &st.FailureCount, &blacklisted, &blacklistedUntil, &lastError, &st.LastLatencyMs, &lastSuccess, &lastFailure, &st.CountryCode, &st.Country, &st.ExitIP, &st.ProxyType, &st.QualitySource, &st.QualityError, &checkedAt, &ipValid, &st.IPVersion, &st.IPType, &st.IPInvalidReason, &st.ASN, &st.ASName, &st.ISP, &st.Org, &mobile, &hosting, &proxyFlag, new(string)); err != nil {
			return nil, fmt.Errorf("scan runtime state: %w", err)
		}
		st.StateKey = strings.TrimSpace(stateKey)
		st.Available = available == 1
		st.InitialCheckDone = initialDone == 1
		st.Blacklisted = blacklisted == 1
		st.IPValid = ipValid == 1
		st.Mobile = mobile == 1
		st.Hosting = hosting == 1
		st.Proxy = proxyFlag == 1
		if lastError.Valid {
			st.LastError = lastError.String
		}
		if blacklistedUntil.Valid {
			st.BlacklistedUntil, _ = time.Parse(time.RFC3339Nano, blacklistedUntil.String)
		}
		if lastSuccess.Valid {
			st.LastSuccess, _ = time.Parse(time.RFC3339Nano, lastSuccess.String)
		}
		if lastFailure.Valid {
			st.LastFailure, _ = time.Parse(time.RFC3339Nano, lastFailure.String)
		}
		if checkedAt.Valid {
			st.QualityCheckedAt, _ = time.Parse(time.RFC3339Nano, checkedAt.String)
		}
		key := st.StateKey
		if key == "" {
			key = config.NodeStateKey(uri)
			st.StateKey = key
		}
		out[key] = st
	}
	return out, rows.Err()
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func (s *Store) SaveRuntimeState(ctx context.Context, nodeURI string, st monitor.PersistedState) error {
	nodeURI = strings.TrimSpace(nodeURI)
	if nodeURI == "" {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `INSERT INTO runtime_state (
		node_uri, state_key, available, initial_check_done, failure_count, blacklisted, blacklisted_until,
		last_error, last_latency_ms, last_success, last_failure,
		country_code, country, exit_ip, proxy_type, quality_source, quality_error, quality_checked_at,
		ip_valid, ip_version, ip_type, ip_invalid_reason, asn, as_name, isp, org, mobile, hosting, proxy, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(node_uri) DO UPDATE SET
		state_key=excluded.state_key,
		available=excluded.available,
		initial_check_done=excluded.initial_check_done,
		failure_count=excluded.failure_count,
		blacklisted=excluded.blacklisted,
		blacklisted_until=excluded.blacklisted_until,
		last_error=excluded.last_error,
		last_latency_ms=excluded.last_latency_ms,
		last_success=excluded.last_success,
		last_failure=excluded.last_failure,
		country_code=excluded.country_code,
		country=excluded.country,
		exit_ip=excluded.exit_ip,
		proxy_type=excluded.proxy_type,
		quality_source=excluded.quality_source,
		quality_error=excluded.quality_error,
		quality_checked_at=excluded.quality_checked_at,
		ip_valid=excluded.ip_valid,
		ip_version=excluded.ip_version,
		ip_type=excluded.ip_type,
		ip_invalid_reason=excluded.ip_invalid_reason,
		asn=excluded.asn,
		as_name=excluded.as_name,
		isp=excluded.isp,
		org=excluded.org,
		mobile=excluded.mobile,
		hosting=excluded.hosting,
		proxy=excluded.proxy,
		updated_at=excluded.updated_at`,
		nodeURI,
		strings.TrimSpace(st.StateKey),
		boolToInt(st.Available),
		boolToInt(st.InitialCheckDone),
		st.FailureCount,
		boolToInt(st.Blacklisted),
		nullableTime(st.BlacklistedUntil),
		st.LastError,
		st.LastLatencyMs,
		nullableTime(st.LastSuccess),
		nullableTime(st.LastFailure),
		st.CountryCode,
		st.Country,
		st.ExitIP,
		st.ProxyType,
		st.QualitySource,
		st.QualityError,
		nullableTime(st.QualityCheckedAt),
		boolToInt(st.IPValid),
		st.IPVersion,
		st.IPType,
		st.IPInvalidReason,
		st.ASN,
		st.ASName,
		st.ISP,
		st.Org,
		boolToInt(st.Mobile),
		boolToInt(st.Hosting),
		boolToInt(st.Proxy),
		now,
	)
	if err != nil {
		return fmt.Errorf("save runtime state: %w", err)
	}
	return nil
}

func (s *Store) DeleteRuntimeState(ctx context.Context, nodeURI string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM runtime_state WHERE node_uri = ?`, strings.TrimSpace(nodeURI))
	if err != nil {
		return fmt.Errorf("delete runtime state: %w", err)
	}
	return nil
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func isSQLiteUniqueURLConstraint(err error) bool {
	var se *sqlite.Error
	if !errors.As(err, &se) {
		return strings.Contains(strings.ToLower(err.Error()), "unique constraint failed: subscriptions.url")
	}
	return se.Code() == sqlite3.SQLITE_CONSTRAINT || se.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE
}
