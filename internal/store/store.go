package store

import (
	"context"
	"time"

	"proxyweave/internal/config"
)

// SettingsStore persists runtime settings and can rebuild the effective config.
type SettingsStore interface {
	LoadConfig(ctx context.Context) (*config.Config, error)
	SaveSettings(ctx context.Context, cfg *config.Config) error
}

// NodeStore persists node definitions.
type NodeStore interface {
	ListNodeConfigs(ctx context.Context) ([]config.NodeConfig, error)
	CreateNode(ctx context.Context, node config.NodeConfig) (config.NodeConfig, error)
	UpdateNodeByName(ctx context.Context, currentName string, node config.NodeConfig) (config.NodeConfig, error)
	DeleteNodeByName(ctx context.Context, name string) error
}

// Subscription is the persisted subscription model.
type Subscription struct {
	ID              int64         `json:"id"`
	Name            string        `json:"name"`
	URL             string        `json:"url"`
	Enabled         bool          `json:"enabled"`
	RefreshInterval time.Duration `json:"refresh_interval"`
	LastRefresh     time.Time     `json:"last_refresh,omitempty"`
	LastError       string        `json:"last_error,omitempty"`
	LastNodeCount   int           `json:"last_node_count"`
	CreatedAt       time.Time     `json:"created_at,omitempty"`
	UpdatedAt       time.Time     `json:"updated_at,omitempty"`
}

type SubscriptionNodesPage struct {
	Nodes      []config.NodeConfig `json:"nodes"`
	Total      int                 `json:"total"`
	Page       int                 `json:"page"`
	PageSize   int                 `json:"page_size"`
	TotalPages int                 `json:"total_pages"`
}

// SubscriptionStore persists subscriptions and their imported nodes.
type SubscriptionStore interface {
	ListSubscriptions(ctx context.Context) ([]Subscription, error)
	GetSubscription(ctx context.Context, id int64) (Subscription, error)
	CreateSubscription(ctx context.Context, sub Subscription) (Subscription, error)
	UpdateSubscription(ctx context.Context, sub Subscription) (Subscription, error)
	DeleteSubscription(ctx context.Context, id int64) error
	ListSubscriptionNodes(ctx context.Context, id int64, page int, pageSize int) (SubscriptionNodesPage, error)
	ReplaceSubscriptionNodes(ctx context.Context, sub Subscription, nodes []config.NodeConfig) error
	MarkSubscriptionRefreshError(ctx context.Context, id int64, msg string) error
}

// AppStore is the combined persistence interface used by the runtime.
type AppStore interface {
	SettingsStore
	NodeStore
	SubscriptionStore
}
