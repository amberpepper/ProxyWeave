package config

import "strings"

func ShouldRunStartupHealthCheck(mode string, minAvailable int) bool {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "always":
		return true
	case "never":
		return false
	case "", "auto":
		return minAvailable > 0
	default:
		return minAvailable > 0
	}
}

func ResolveStartupHealthCheck(modeFromPool string, minFromPool int, modeFromSubscription string, minFromSubscription int) (string, int) {
	modeFromPool = strings.TrimSpace(modeFromPool)
	if modeFromPool != "" {
		return modeFromPool, minFromPool
	}
	if minFromPool > 0 {
		return "", minFromPool
	}
	return strings.TrimSpace(modeFromSubscription), minFromSubscription
}

func ResolveStartupHealthCheckFromConfig(cfg *Config) (string, int) {
	if cfg == nil {
		return "", 0
	}
	if cfg.SuppressStartupHealthCheck {
		return "never", 0
	}
	return ResolveStartupHealthCheck(
		cfg.Pool.StartupHealthCheck,
		cfg.Pool.MinAvailable,
		cfg.SubscriptionRefresh.StartupHealthCheck,
		cfg.SubscriptionRefresh.MinAvailableNodes,
	)
}
