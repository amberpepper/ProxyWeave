package pool

import (
	"context"
	"net"
	"sort"
	"strings"
	"sync"

	M "github.com/sagernet/sing/common/metadata"
)

// NetDialer provides standard Go net.Conn dialing through a pool outbound.
type NetDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

var dialerRegistry sync.Map // map[string]NetDialer

// poolDialerAdapter wraps a poolOutbound to satisfy NetDialer.
type poolDialerAdapter struct {
	pool *poolOutbound
}

func (a *poolDialerAdapter) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	dest := M.ParseSocksaddr(address)
	return a.pool.DialContext(ctx, network, dest)
}

// registerDialer adds a pool outbound to the global dialer registry.
func registerDialer(tag string, p *poolOutbound) {
	dialerRegistry.Store(tag, &poolDialerAdapter{pool: p})
}

// GetDialer returns a NetDialer for the given pool tag.
func GetDialer(tag string) (NetDialer, bool) {
	v, ok := dialerRegistry.Load(tag)
	if !ok {
		return nil, false
	}
	return v.(NetDialer), true
}

// ResetDialerRegistry clears the dialer registry (called during config reload).
func ResetDialerRegistry() {
	dialerRegistry.Range(func(key, _ any) bool {
		dialerRegistry.Delete(key)
		return true
	})
}

// ListDialerTagsByPrefix returns registered dialer tags with the given prefix.
func ListDialerTagsByPrefix(prefix string) []string {
	var tags []string
	dialerRegistry.Range(func(key, _ any) bool {
		tag, ok := key.(string)
		if ok && strings.HasPrefix(tag, prefix) {
			tags = append(tags, tag)
		}
		return true
	})
	sort.Strings(tags)
	return tags
}
