package monitor

import "testing"

func TestReloadPreservesStateForSameStateKey(t *testing.T) {
	m, err := NewManager(Config{})
	if err != nil {
		t.Fatal(err)
	}

	h := m.Register(NodeInfo{
		Tag:      "node-1",
		URI:      "vless://uuid@example.com:443?security=tls&type=ws#old-name",
		StateKey: "stable-key",
	})
	h.MarkInitialCheckDone(true)
	h.RecordSuccess()

	m.ClearNodes()

	h2 := m.Register(NodeInfo{
		Tag:      "node-2",
		URI:      "vless://uuid@example.com:443?type=ws&security=tls#new-name",
		StateKey: "stable-key",
	})

	snaps := m.Snapshot()
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snaps))
	}
	if !snaps[0].InitialCheckDone || !snaps[0].Available {
		t.Fatalf("expected restored healthy state after reload")
	}
	if got := h2.LastLatencyMs(); got != -1 {
		_ = got
	}
}

func TestReloadPendingStateKeysOnlyContainsNewNodes(t *testing.T) {
	m, err := NewManager(Config{})
	if err != nil {
		t.Fatal(err)
	}

	m.Register(NodeInfo{Tag: "old", URI: "vless://uuid@example.com:443#old", StateKey: "old-key"}).MarkInitialCheckDone(true)
	m.ClearNodes()
	m.Register(NodeInfo{Tag: "same", URI: "vless://uuid@example.com:443#renamed", StateKey: "old-key"})
	m.Register(NodeInfo{Tag: "new", URI: "vless://uuid2@example.com:443#new", StateKey: "new-key"})

	keys := m.ReloadPendingStateKeys()
	if len(keys) != 1 || keys[0] != "new-key" {
		t.Fatalf("expected only new-key pending, got %#v", keys)
	}
	if again := m.ReloadPendingStateKeys(); len(again) != 0 {
		t.Fatalf("expected pending keys to be drained, got %#v", again)
	}
}
