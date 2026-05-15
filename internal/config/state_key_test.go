package config

import "testing"

func TestNodeStateKeyIgnoresFragmentAndQueryOrder(t *testing.T) {
	a := `vless://uuid@example.com:443?security=tls&type=ws&host=a.example.com&path=%2Fws#node-a`
	b := `vless://uuid@example.com:443?host=a.example.com&path=%2Fws&type=ws&security=tls#node-b`
	if NodeStateKey(a) == "" {
		t.Fatal("expected non-empty state key")
	}
	if NodeStateKey(a) != NodeStateKey(b) {
		t.Fatalf("expected equal state key for semantically identical nodes")
	}
}

func TestNodeStateKeyVmessJSONIgnoresPS(t *testing.T) {
	a := "vmess://eyJhZGQiOiJleGFtcGxlLmNvbSIsInBvcnQiOiI0NDMiLCJpZCI6InV1aWQtMSIsImFpZCI6IjAiLCJuZXQiOiJ3cyIsImhvc3QiOiJhLmV4YW1wbGUuY29tIiwicGF0aCI6Ii93cyIsInRscyI6InRscyIsInBzIjoiTmFtZS0xIn0="
	b := "vmess://eyJhZGQiOiJleGFtcGxlLmNvbSIsInBvcnQiOiI0NDMiLCJpZCI6InV1aWQtMSIsImFpZCI6IjAiLCJuZXQiOiJ3cyIsImhvc3QiOiJhLmV4YW1wbGUuY29tIiwicGF0aCI6Ii93cyIsInRscyI6InRscyIsInBzIjoiTmFtZS0yIn0="
	if NodeStateKey(a) != NodeStateKey(b) {
		t.Fatalf("expected vmess state key to ignore display name changes")
	}
}
