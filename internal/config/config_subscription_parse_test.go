package config

import (
	"encoding/base64"
	"testing"
)

func TestParseSubscriptionContent_SupportsBareHTTPProxyLines(t *testing.T) {
	content := "170.239.207.241:999\n42.2.156.79:80:user:pass\nhttp://1.2.3.4:8080\n# comment\n"

	nodes, err := ParseSubscriptionContent(content)
	if err != nil {
		t.Fatalf("ParseSubscriptionContent error: %v", err)
	}

	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	if got := nodes[0].URI; got != "http://170.239.207.241:999" {
		t.Fatalf("unexpected first URI: %s", got)
	}

	if got := nodes[1].URI; got != "http://user:pass@42.2.156.79:80" {
		t.Fatalf("unexpected second URI: %s", got)
	}

	if got := nodes[2].URI; got != "http://1.2.3.4:8080" {
		t.Fatalf("unexpected third URI: %s", got)
	}
}

func TestParseSubscriptionContent_Base64BareHTTPProxyLines(t *testing.T) {
	raw := "170.239.207.241:999\n"
	content := base64.StdEncoding.EncodeToString([]byte(raw))

	nodes, err := ParseSubscriptionContent(content)
	if err != nil {
		t.Fatalf("ParseSubscriptionContent error: %v", err)
	}

	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}

	if got := nodes[0].URI; got != "http://170.239.207.241:999" {
		t.Fatalf("unexpected URI: %s", got)
	}
}
