package monitor

import (
	"net/url"
	"testing"
)

func TestIsNoContentProbeTarget(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{name: "default cloudflare target", raw: "http://cp.cloudflare.com/generate_204", want: true},
		{name: "target with query", raw: "https://example.com/generate_204?x=1", want: true},
		{name: "different target", raw: "https://example.com/status", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.raw)
			if err != nil {
				t.Fatal(err)
			}
			if got := isNoContentProbeTarget(u); got != tt.want {
				t.Fatalf("isNoContentProbeTarget() = %v, want %v", got, tt.want)
			}
		})
	}
}
