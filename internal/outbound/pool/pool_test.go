package pool

import "testing"

func TestIsNoContentProbePath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "empty defaults to generate 204", path: "", want: true},
		{name: "generate 204", path: "/generate_204", want: true},
		{name: "generate 204 with query", path: "/generate_204?cachebust=1", want: true},
		{name: "other path", path: "/status", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isNoContentProbePath(tt.path); got != tt.want {
				t.Fatalf("isNoContentProbePath() = %v, want %v", got, tt.want)
			}
		})
	}
}
