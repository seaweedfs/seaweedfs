package wdclient

import (
	"context"
	"strings"
	"testing"
)

type testLocationProvider struct {
	locations map[string][]Location
}

func (p *testLocationProvider) LookupVolumeIds(ctx context.Context, volumeIds []string) (map[string][]Location, error) {
	result := make(map[string][]Location)
	for _, vid := range volumeIds {
		if locs, found := p.locations[vid]; found {
			result[vid] = locs
		}
	}
	return result, nil
}

// TestLookupFileIdWithFallbackLocalFirst ensures volumes whose data is still
// local are tried before remote-tier replicas, on the provider (cache miss) path.
func TestLookupFileIdWithFallbackLocalFirst(t *testing.T) {
	vc := newVidMapClient(&testLocationProvider{
		locations: map[string][]Location{
			"5": {
				{Url: "10.0.0.1:8080", DataInRemote: true},
				{Url: "10.0.0.2:8080", DataInRemote: false},
			},
		},
	}, "", 5)

	urls, err := vc.LookupFileIdWithFallback(context.Background(), "5,abcdef0123456789")
	if err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
	if len(urls) != 2 {
		t.Fatalf("expected 2 urls, got %v", urls)
	}
	hasLocal, hasRemote := false, false
	for _, u := range urls {
		if strings.Contains(u, "10.0.0.2:8080") {
			hasLocal = true
		}
		if strings.Contains(u, "10.0.0.1:8080") {
			hasRemote = true
		}
	}
	if !hasLocal {
		t.Errorf("local replica missing from result: %v", urls)
	}
	if !hasRemote {
		t.Errorf("remote replica missing from result: %v", urls)
	}
	if !strings.Contains(urls[0], "10.0.0.2:8080") {
		t.Errorf("expected local replica first, got %v", urls)
	}
}

// TestLookupFileIdWithFallbackAllRemote keeps shuffled order when every replica is remote.
func TestLookupFileIdWithFallbackAllRemote(t *testing.T) {
	vc := newVidMapClient(&testLocationProvider{
		locations: map[string][]Location{
			"6": {
				{Url: "10.0.0.1:8080", DataInRemote: true},
				{Url: "10.0.0.2:8080", DataInRemote: true},
			},
		},
	}, "", 5)

	urls, err := vc.LookupFileIdWithFallback(context.Background(), "6,abcdef0123456789")
	if err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
	if len(urls) != 2 {
		t.Fatalf("expected 2 urls, got %v", urls)
	}
}

// TestLookupFileIdWithFallbackGlobalLocalFirst verifies that a local replica in
// any data center is preferred over a same-DC remote replica. The cheap read
// should win even when it crosses a data-center boundary.
func TestLookupFileIdWithFallbackGlobalLocalFirst(t *testing.T) {
	vc := newVidMapClient(&testLocationProvider{
		locations: map[string][]Location{
			"7": {
				{Url: "10.0.0.1:8080", DataCenter: "dc1", DataInRemote: true},
				{Url: "10.0.0.2:8080", DataCenter: "dc1", DataInRemote: false},
				{Url: "10.0.0.3:8080", DataCenter: "dc2", DataInRemote: false},
				{Url: "10.0.0.4:8080", DataCenter: "dc2", DataInRemote: true},
			},
		},
	}, "dc1", 5)

	urls, err := vc.LookupFileIdWithFallback(context.Background(), "7,abcdef0123456789")
	if err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
	if len(urls) != 4 {
		t.Fatalf("expected 4 urls, got %v", urls)
	}

	// First two: the local replicas (dc1-local, dc2-local) in some order.
	// Last two: the remote replicas (dc1-remote, dc2-remote) in some order.
	// DC priority is preserved within each tier.
	first := urls[:2]
	second := urls[2:]

	for _, u := range first {
		if !strings.Contains(u, "10.0.0.2:8080") && !strings.Contains(u, "10.0.0.3:8080") {
			t.Errorf("first half must be local replicas, got %v", first)
		}
		if strings.Contains(u, "10.0.0.1:8080") || strings.Contains(u, "10.0.0.4:8080") {
			t.Errorf("first half must not contain remote replicas, got %v", first)
		}
	}
	for _, u := range second {
		if !strings.Contains(u, "10.0.0.1:8080") && !strings.Contains(u, "10.0.0.4:8080") {
			t.Errorf("second half must be remote replicas, got %v", second)
		}
	}

	// Within each tier, DC1 precedes DC2.
	dc1Local, dc2Local := -1, -1
	dc1Remote, dc2Remote := -1, -1
	for i, u := range urls {
		switch {
		case strings.Contains(u, "10.0.0.2:8080"):
			dc1Local = i
		case strings.Contains(u, "10.0.0.3:8080"):
			dc2Local = i
		case strings.Contains(u, "10.0.0.1:8080"):
			dc1Remote = i
		case strings.Contains(u, "10.0.0.4:8080"):
			dc2Remote = i
		}
	}
	if dc1Local > dc2Local {
		t.Errorf("dc1-local should precede dc2-local, got order %v", urls)
	}
	if dc1Remote > dc2Remote {
		t.Errorf("dc1-remote should precede dc2-remote, got order %v", urls)
	}
}
