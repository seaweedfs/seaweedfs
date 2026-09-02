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
