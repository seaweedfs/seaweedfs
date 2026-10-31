package wdclient

import (
	"context"
	"strings"
	"testing"
	"time"
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

// TestLookupFileIdWithFallbackKeepsDataCenterFirst verifies that the local-first
// ordering applies inside each data center and does not override the data-center
// preference: a same-DC remote replica still beats an other-DC local one, because
// the remote tier is usually nearer than another data center.
func TestLookupFileIdWithFallbackKeepsDataCenterFirst(t *testing.T) {
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

	// dc1 local, dc1 remote, dc2 local, dc2 remote.
	want := []string{"10.0.0.2:8080", "10.0.0.1:8080", "10.0.0.3:8080", "10.0.0.4:8080"}
	for i, host := range want {
		if !strings.Contains(urls[i], host) {
			t.Fatalf("position %d should be %s, got %v", i, host, urls)
		}
	}
}

// resetOnLookupProvider answers after clearing the cache, as a provider
// response landing on the far side of a master reconnect would.
type resetOnLookupProvider struct {
	vc *vidMapClient
}

func (p *resetOnLookupProvider) LookupVolumeIds(ctx context.Context, volumeIds []string) (map[string][]Location, error) {
	p.vc.clearVidMap()
	return map[string][]Location{"5": {{Url: "stale:8080"}}}, nil
}

func TestProviderFillServesWithoutCachingAcrossReset(t *testing.T) {
	provider := &resetOnLookupProvider{}
	vc := newVidMapClient(provider, "", 5)
	provider.vc = vc

	locations, err := vc.LookupVolumeIdsWithFallback(context.Background(), []string{"5"})
	if err != nil || len(locations["5"]) != 1 {
		t.Fatalf("lookup crossing a cache reset returned %v, err %v", locations["5"], err)
	}
	if cached, found := vc.GetLocations(5); found && len(cached) > 0 {
		t.Fatalf("lookup crossed a cache reset but cached %v", cached)
	}
}

func TestProviderFillSkipsTombstonedLocation(t *testing.T) {
	retired := Location{Url: "retired:8080"}
	live := Location{Url: "live:8080"}
	vc := newVidMapClient(&testLocationProvider{
		locations: map[string][]Location{"5": {retired, live}},
	}, "", 5)

	vc.tombstone(5, retired)

	locations, err := vc.LookupVolumeIdsWithFallback(context.Background(), []string{"5"})
	if err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
	if got := locations["5"]; len(got) != 1 || got[0].Url != live.Url {
		t.Fatalf("lookup returned %v, want only the surviving location", got)
	}
	if cached, found := vc.GetLocations(5); !found || len(cached) != 1 || cached[0].Url != live.Url {
		t.Errorf("cache holds %v, want only the surviving location", cached)
	}
}

func TestTombstoneExpires(t *testing.T) {
	retired := Location{Url: "retired:8080"}
	vc := newVidMapClient(&testLocationProvider{
		locations: map[string][]Location{"5": {retired}},
	}, "", 5)
	vc.tombstone(5, retired)
	e := vc.recentDeletes[vidLocationKey{vid: 5, url: retired.Url}]
	e.Value = recentDelete{key: vidLocationKey{vid: 5, url: retired.Url}, at: time.Now().Add(-recentDeleteTTL)}

	locations, err := vc.LookupVolumeIdsWithFallback(context.Background(), []string{"5"})
	if err != nil || len(locations["5"]) != 1 {
		t.Fatalf("expired mark still fenced the location: %v, err %v", locations["5"], err)
	}
	if len(vc.recentDeletes) != 0 || vc.recentDeleteOrder.Len() != 0 {
		t.Errorf("expired mark kept: %d marks, %d ordered", len(vc.recentDeletes), vc.recentDeleteOrder.Len())
	}
}

func TestTombstoneEvictsOldestAtCap(t *testing.T) {
	vc := newVidMapClient(nil, "", 5)
	for vid := uint32(0); vid <= recentDeleteCap; vid++ {
		vc.tombstone(vid, Location{Url: "a:8080"})
	}
	vc.tombstone(recentDeleteCap, Location{Url: "a:8080"})

	if len(vc.recentDeletes) != recentDeleteCap || vc.recentDeleteOrder.Len() != recentDeleteCap {
		t.Fatalf("fence holds %d marks (%d ordered), want %d", len(vc.recentDeletes), vc.recentDeleteOrder.Len(), recentDeleteCap)
	}
	if _, found := vc.recentDeletes[vidLocationKey{vid: 0, url: "a:8080"}]; found {
		t.Errorf("oldest mark survived eviction")
	}
	if _, found := vc.recentDeletes[vidLocationKey{vid: 1, url: "a:8080"}]; !found {
		t.Errorf("a repeated mark evicted another one")
	}
}
