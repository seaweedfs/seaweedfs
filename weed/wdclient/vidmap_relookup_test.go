package wdclient

import (
	"context"
	"errors"
	"sort"
	"testing"
)

// stubProvider answers lookups with locations, with no locations, or with an
// error, as the test sets it.
type stubProvider struct {
	locations map[string][]Location
	err       error
	calls     int
}

func (p *stubProvider) LookupVolumeIds(ctx context.Context, volumeIds []string) (map[string][]Location, error) {
	p.calls++
	if p.err != nil {
		return nil, p.err
	}
	result := make(map[string][]Location)
	for _, vid := range volumeIds {
		result[vid] = p.locations[vid]
	}
	return result, nil
}

func sortedUrls(locations []Location) []string {
	urls := make([]string, 0, len(locations))
	for _, loc := range locations {
		urls = append(urls, loc.Url)
	}
	sort.Strings(urls)
	return urls
}

func requireLocations(t *testing.T, vc *vidMapClient, vid uint32, want ...string) {
	t.Helper()
	locations, found := vc.GetLocations(vid)
	if len(want) == 0 {
		if found {
			t.Fatalf("volume %d: got %v, want no locations", vid, sortedUrls(locations))
		}
		return
	}
	got := sortedUrls(locations)
	if !found || len(got) != len(want) {
		t.Fatalf("volume %d: got %v, want %v", vid, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("volume %d: got %v, want %v", vid, got, want)
		}
	}
}

// A read that finds one replica dead invalidates the volume and looks it up
// again. If that lookup gets no answer, the cache must keep the replicas it
// had: before the fix it was left empty, and every later read of the volume
// failed although its other replica was serving.
func TestInvalidateKeepsLocationsWhenRelookupFails(t *testing.T) {
	provider := &stubProvider{err: errors.New("filer unavailable")}
	vc := newVidMapClient(provider, "", DefaultVidMapCacheSize)
	vc.addLocation(5, Location{Url: "dead:8080"})
	vc.addLocation(5, Location{Url: "live:8080"})

	vc.InvalidateCache("5,0102")
	requireLocations(t, vc, 5)

	urls, err := vc.LookupFileIdWithFallback(context.Background(), "5,0102")
	if err != nil {
		t.Fatalf("lookup after a failed refresh: %v", err)
	}
	if len(urls) != 2 {
		t.Fatalf("urls = %v, want both replicas", urls)
	}
	requireLocations(t, vc, 5, "dead:8080", "live:8080")
	if provider.calls != 1 {
		t.Fatalf("provider asked %d times, want 1", provider.calls)
	}
}

// An answer replaces what was dropped, and a later failed refresh restores
// that answer rather than the older locations.
func TestRelookupAnswerReplacesDroppedLocations(t *testing.T) {
	provider := &stubProvider{locations: map[string][]Location{"5": {{Url: "live:8080"}}}}
	vc := newVidMapClient(provider, "", DefaultVidMapCacheSize)
	vc.addLocation(5, Location{Url: "dead:8080"})
	vc.addLocation(5, Location{Url: "live:8080"})

	vc.InvalidateCache("5,0102")
	if _, err := vc.LookupFileIdWithFallback(context.Background(), "5,0102"); err != nil {
		t.Fatal(err)
	}
	requireLocations(t, vc, 5, "live:8080")

	provider.err = errors.New("filer unavailable")
	vc.InvalidateCache("5,0102")
	if _, err := vc.LookupFileIdWithFallback(context.Background(), "5,0102"); err != nil {
		t.Fatal(err)
	}
	requireLocations(t, vc, 5, "live:8080")
}

// A provider that answers that the volume has no locations is believed: the
// dropped locations are not put back.
func TestRelookupNotFoundAnswerIsBelieved(t *testing.T) {
	provider := &stubProvider{locations: map[string][]Location{}}
	vc := newVidMapClient(provider, "", DefaultVidMapCacheSize)
	vc.addLocation(5, Location{Url: "gone:8080"})

	vc.InvalidateCache("5,0102")
	if _, err := vc.LookupFileIdWithFallback(context.Background(), "5,0102"); err == nil {
		t.Fatal("lookup succeeded, want not found")
	}
	requireLocations(t, vc, 5)

	provider.err = errors.New("filer unavailable")
	if _, err := vc.LookupFileIdWithFallback(context.Background(), "5,0102"); err == nil {
		t.Fatal("a later failed lookup brought back locations the provider said were gone")
	}
}

// A location update pushed after the invalidation is newer than what was
// dropped, so a failed refresh must not put the dropped locations back.
func TestPushedUpdateForgetsDroppedLocations(t *testing.T) {
	provider := &stubProvider{err: errors.New("filer unavailable")}
	vc := newVidMapClient(provider, "", DefaultVidMapCacheSize)
	vc.addLocation(5, Location{Url: "dead:8080"})
	vc.addLocation(5, Location{Url: "live:8080"})

	vc.InvalidateCache("5,0102")
	vc.deleteLocation(5, Location{Url: "dead:8080"})

	if _, err := vc.LookupFileIdWithFallback(context.Background(), "5,0102"); err == nil {
		t.Fatal("dropped locations came back after a newer update")
	}
	requireLocations(t, vc, 5)
}

// restoreVid leaves a volume alone once it has been learned again, and skips
// an entry that resets since its removal would have expired.
func TestRestoreVid(t *testing.T) {
	t.Run("learned again", func(t *testing.T) {
		vm := newVidMap("", DefaultVidMapCacheSize)
		vm.addLocation(5, Location{Url: "old:8080"})
		taken := vm.takeVid(5)
		vm.addLocation(5, Location{Url: "new:8080"})
		if vm.restoreVid(5, taken) {
			t.Fatal("restored over a newer entry")
		}
		locations, _ := vm.GetLocations(5)
		if got := sortedUrls(locations); len(got) != 1 || got[0] != "new:8080" {
			t.Fatalf("locations = %v", got)
		}
	})

	t.Run("expired", func(t *testing.T) {
		vm := newVidMap("", 2)
		vm.addLocation(5, Location{Url: "old:8080"})
		taken := vm.takeVid(5)
		for i := 0; i < 3; i++ {
			vm.reset()
		}
		if vm.restoreVid(5, taken) {
			t.Fatal("restored an entry older than the retained generations")
		}
	})

	t.Run("server references", func(t *testing.T) {
		vm := newVidMap("", DefaultVidMapCacheSize)
		vm.addLocation(5, Location{Url: "a:8080"})
		vm.addEcLocation(6, Location{Url: "b:8080"})
		taken5, taken6 := vm.takeVid(5), vm.takeVid(6)
		if vm.hasVolumeServer("a:8080") || vm.hasVolumeServer("b:8080") {
			t.Fatal("takeVid kept server references")
		}
		if !vm.restoreVid(5, taken5) || !vm.restoreVid(6, taken6) {
			t.Fatal("restore refused")
		}
		if !vm.hasVolumeServer("a:8080") || !vm.hasVolumeServer("b:8080") {
			t.Fatal("restoreVid did not take the server references back")
		}
		if locations, found := vm.GetLocations(6); !found || locations[0].Url != "b:8080" {
			t.Fatalf("EC entry not restored as EC: %v", locations)
		}
	})
}

// A location a pushed removal fenced off after the invalidation stays out of
// what is restored; the volume's other replicas come back.
func TestRestoreLeavesOutFencedLocations(t *testing.T) {
	provider := &stubProvider{err: errors.New("filer unavailable")}
	vc := newVidMapClient(provider, "", DefaultVidMapCacheSize)
	vc.addLocation(5, Location{Url: "dead:8080"})
	vc.addLocation(5, Location{Url: "live:8080"})

	vc.InvalidateCache("5,0102")
	vc.tombstone(5, Location{Url: "dead:8080"})

	if _, err := vc.LookupFileIdWithFallback(context.Background(), "5,0102"); err != nil {
		t.Fatalf("lookup after a failed refresh: %v", err)
	}
	requireLocations(t, vc, 5, "live:8080")
}

// A reset or clear after the invalidation means the dropped locations are
// older than what the cache is being refilled with, so they are not restored.
func TestRestoreSkippedAfterEpochMoves(t *testing.T) {
	for name, move := range map[string]func(vc *vidMapClient){
		"reset": func(vc *vidMapClient) { vc.resetVidMap() },
		"clear": func(vc *vidMapClient) { vc.clearVidMap() },
	} {
		t.Run(name, func(t *testing.T) {
			provider := &stubProvider{err: errors.New("filer unavailable")}
			vc := newVidMapClient(provider, "", DefaultVidMapCacheSize)
			vc.addLocation(5, Location{Url: "live:8080"})

			vc.InvalidateCache("5,0102")
			move(vc)

			if _, err := vc.LookupFileIdWithFallback(context.Background(), "5,0102"); err == nil {
				t.Fatal("restored locations dropped before the epoch moved")
			}
			requireLocations(t, vc, 5)
		})
	}
}
