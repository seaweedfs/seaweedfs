package wdclient

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
)

// stubProvider answers lookups with locations, with no locations, or with an
// error, as the test sets it. beforeAnswer, when set, runs inside the lookup
// before it returns, the way a pushed update lands while a lookup is in flight.
// answer, when set, decides each call by its number instead.
type stubProvider struct {
	mu           sync.Mutex
	locations    map[string][]Location
	err          error
	calls        int
	beforeAnswer func()
	answer       func(call int) (map[string][]Location, error)
}

func (p *stubProvider) LookupVolumeIds(ctx context.Context, volumeIds []string) (map[string][]Location, error) {
	p.mu.Lock()
	p.calls++
	call, err, beforeAnswer, answer := p.calls, p.err, p.beforeAnswer, p.answer
	p.mu.Unlock()
	if answer != nil {
		return answer(call)
	}
	if beforeAnswer != nil {
		beforeAnswer()
	}
	if err != nil {
		return nil, err
	}
	result := make(map[string][]Location)
	for _, vid := range volumeIds {
		result[vid] = p.locations[vid]
	}
	return result, nil
}

var errNoAnswer = errors.New("filer unavailable")

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

// refresh does what a read does after a replica failed: invalidate the
// volume, then look it up again.
func refresh(vc *vidMapClient) error {
	vc.InvalidateCache("5,0102")
	_, err := vc.LookupFileIdWithFallback(context.Background(), "5,0102")
	return err
}

// A read that finds one replica dead invalidates the volume and looks it up
// again. If that lookup gets no answer, the cache must keep the replicas it
// had: before the fix it was left empty, and every later read of the volume
// failed although its other replica was serving.
func TestInvalidateKeepsLocationsWhenRelookupFails(t *testing.T) {
	provider := &stubProvider{err: errNoAnswer}
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

// A lookup that recovered every volume it was asked for reports no error: a
// gRPC handler that returns one drops the locations along with it. A volume
// that stays unresolved is still an error, next to what was recovered.
func TestFailedRelookupReportsOnlyUnresolvedVolumes(t *testing.T) {
	provider := &stubProvider{err: errNoAnswer}
	vc := newVidMapClient(provider, "", DefaultVidMapCacheSize)
	vc.addLocation(5, Location{Url: "live:8080"})

	vc.InvalidateCache("5,0102")
	locations, err := vc.LookupVolumeIdsWithFallback(context.Background(), []string{"5"})
	if err != nil || len(locations["5"]) != 1 {
		t.Fatalf("recovered lookup: locations %v, err %v", locations, err)
	}

	vc.InvalidateCache("5,0102")
	locations, err = vc.LookupVolumeIdsWithFallback(context.Background(), []string{"5", "6"})
	if err == nil {
		t.Fatal("volume 6 has no locations, want an error")
	}
	if len(locations["5"]) != 1 || len(locations["6"]) != 0 {
		t.Fatalf("locations %v, want volume 5 only", locations)
	}
}

// An answer replaces what was set aside, and a later failed refresh brings
// back that answer rather than the older locations.
func TestRelookupAnswerReplacesStaleLocations(t *testing.T) {
	provider := &stubProvider{locations: map[string][]Location{"5": {{Url: "live:8080"}}}}
	vc := newVidMapClient(provider, "", DefaultVidMapCacheSize)
	vc.addLocation(5, Location{Url: "dead:8080"})
	vc.addLocation(5, Location{Url: "live:8080"})

	if err := refresh(vc); err != nil {
		t.Fatal(err)
	}
	requireLocations(t, vc, 5, "live:8080")

	provider.err = errNoAnswer
	if err := refresh(vc); err != nil {
		t.Fatal(err)
	}
	requireLocations(t, vc, 5, "live:8080")
}

// A provider that answers that the volume has no locations is believed: what
// was set aside does not come back, then or on a later failed lookup.
func TestRelookupNotFoundAnswerIsBelieved(t *testing.T) {
	provider := &stubProvider{locations: map[string][]Location{}}
	vc := newVidMapClient(provider, "", DefaultVidMapCacheSize)
	vc.addLocation(5, Location{Url: "gone:8080"})

	if err := refresh(vc); err == nil {
		t.Fatal("lookup succeeded, want not found")
	}
	requireLocations(t, vc, 5)

	provider.err = errNoAnswer
	if _, err := vc.LookupFileIdWithFallback(context.Background(), "5,0102"); err == nil {
		t.Fatal("a later failed lookup brought back locations the provider said were gone")
	}
}

// Two lookups of one volume can overlap under different batch keys. When one
// fails and brings the old locations back while the other is still in flight,
// the other's answer is the newer knowledge and replaces them.
func TestAnswerReplacesLocationsRevivedMeanwhile(t *testing.T) {
	for name, tc := range map[string]struct {
		answer []Location
		want   []string
	}{
		"no locations": {nil, nil},
		"one replica":  {[]Location{{Url: "live:8080"}}, []string{"live:8080"}},
	} {
		t.Run(name, func(t *testing.T) {
			provider := &stubProvider{}
			vc := newVidMapClient(provider, "", DefaultVidMapCacheSize)
			vc.addLocation(5, Location{Url: "dead:8080"})
			vc.addLocation(5, Location{Url: "live:8080"})
			provider.answer = func(call int) (map[string][]Location, error) {
				if call > 1 {
					return nil, errNoAnswer
				}
				// While this lookup is in flight, another one fails and
				// falls back to what was set aside.
				if _, err := vc.LookupVolumeIdsWithFallback(context.Background(), []string{"5"}); err != nil {
					t.Errorf("overlapping lookup: %v", err)
				}
				if locations, _ := vc.GetLocations(5); len(locations) != 2 {
					t.Errorf("overlapping lookup revived %v, want both replicas", locations)
				}
				return map[string][]Location{"5": tc.answer, "6": {{Url: "six:8080"}}}, nil
			}

			vc.InvalidateCache("5,0102")
			vc.LookupVolumeIdsWithFallback(context.Background(), []string{"5", "6"})

			requireLocations(t, vc, 5, tc.want...)
			requireLocations(t, vc, 6, "six:8080")
		})
	}
}

// masterClientWith is a MasterClient whose cache is fed by pushes, as from the
// master's stream, and whose lookups go to provider.
func masterClientWith(provider VolumeLocationProvider) *MasterClient {
	return &MasterClient{vidMapClient: newVidMapClient(provider, "", DefaultVidMapCacheSize)}
}

// A removal the master pushes must hold however it is ordered against a
// refresh whose lookup gets no answer: the removed server stays out of the
// cache, and the volume's other servers still come back.
func TestPushedRemovalHoldsAcrossFailedRefresh(t *testing.T) {
	type push struct{ newVids, deletedVids, newEcVids, deletedEcVids []uint32 }
	forms := map[string]struct{ add, remove push }{
		"regular": {push{newVids: []uint32{5}}, push{deletedVids: []uint32{5}}},
		"ec":      {push{newEcVids: []uint32{5}}, push{deletedEcVids: []uint32{5}}},
	}
	send := func(mc *MasterClient, url string, p push) {
		pushVolumeLocation(mc, url, p.newVids, p.deletedVids, p.newEcVids, p.deletedEcVids)
	}

	for form, pushes := range forms {
		for _, tc := range []struct {
			name    string
			servers []string
			want    []string
		}{
			{"another replica left", []string{"old:8080", "live:8080"}, []string{"live:8080"}},
			{"only replica", []string{"old:8080"}, nil},
		} {
			setup := func() (*MasterClient, *stubProvider) {
				provider := &stubProvider{err: errNoAnswer}
				mc := masterClientWith(provider)
				for _, url := range tc.servers {
					send(mc, url, pushes.add)
				}
				return mc, provider
			}
			check := func(t *testing.T, mc *MasterClient) {
				t.Helper()
				requireLocations(t, mc.vidMapClient, 5, tc.want...)
				// and it does not come back on the refreshes that follow
				refresh(mc.vidMapClient)
				requireLocations(t, mc.vidMapClient, 5, tc.want...)
			}

			t.Run(form+"/"+tc.name+"/removed before the lookup", func(t *testing.T) {
				mc, _ := setup()
				mc.InvalidateCache("5,0102")
				send(mc, "old:8080", pushes.remove)
				mc.LookupFileIdWithFallback(context.Background(), "5,0102")
				check(t, mc)
			})

			t.Run(form+"/"+tc.name+"/removed during the lookup", func(t *testing.T) {
				mc, provider := setup()
				var once sync.Once
				provider.beforeAnswer = func() {
					once.Do(func() { send(mc, "old:8080", pushes.remove) })
				}
				refresh(mc.vidMapClient)
				check(t, mc)
			})

			t.Run(form+"/"+tc.name+"/removed after the lookup", func(t *testing.T) {
				mc, _ := setup()
				refresh(mc.vidMapClient)
				send(mc, "old:8080", pushes.remove)
				check(t, mc)
			})
		}
	}
}

// The same through a filer client that hears the master's pushes: the removal
// fences the server and takes it out of what was set aside.
func TestFencedRemovalHoldsAcrossFailedRefresh(t *testing.T) {
	old := Location{Url: "old:8080"}
	live := Location{Url: "live:8080"}
	provider := &stubProvider{locations: map[string][]Location{"5": {old, live}}}
	mc := moveClient()
	fc := hookedFilerClient(mc, provider)
	pushVolumeLocation(mc, old.Url, []uint32{5}, nil, nil, nil)
	pushVolumeLocation(mc, live.Url, []uint32{5}, nil, nil, nil)
	if _, err := fc.LookupFileIdWithFallback(context.Background(), "5,0102"); err != nil {
		t.Fatal(err)
	}
	requireLocations(t, fc.vidMapClient, 5, "live:8080", "old:8080")

	provider.err = errNoAnswer
	fc.InvalidateCache("5,0102")
	pushVolumeLocation(mc, old.Url, nil, []uint32{5}, nil, nil)
	if _, err := fc.LookupFileIdWithFallback(context.Background(), "5,0102"); err != nil {
		t.Fatalf("lookup after a failed refresh: %v", err)
	}
	requireLocations(t, fc.vidMapClient, 5, "live:8080")
}

// A removal racing refreshes that get no answer: whatever the interleaving,
// the removed server is gone once both are done. Run with -race.
func TestPushedRemovalRacingFailedRefreshes(t *testing.T) {
	for i := 0; i < 300; i++ {
		mc := masterClientWith(&stubProvider{err: errNoAnswer})
		pushVolumeLocation(mc, "old:8080", []uint32{5}, nil, nil, nil)
		pushVolumeLocation(mc, "live:8080", []uint32{5}, nil, nil, nil)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				refresh(mc.vidMapClient)
			}
		}()
		go func() {
			defer wg.Done()
			pushVolumeLocation(mc, "old:8080", nil, []uint32{5}, nil, nil)
		}()
		wg.Wait()

		refresh(mc.vidMapClient)
		requireLocations(t, mc.vidMapClient, 5, "live:8080")
	}
}

// A pushed add after the invalidation means the volume is being learned
// again: it replaces what was set aside, in both forms, as it did when the
// invalidation deleted the entries.
func TestPushedAddReplacesStaleLocations(t *testing.T) {
	mc := masterClientWith(&stubProvider{err: errNoAnswer})
	pushVolumeLocation(mc, "dead:8080", []uint32{5}, nil, nil, nil)
	pushVolumeLocation(mc, "live:8080", []uint32{5}, nil, nil, nil)
	pushVolumeLocation(mc, "shard:8080", nil, nil, []uint32{5}, nil)

	mc.InvalidateCache("5,0102")
	pushVolumeLocation(mc, "new:8080", []uint32{5}, nil, nil, nil)
	requireLocations(t, mc.vidMapClient, 5, "new:8080")

	if mc.vidMap.reviveStale(5) {
		t.Fatal("a stale entry outlived the volume being learned again")
	}
	requireLocations(t, mc.vidMapClient, 5, "new:8080")

	// The same once a failed refresh has brought the old locations back.
	refresh(mc.vidMapClient)
	requireLocations(t, mc.vidMapClient, 5, "new:8080")
	pushVolumeLocation(mc, "newer:8080", []uint32{5}, nil, nil, nil)
	requireLocations(t, mc.vidMapClient, 5, "newer:8080")
}

// A stale entry follows the map's own rules while it is set aside: it holds
// no server references, a removal edits it without touching another volume's
// references, a newer answer drops it revived or not, and a clear or enough
// resets end it like any other entry.
func TestStaleEntryLifecycle(t *testing.T) {
	a, b := Location{Url: "a:8080"}, Location{Url: "b:8080"}

	t.Run("server references", func(t *testing.T) {
		vm := newVidMap("", DefaultVidMapCacheSize)
		vm.addLocation(5, a)
		vm.addEcLocation(5, b)
		vm.addLocation(6, a)

		vm.markStale(5)
		if !vm.hasVolumeServer("a:8080") || vm.hasVolumeServer("b:8080") {
			t.Fatal("markStale must release volume 5's references and only those")
		}
		vm.deleteLocation(5, a)
		if !vm.hasVolumeServer("a:8080") {
			t.Fatal("a removal from a stale entry released volume 6's reference")
		}
		if !vm.reviveStale(5) {
			t.Fatal("nothing revived")
		}
		if locations, found := vm.GetLocations(5); !found || len(locations) != 1 || locations[0].Url != "b:8080" {
			t.Fatalf("revived %v, want the ec entry on b only", locations)
		}
		if !vm.hasVolumeServer("b:8080") {
			t.Fatal("reviveStale did not take the references back")
		}
		vm.deleteLocation(6, a)
		if vm.hasVolumeServer("a:8080") {
			t.Fatal("a:8080 still referenced after its last volume left")
		}
	})

	t.Run("superseded", func(t *testing.T) {
		vm := newVidMap("", DefaultVidMapCacheSize)
		vm.addLocation(5, a)
		vm.addLocation(6, b)
		vm.markStale(5)
		vm.reviveStale(5)
		asked := vm.learnedSoFar()
		vm.dropSetAside(5, asked)
		vm.dropSetAside(6, asked)
		if _, found := vm.GetLocations(5); found || vm.hasVolumeServer("a:8080") {
			t.Fatal("a revived entry or its server reference outlived a newer answer")
		}
		if _, found := vm.GetLocations(6); !found {
			t.Fatal("dropSetAside deleted an entry that was learned")
		}
	})

	t.Run("cleared", func(t *testing.T) {
		vm := newVidMap("", DefaultVidMapCacheSize)
		vm.addLocation(5, a)
		vm.markStale(5)
		vm.clear()
		if vm.reviveStale(5) {
			t.Fatal("revived an entry the clear dropped")
		}
	})

	t.Run("expired", func(t *testing.T) {
		vm := newVidMap("", 2)
		vm.addLocation(5, a)
		vm.markStale(5)
		for i := 0; i < 3; i++ {
			vm.reset()
		}
		if vm.reviveStale(5) {
			t.Fatal("revived an entry older than the retained generations")
		}
		if vm.hasVolumeServer("a:8080") {
			t.Fatal("expiring a stale entry left a server reference")
		}
	})
}

// The master answers a batch with an error for each volume it does not know,
// next to the locations of the volumes it does. That is an answer: a volume it
// does not know must not get what was set aside back, and a volume it does
// know is served from the answer.
func TestRelookupAnswerWithVolumeErrorsIsBelieved(t *testing.T) {
	provider := &stubProvider{answer: func(call int) (map[string][]Location, error) {
		return map[string][]Location{"6": {{Url: "new:8080"}}}, errors.New("master volume lookup errors: volume 5: volume id 5 not found")
	}}
	vc := newVidMapClient(provider, "", DefaultVidMapCacheSize)
	vc.addLocation(5, Location{Url: "gone:8080"})
	vc.addLocation(6, Location{Url: "old:8080"})
	vc.InvalidateCache("5,0102")
	vc.InvalidateCache("6,0102")

	locations, err := vc.LookupVolumeIdsWithFallback(context.Background(), []string{"5", "6"})
	if err == nil {
		t.Fatal("volume 5 is not known to the master, want an error")
	}
	if len(locations["5"]) != 0 || len(sortedUrls(locations["6"])) != 1 || locations["6"][0].Url != "new:8080" {
		t.Fatalf("locations %v, want volume 6 at new:8080 only", locations)
	}
	requireLocations(t, vc, 5)
	requireLocations(t, vc, 6, "new:8080")
}

// An answer to a lookup asked before the volume was learned again is older
// than what that learning set aside. When a failed lookup revives those newer
// locations, the older answer must not delete them.
func TestOlderAnswerKeepsLocationsLearnedMeanwhile(t *testing.T) {
	provider := &stubProvider{}
	vc := newVidMapClient(provider, "", DefaultVidMapCacheSize)
	provider.answer = func(call int) (map[string][]Location, error) {
		if call > 1 {
			return nil, errNoAnswer
		}
		// While this lookup is in flight, the master confirms new locations,
		// a read invalidates them, and the refresh that follows gets no
		// answer and revives them.
		vc.confirmLocations(5, []Location{{Url: "new:8080"}})
		vc.InvalidateCache("5,0102")
		if _, err := vc.LookupVolumeIdsWithFallback(context.Background(), []string{"5"}); err != nil {
			t.Errorf("overlapping lookup: %v", err)
		}
		return map[string][]Location{"5": {{Url: "old:8080"}}, "6": {{Url: "six:8080"}}}, nil
	}

	vc.LookupVolumeIdsWithFallback(context.Background(), []string{"5", "6"})

	requireLocations(t, vc, 5, "new:8080")
}
