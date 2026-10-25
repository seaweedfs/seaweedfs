package operation

import (
	"fmt"
	"testing"
	"time"
)

func TestCaching(t *testing.T) {
	var (
		vc VidCache
	)
	var locations []Location
	locations = append(locations, Location{Url: "a.com:8080"})
	vc.Set("123", locations, time.Second)
	ret, _ := vc.Get("123")
	if ret == nil {
		t.Fatal("Not found vid 123")
	}
	fmt.Printf("vid 123 locations = %v\n", ret)
	time.Sleep(2 * time.Second)
	ret, _ = vc.Get("123")
	if ret != nil {
		t.Fatal("Not found vid 123")
	}
}

// a stale under-replicated result must not linger for the full cache TTL
func TestCachingDelete(t *testing.T) {
	var vc VidCache
	locations := []Location{{Url: "a.com:8080"}}
	vc.Set("123", locations, time.Minute)
	if ret, _ := vc.Get("123"); ret == nil {
		t.Fatal("expected vid 123 to be cached")
	}
	vc.Delete("123")
	if ret, _ := vc.Get("123"); ret != nil {
		t.Fatal("expected vid 123 to be evicted after Delete")
	}
	// deleting a missing or out-of-range id must not panic
	vc.Delete("123")
	vc.Delete("4294967296")
}

// a single large volume id must not allocate an entry per id below it
func TestCachingLargeVolumeId(t *testing.T) {
	var vc VidCache
	locations := []Location{{Url: "a.com:8080"}}
	vc.Set("32000000", locations, time.Minute)
	if got := len(vc.cache); got != 1 {
		t.Fatalf("expected 1 cached entry, got %d", got)
	}
	if ret, _ := vc.Get("32000000"); ret == nil {
		t.Fatal("Not found vid 32000000")
	}

	// ids beyond uint32 are not real volume ids and must not wrap into the cache
	vc.Set("4294967296", locations, time.Minute)
	if got := len(vc.cache); got != 1 {
		t.Fatalf("out-of-range id must not be cached, got %d entries", got)
	}
	if ret, _ := vc.Get("4294967296"); ret != nil {
		t.Fatal("out-of-range vid 4294967296 should not be found")
	}
}
