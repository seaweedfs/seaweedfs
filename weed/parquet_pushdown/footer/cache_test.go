package footer

import (
	"errors"
	"sync"
	"testing"
)

func TestCache_NewSizeRejectsZero(t *testing.T) {
	if _, err := NewCache(0); err == nil {
		t.Error("size=0 should be rejected")
	}
	if _, err := NewCache(-1); err == nil {
		t.Error("negative size should be rejected")
	}
}

func TestCache_HitsAndMissesTracked(t *testing.T) {
	c, err := NewCache(8)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	id := Identity{Path: "a", SizeBytes: 1}
	pf := &ParsedFooter{NumRows: 10}

	if _, ok := c.Get(id); ok {
		t.Fatal("first Get should miss")
	}
	c.Add(id, pf)
	if got, ok := c.Get(id); !ok || got != pf {
		t.Fatalf("expected cached pointer, got=%v ok=%v", got, ok)
	}
	hits, misses := c.Stats()
	if hits != 1 || misses != 1 {
		t.Errorf("hits=%d misses=%d, want 1,1", hits, misses)
	}
}

// Two distinct identities must not alias. Same path, different size
// is still a different file from the cache's POV.
func TestCache_IdentityIsCompositeKey(t *testing.T) {
	c, err := NewCache(8)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	a := Identity{Path: "x", SizeBytes: 100, RecordCount: 1}
	b := Identity{Path: "x", SizeBytes: 200, RecordCount: 1}
	c.Add(a, &ParsedFooter{NumRows: 1})
	c.Add(b, &ParsedFooter{NumRows: 2})

	gotA, _ := c.Get(a)
	gotB, _ := c.Get(b)
	if gotA == gotB {
		t.Errorf("entries with different sizes must not alias")
	}
	if gotA.NumRows != 1 || gotB.NumRows != 2 {
		t.Errorf("entries swapped: gotA=%d gotB=%d", gotA.NumRows, gotB.NumRows)
	}
}

// Eviction is the LRU's job; the test just verifies the cache obeys
// its size cap and doesn't grow without bound.
func TestCache_EvictsAboveCap(t *testing.T) {
	c, err := NewCache(2)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	c.Add(Identity{Path: "a", SizeBytes: 1}, &ParsedFooter{NumRows: 1})
	c.Add(Identity{Path: "b", SizeBytes: 1}, &ParsedFooter{NumRows: 2})
	c.Add(Identity{Path: "c", SizeBytes: 1}, &ParsedFooter{NumRows: 3})

	if got := c.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2 after eviction", got)
	}
}

func TestCache_GetOrLoadCachesAndDedupes(t *testing.T) {
	c, err := NewCache(8)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	id := Identity{Path: "a", SizeBytes: 1}

	calls := 0
	load := func() (*ParsedFooter, error) {
		calls++
		return &ParsedFooter{NumRows: 42}, nil
	}

	first, err := c.GetOrLoad(id, load)
	if err != nil {
		t.Fatalf("first GetOrLoad: %v", err)
	}
	second, err := c.GetOrLoad(id, load)
	if err != nil {
		t.Fatalf("second GetOrLoad: %v", err)
	}
	if first != second {
		t.Errorf("second GetOrLoad returned different pointer")
	}
	if calls != 1 {
		t.Errorf("load called %d times, want 1", calls)
	}
}

func TestCache_GetOrLoadPropagatesError(t *testing.T) {
	c, err := NewCache(8)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	wantErr := errors.New("io boom")
	_, err = c.GetOrLoad(Identity{Path: "a"}, func() (*ParsedFooter, error) { return nil, wantErr })
	if !errors.Is(err, wantErr) {
		t.Errorf("err = %v, want %v", err, wantErr)
	}
	if c.Len() != 0 {
		t.Errorf("failed load should not insert; Len=%d", c.Len())
	}
}

// Parallel reads on the same key should all observe a hit after the
// first writer wins, and the cache must not race-detect.
func TestCache_ConcurrentSafe(t *testing.T) {
	c, err := NewCache(64)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	id := Identity{Path: "a", SizeBytes: 1}
	c.Add(id, &ParsedFooter{NumRows: 1})

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 256; k++ {
				_, _ = c.Get(id)
			}
		}()
	}
	wg.Wait()
}
