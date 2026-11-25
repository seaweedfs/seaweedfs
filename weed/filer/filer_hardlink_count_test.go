package filer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/require"
)

// A filer's own store counts the names of a hard-linked file itself and lets
// no write leave the file's link count below them (linkNames). These tests
// send it what clients send, counts worked out from an entry another client
// has changed since, and check the record against the names.

// countingWrapper is the wrapper Filer.SetStore puts around a filer's store.
func countingWrapper(store FilerStore) *FilerStoreWrapper {
	wrapper := NewFilerStoreWrapper(store)
	wrapper.countsLinks = true
	return wrapper
}

// linkName is the row a client sends for name: the file linkedFile made, with
// the count the client believes in.
func linkName(id HardLinkId, name util.FullPath, count int32) *Entry {
	return &Entry{
		FullPath:        name,
		Attr:            Attr{Mode: 0o644},
		HardLinkId:      id,
		HardLinkCounter: count,
		Chunks:          []*filer_pb.FileChunk{{FileId: linkedChunk, Size: 4}},
	}
}

// namesCounted is the number of names the store has counted for id, -1 when
// it has counted none.
func namesCounted(t *testing.T, store *stubFilerStore, id HardLinkId) int32 {
	t.Helper()
	value, err := store.KvGet(context.Background(), linkNamesKey(id))
	if errors.Is(err, ErrKvNotFound) {
		return -1
	}
	require.NoError(t, err)
	return int32(util.BytesToUint32(value))
}

func checkNames(t *testing.T, wrapper *FilerStoreWrapper, want int32, names ...util.FullPath) {
	t.Helper()
	for _, name := range names {
		e, err := wrapper.FindEntry(context.Background(), name)
		require.NoError(t, err, "%s", name)
		require.Equal(t, want, e.HardLinkCounter, "%s", name)
		require.Len(t, e.GetChunks(), 1, "%s", name)
	}
}

// Two mounts each add a name to a file with two. Each read the count as 2 and
// sends 3, with its update of the name it linked from and with the new name.
// Four names are counted, in whatever order the four writes arrive. Counted
// as 3, deleting three of them deleted the record and the chunks under the
// fourth.
func TestTwoLinksFromOneCountAreBothCounted(t *testing.T) {
	for _, order := range [][]int{{0, 1, 2, 3}, {0, 2, 1, 3}, {1, 3, 0, 2}, {2, 3, 0, 1}} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			store := newStubFilerStore()
			id := linkedFile(t, store, "/d/x", "/d/y")
			wrapper := countingWrapper(store)
			ctx := context.Background()
			writes := []func() error{
				func() error { return wrapper.UpdateEntry(ctx, linkName(id, "/d/x", 3)) },
				func() error { return wrapper.UpdateEntry(ctx, linkName(id, "/d/y", 3)) },
				func() error { return wrapper.InsertEntry(ctx, linkName(id, "/d/a", 3)) },
				func() error { return wrapper.InsertEntry(ctx, linkName(id, "/d/b", 3)) },
			}
			for _, i := range order {
				require.NoError(t, writes[i]())
			}
			require.Equal(t, int32(4), linkCount(t, store, id))
			checkNames(t, wrapper, 4, "/d/x", "/d/y", "/d/a", "/d/b")

			for _, name := range []util.FullPath{"/d/x", "/d/y", "/d/a"} {
				require.NoError(t, wrapper.DeleteEntry(ctx, name))
			}
			require.Equal(t, int32(1), linkCount(t, store, id), "the record went before the file's last name")
			checkNames(t, wrapper, 1, "/d/b")
		})
	}
}

// A write through one name carries the count its client had before another
// added a name: a mode change, or a flush of the file's content, which a mount
// sends as a create over the name. The count stays.
func TestWriteWithAnOldCountKeepsTheCount(t *testing.T) {
	for _, write := range []struct {
		name string
		run  func(*FilerStoreWrapper, context.Context, *Entry) error
	}{
		{"UpdateEntry", (*FilerStoreWrapper).UpdateEntry},
		{"InsertEntry", (*FilerStoreWrapper).InsertEntry},
	} {
		t.Run(write.name, func(t *testing.T) {
			store := newStubFilerStore()
			id := linkedFile(t, store, "/d/x", "/d/y", "/d/a")
			wrapper := countingWrapper(store)
			ctx := context.Background()

			old := linkName(id, "/d/y", 2)
			old.Attr.Mode = 0o600
			require.NoError(t, write.run(wrapper, ctx, old))
			require.Equal(t, int32(3), linkCount(t, store, id))
			require.Equal(t, int32(3), old.HardLinkCounter, "the entry, and the event made from it, carry the count")
			checkNames(t, wrapper, 3, "/d/x", "/d/y", "/d/a")
			y, err := wrapper.FindEntry(ctx, "/d/y")
			require.NoError(t, err)
			require.EqualValues(t, 0o600, y.Attr.Mode, "the write itself is stored")
		})
	}
}

// The first link of a file: the client gives the file's one name a link id
// and sends the count with the new name in it already, then the new name. Or
// the other way around. The record has the client's count from the first
// write on, so that the file is not taken for one with a single name while
// the second is on its way, and the filer counts the names as they come.
func TestFirstLinkCountsTheNamesAsTheyCome(t *testing.T) {
	chunk := []*filer_pb.FileChunk{{FileId: linkedChunk, Size: 4}}
	plain := func(store *stubFilerStore) {
		store.entries["/d/x"] = &Entry{FullPath: "/d/x", Attr: Attr{Mode: 0o644}, Chunks: chunk}
	}
	t.Run("old name first", func(t *testing.T) {
		store := newStubFilerStore()
		plain(store)
		wrapper := countingWrapper(store)
		ctx := context.Background()
		id := NewHardLinkId()

		require.NoError(t, wrapper.UpdateEntry(ctx, linkName(id, "/d/x", 2)))
		require.Equal(t, int32(2), linkCount(t, store, id))
		require.Equal(t, int32(1), namesCounted(t, store, id))
		require.NoError(t, wrapper.InsertEntry(ctx, linkName(id, "/d/y", 2)))
		require.Equal(t, int32(2), linkCount(t, store, id))
		require.Equal(t, int32(2), namesCounted(t, store, id))
		checkNames(t, wrapper, 2, "/d/x", "/d/y")
	})
	t.Run("new name first", func(t *testing.T) {
		store := newStubFilerStore()
		plain(store)
		wrapper := countingWrapper(store)
		ctx := context.Background()
		id := NewHardLinkId()

		require.NoError(t, wrapper.InsertEntryKnownAbsent(ctx, linkName(id, "/d/y", 2)))
		require.Equal(t, int32(1), namesCounted(t, store, id))
		require.NoError(t, wrapper.UpdateEntry(ctx, linkName(id, "/d/x", 2)))
		require.Equal(t, int32(2), linkCount(t, store, id))
		require.Equal(t, int32(2), namesCounted(t, store, id))
		checkNames(t, wrapper, 2, "/d/x", "/d/y")
	})
	// Another client deletes the source between the two writes of the link.
	// The count the source's update left has the coming name in it: the
	// delete is not the file's last (lastLinkDropped goes by the count it
	// read), the chunks stay, and the new name reads them.
	t.Run("the source goes before the new name comes", func(t *testing.T) {
		store := newStubFilerStore()
		plain(store)
		wrapper := countingWrapper(store)
		ctx := context.Background()
		id := NewHardLinkId()

		require.NoError(t, wrapper.UpdateEntry(ctx, linkName(id, "/d/x", 2)))
		x, err := wrapper.FindEntry(ctx, "/d/x")
		require.NoError(t, err)
		require.Equal(t, int32(2), x.HardLinkCounter, "the delete would take the source for the file's last name")
		require.NoError(t, wrapper.DeleteEntry(ctx, "/d/x"))
		require.Equal(t, int32(1), linkCount(t, store, id))
		require.Equal(t, int32(0), namesCounted(t, store, id))

		require.NoError(t, wrapper.InsertEntry(ctx, linkName(id, "/d/y", 2)))
		require.Equal(t, int32(1), namesCounted(t, store, id))
		y, err := wrapper.FindEntry(ctx, "/d/y")
		require.NoError(t, err)
		require.Len(t, y.GetChunks(), 1)
	})
	t.Run("the new name is not made", func(t *testing.T) {
		store := newStubFilerStore()
		plain(store)
		wrapper := countingWrapper(store)
		ctx := context.Background()
		id := NewHardLinkId()

		require.NoError(t, wrapper.UpdateEntry(ctx, linkName(id, "/d/x", 2)))
		// the client takes the link id off the name again
		require.NoError(t, wrapper.UpdateEntry(ctx, &Entry{FullPath: "/d/x", Attr: Attr{Mode: 0o644}, Chunks: chunk}))
		x, err := wrapper.FindEntry(ctx, "/d/x")
		require.NoError(t, err)
		require.Empty(t, x.HardLinkId)
		require.Equal(t, int32(0), namesCounted(t, store, id), "a name was left counted on a link no name holds")
	})
}

// A write the store refuses, sent again: the name is counted once it is
// stored, and once. Counted with the refused attempt as well, every retry
// left the count one higher, and the record and the chunks outlived the
// file's last name.
func TestRefusedWriteOfANameIsCountedOnceWhenSentAgain(t *testing.T) {
	for _, counted := range []bool{false, true} {
		t.Run(fmt.Sprintf("names counted before=%v", counted), func(t *testing.T) {
			store := newStubFilerStore()
			id := linkedFile(t, store, "/d/x", "/d/y")
			wrapper := countingWrapper(store)
			ctx := context.Background()
			if counted {
				require.NoError(t, wrapper.UpdateEntry(ctx, linkName(id, "/d/x", 2)))
				require.Equal(t, int32(2), namesCounted(t, store, id))
			}

			store.insertErr = storeRefusal
			for i := 0; i < 3; i++ {
				require.ErrorIs(t, wrapper.InsertEntry(ctx, linkName(id, "/d/a", 3)), storeRefusal)
			}
			require.Equal(t, int32(2), namesCounted(t, store, id), "a name that was not stored was counted")
			store.insertErr = nil
			require.NoError(t, wrapper.InsertEntry(ctx, linkName(id, "/d/a", 3)))
			require.Equal(t, int32(3), linkCount(t, store, id))
			require.Equal(t, int32(3), namesCounted(t, store, id))

			for _, name := range []util.FullPath{"/d/x", "/d/y", "/d/a"} {
				require.NoError(t, wrapper.DeleteEntry(ctx, name))
			}
			require.Equal(t, int32(0), linkCount(t, store, id), "the record outlived the file's last name")
			require.Equal(t, int32(-1), namesCounted(t, store, id))
		})
	}
}

// namesPutFailingStore refuses the puts of the names key that refuse picks,
// by their number.
type namesPutFailingStore struct {
	*stubFilerStore
	id     HardLinkId
	puts   int
	refuse func(put int) bool
}

func (s *namesPutFailingStore) KvPut(ctx context.Context, key []byte, value []byte) error {
	if bytes.Equal(key, linkNamesKey(s.id)) {
		if s.puts++; s.refuse != nil && s.refuse(s.puts) {
			return storeRefusal
		}
	}
	return s.stubFilerStore.KvPut(ctx, key, value)
}

// A store without transactions may fail between a name and the names
// recorded for it. They are recorded first: a name that cannot be counted is
// not stored, and sent again it is counted once. Recorded after the name, the
// failure left the name stored and not counted, and the write sent again
// found it there and did not count it either.
func TestNameThatCannotBeCountedIsNotStored(t *testing.T) {
	for _, where := range []string{"default store", "path-specific store"} {
		for _, known := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, known absent=%v", where, known), func(t *testing.T) {
				inner := newStubFilerStore()
				id := linkedFile(t, inner, "/d/x", "/d/y")
				store := &namesPutFailingStore{stubFilerStore: inner, id: id}
				wrapper := countingWrapper(store)
				names, name := inner, util.FullPath("/d/a")
				if where == "path-specific store" {
					names, name = newStubFilerStore(), "/s/a"
					wrapper.AddPathSpecificStore("/s/", "s", names)
				}
				insert := wrapper.InsertEntry
				if known {
					insert = wrapper.InsertEntryKnownAbsent
				}
				ctx := context.Background()
				require.NoError(t, wrapper.UpdateEntry(ctx, linkName(id, "/d/x", 2)))
				require.Equal(t, int32(2), namesCounted(t, inner, id))

				store.refuse = func(int) bool { return true }
				require.ErrorIs(t, insert(ctx, linkName(id, name, 3)), storeRefusal)
				require.Empty(t, names.entries[pathInStore(where, name)], "a name that could not be counted was stored")
				store.refuse = nil
				require.NoError(t, insert(ctx, linkName(id, name, 3)))
				require.Equal(t, int32(3), namesCounted(t, inner, id))

				for _, n := range []util.FullPath{"/d/x", "/d/y", name} {
					require.NoError(t, wrapper.DeleteEntry(ctx, n))
				}
				require.Equal(t, int32(0), linkCount(t, inner, id), "the record outlived the file's last name")
			})
		}
	}
}

// pathInStore is the path a store holds name under: a path-specific store has
// its location cut off.
func pathInStore(where string, name util.FullPath) string {
	if where == "path-specific store" {
		return "/" + name.Name()
	}
	return string(name)
}

// A name that was counted and then refused by the store is counted off again.
// Where that fails as well, the names recorded are above the names stored and
// the file is leaked. They are never below, which would delete it under a
// name.
func TestNameRefusedAfterItWasCountedLeavesTheNamesNoLower(t *testing.T) {
	for _, where := range []string{"default store", "path-specific store"} {
		t.Run(where, func(t *testing.T) {
			inner := newStubFilerStore()
			id := linkedFile(t, inner, "/d/x", "/d/y")
			store := &namesPutFailingStore{stubFilerStore: inner, id: id}
			wrapper := countingWrapper(store)
			names, name := inner, util.FullPath("/d/a")
			if where == "path-specific store" {
				names, name = newStubFilerStore(), "/s/a"
				wrapper.AddPathSpecificStore("/s/", "s", names)
			}
			ctx := context.Background()
			require.NoError(t, wrapper.UpdateEntry(ctx, linkName(id, "/d/x", 2)))
			require.Equal(t, int32(2), namesCounted(t, inner, id))

			// the name is counted, refused, and cannot be counted off
			names.insertErr = storeRefusal
			counted := store.puts
			store.refuse = func(put int) bool { return put > counted+1 }
			require.ErrorIs(t, wrapper.InsertEntry(ctx, linkName(id, name, 3)), storeRefusal)
			require.Equal(t, int32(3), namesCounted(t, inner, id))

			names.insertErr, store.refuse = nil, nil
			require.NoError(t, wrapper.InsertEntry(ctx, linkName(id, name, 3)))
			for _, n := range []util.FullPath{"/d/x", "/d/y", name} {
				require.NoError(t, wrapper.DeleteEntry(ctx, n))
			}
			require.Equal(t, int32(1), linkCount(t, inner, id), "leaked, by the one name counted twice")
		})
	}
}

// A rename stores the file under the new name (WithMovedLink) and deletes the
// old one (OP "MV"): the name moves, link and all, and neither the count nor
// the names counted change at any step.
func TestRenameOfALinkedNameKeepsTheCount(t *testing.T) {
	store := newStubFilerStore()
	id := linkedFile(t, store, "/d/x", "/d/y")
	wrapper := countingWrapper(store)
	ctx := context.Background()

	require.NoError(t, wrapper.InsertEntry(WithMovedLink(ctx), linkName(id, "/e/z", 2)))
	require.Equal(t, int32(2), linkCount(t, store, id))
	require.Equal(t, int32(2), namesCounted(t, store, id))
	require.NoError(t, wrapper.DeleteEntry(context.WithValue(ctx, "OP", "MV"), "/d/x"))
	require.Equal(t, int32(2), linkCount(t, store, id))
	require.Equal(t, int32(2), namesCounted(t, store, id))
	checkNames(t, wrapper, 2, "/d/y", "/e/z")

	require.NoError(t, wrapper.InsertEntryKnownAbsent(WithMovedLink(ctx), linkName(id, "/e/w", 2)))
	z, err := wrapper.FindEntry(ctx, "/e/z")
	require.NoError(t, err)
	require.NoError(t, wrapper.DeleteOneEntry(context.WithValue(ctx, "OP", "MV"), z))
	require.Equal(t, int32(2), linkCount(t, store, id))
	require.Equal(t, int32(2), namesCounted(t, store, id))
	checkNames(t, wrapper, 2, "/d/y", "/e/w")

	// a renamed file written over a name of another file takes that name off the other
	other := linkedFile(t, store, "/d/p", "/d/q")
	require.NoError(t, wrapper.InsertEntry(WithMovedLink(ctx), linkName(id, "/d/q", 2)))
	require.NoError(t, wrapper.DeleteEntry(context.WithValue(ctx, "OP", "MV"), "/e/w"))
	require.Equal(t, int32(2), linkCount(t, store, id))
	require.Equal(t, int32(1), linkCount(t, store, other))
}

// A peer's update of a name is replayed as a delete of the row and an insert
// of the new one (Filer.Replay). The count comes out as it went in, also for
// a file's only name, whose record goes with the delete and is made again,
// and also when the event carries a count from before another name was added.
func TestReplayedUpdateKeepsTheCount(t *testing.T) {
	for _, names := range [][]util.FullPath{{"/d/x"}, {"/d/x", "/d/y"}, {"/d/x", "/d/y", "/d/z"}} {
		t.Run(fmt.Sprint(len(names)), func(t *testing.T) {
			store := newStubFilerStore()
			id := linkedFile(t, store, names...)
			wrapper := countingWrapper(store)
			ctx := context.Background()

			sent := int32(len(names))
			if sent > 2 {
				sent = 2 // from before the third name
			}
			require.NoError(t, wrapper.DeleteEntry(ctx, "/d/x"))
			require.NoError(t, wrapper.InsertEntry(ctx, linkName(id, "/d/x", sent)))
			require.Equal(t, int32(len(names)), linkCount(t, store, id))
			checkNames(t, wrapper, int32(len(names)), names...)
		})
	}
}

// One file's name written over a name of another: the first gains a name, the
// second loses one.
func TestNameWrittenOverAnotherFilesNameCountsBoth(t *testing.T) {
	store := newStubFilerStore()
	f := linkedFile(t, store, "/d/f1", "/d/f2")
	g := linkedFile(t, store, "/d/g1", "/d/g2")
	wrapper := countingWrapper(store)

	require.NoError(t, wrapper.InsertEntry(context.Background(), linkName(g, "/d/f2", 2)))
	require.Equal(t, int32(3), linkCount(t, store, g))
	require.Equal(t, int32(1), linkCount(t, store, f))
}

// A name whose record is lost says how many names its file had. Nothing else
// does, so that count is kept: too high leaks the chunks, too low deletes them
// under whatever other names are left.
func TestNameWithoutARecordKeepsItsCount(t *testing.T) {
	store := newStubFilerStore()
	id := linkedFile(t, store, "/d/x", "/d/y", "/d/z")
	delete(store.kv, string(id))
	wrapper := countingWrapper(store)

	require.NoError(t, wrapper.UpdateEntry(context.Background(), linkName(id, "/d/x", 3)))
	require.Equal(t, int32(3), linkCount(t, store, id))

	// and so does one a rename moves
	delete(store.kv, string(id))
	require.NoError(t, wrapper.InsertEntry(WithMovedLink(context.Background()), linkName(id, "/e/y", 3)))
	require.Equal(t, int32(3), linkCount(t, store, id))
}

// A store that only copies some of a filer's entries (a mount's metadata
// cache) does not see every name: it stores the count the filer sent.
func TestCopyOfTheEntriesTakesTheCountItIsGiven(t *testing.T) {
	store := newStubFilerStore()
	id := linkedFile(t, store, "/d/x")
	wrapper := NewFilerStoreWrapper(store)

	require.NoError(t, wrapper.UpdateEntry(context.Background(), linkName(id, "/d/x", 5)))
	require.Equal(t, int32(5), linkCount(t, store, id))
	require.NoError(t, wrapper.InsertEntry(context.Background(), linkName(id, "/d/y", 6)))
	require.Equal(t, int32(6), linkCount(t, store, id))
}

// Names added and removed at once on one filer: every one is counted.
func TestConcurrentLinksAndUnlinksAreAllCounted(t *testing.T) {
	store := newStubFilerStore()
	id := linkedFile(t, store, "/d/x", "/d/y")
	wrapper := countingWrapper(store)
	ctx := context.Background()
	const n = 40
	for i := 0; i < n; i++ {
		require.NoError(t, wrapper.InsertEntry(ctx, linkName(id, util.FullPath(fmt.Sprintf("/d/old%d", i)), 2)))
	}
	require.Equal(t, int32(n+2), linkCount(t, store, id))

	var wg sync.WaitGroup
	errs := make(chan error, 2*n)
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			errs <- wrapper.InsertEntry(ctx, linkName(id, util.FullPath(fmt.Sprintf("/d/new%d", i)), 2))
		}(i)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				errs <- wrapper.DeleteEntry(ctx, util.FullPath(fmt.Sprintf("/d/old%d", i)))
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int32(2+n+n/2), linkCount(t, store, id))
}

// versionedStore is a store two filers share whose commits are refused when a
// row or a key the transaction wrote was committed by another since it began,
// as TiKV refuses them.
type versionedStore struct {
	*stubFilerStore
	vmu       sync.Mutex
	clock     int64
	committed map[string]int64
	// afterRead runs after a transaction read a row ("r/d/x") or a key ("k"
	// and the key), outside every lock
	afterRead func(what string)
}

type versionedTxn struct {
	store *versionedStore
	start int64
	rows  map[string]*Entry // nil: deleted
	keys  map[string][]byte // nil: deleted
}

type versionedTxnKey struct{}

func newVersionedStore() *versionedStore {
	return &versionedStore{stubFilerStore: newStubFilerStore(), committed: map[string]int64{}}
}

func (s *versionedStore) RefusesConflictingCommits() bool { return true }

func (s *versionedStore) txn(ctx context.Context) *versionedTxn {
	txn, _ := ctx.Value(versionedTxnKey{}).(*versionedTxn)
	return txn
}

// wrote marks what a write outside a transaction committed.
func (s *versionedStore) wrote(what string) {
	s.vmu.Lock()
	defer s.vmu.Unlock()
	s.clock++
	s.committed[what] = s.clock
}

func (s *versionedStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	s.vmu.Lock()
	defer s.vmu.Unlock()
	return context.WithValue(ctx, versionedTxnKey{}, &versionedTxn{store: s, start: s.clock, rows: map[string]*Entry{}, keys: map[string][]byte{}}), nil
}

func (s *versionedStore) CommitTransaction(ctx context.Context) error {
	txn := s.txn(ctx)
	s.vmu.Lock()
	defer s.vmu.Unlock()
	for p := range txn.rows {
		if s.committed["r"+p] > txn.start {
			return fmt.Errorf("row %s: %w", p, ErrTransactionConflict)
		}
	}
	for key := range txn.keys {
		if s.committed["k"+key] > txn.start {
			return fmt.Errorf("key %x: %w", key, ErrTransactionConflict)
		}
	}
	s.clock++
	bare := context.Background()
	for p, row := range txn.rows {
		if row == nil {
			s.stubFilerStore.DeleteEntry(bare, util.FullPath(p))
		} else {
			s.stubFilerStore.InsertEntry(bare, row)
		}
		s.committed["r"+p] = s.clock
	}
	for key, value := range txn.keys {
		if value == nil {
			s.stubFilerStore.KvDelete(bare, []byte(key))
		} else {
			s.stubFilerStore.KvPut(bare, []byte(key), value)
		}
		s.committed["k"+key] = s.clock
	}
	return nil
}

func (s *versionedStore) RollbackTransaction(context.Context) error { return nil }

func (s *versionedStore) FindEntry(ctx context.Context, p util.FullPath) (*Entry, error) {
	txn := s.txn(ctx)
	if txn != nil {
		if row, found := txn.rows[string(p)]; found {
			if row == nil {
				return nil, filer_pb.ErrNotFound
			}
			return row.ShallowClone(), nil
		}
	}
	from := s
	if txn != nil {
		// reads go through the transaction's store, as TiKV's do
		from = txn.store
	}
	row, err := from.stubFilerStore.FindEntry(ctx, p)
	if txn != nil && s.afterRead != nil {
		s.afterRead("r" + string(p))
	}
	return row, err
}

func (s *versionedStore) InsertEntry(ctx context.Context, entry *Entry) error {
	if txn := s.txn(ctx); txn != nil {
		txn.rows[string(entry.FullPath)] = entry
		return nil
	}
	s.wrote("r" + string(entry.FullPath))
	return s.stubFilerStore.InsertEntry(ctx, entry)
}

func (s *versionedStore) UpdateEntry(ctx context.Context, entry *Entry) error {
	return s.InsertEntry(ctx, entry)
}

func (s *versionedStore) DeleteEntry(ctx context.Context, p util.FullPath) error {
	if txn := s.txn(ctx); txn != nil {
		txn.rows[string(p)] = nil
		return nil
	}
	s.wrote("r" + string(p))
	return s.stubFilerStore.DeleteEntry(ctx, p)
}

func (s *versionedStore) KvGet(ctx context.Context, key []byte) ([]byte, error) {
	txn := s.txn(ctx)
	if txn != nil {
		if value, found := txn.keys[string(key)]; found {
			if value == nil {
				return nil, ErrKvNotFound
			}
			return value, nil
		}
	}
	value, err := s.stubFilerStore.KvGet(ctx, key)
	if txn != nil && s.afterRead != nil {
		s.afterRead("k" + string(key))
	}
	return value, err
}

func (s *versionedStore) KvPut(ctx context.Context, key []byte, value []byte) error {
	if txn := s.txn(ctx); txn != nil {
		txn.keys[string(key)] = append([]byte{}, value...)
		return nil
	}
	s.wrote("k" + string(key))
	return s.stubFilerStore.KvPut(ctx, key, value)
}

func (s *versionedStore) KvDelete(ctx context.Context, key []byte) error {
	if txn := s.txn(ctx); txn != nil {
		txn.keys[string(key)] = nil
		return nil
	}
	s.wrote("k" + string(key))
	return s.stubFilerStore.KvDelete(ctx, key)
}

// once runs f the first time what is read in a transaction.
func (s *versionedStore) once(t *testing.T, what string, f func() error) (ran *bool) {
	ran = new(bool)
	s.afterRead = func(read string) {
		if read == what && !*ran {
			*ran = true
			require.NoError(t, f())
		}
	}
	return ran
}

// Two filers on one store each add a name to the same file, the second
// between the first one's read of the names counted and its write. The store
// refuses the first one's commit, it counts again from what is there now, and
// both names are counted. The same for a name taken off in between.
func TestLinkCountedAgainAfterAnotherFilerChangedTheRecord(t *testing.T) {
	for _, between := range []struct {
		name string
		run  func(*FilerStoreWrapper, HardLinkId) error
		want int32
	}{
		{"link", func(other *FilerStoreWrapper, id HardLinkId) error {
			return other.InsertEntry(context.Background(), linkName(id, "/d/b", 3))
		}, 4},
		{"unlink", func(other *FilerStoreWrapper, id HardLinkId) error {
			return other.DeleteEntry(context.Background(), "/d/y")
		}, 2},
	} {
		t.Run(between.name, func(t *testing.T) {
			store := newVersionedStore()
			id := linkedFile(t, store.stubFilerStore, "/d/x", "/d/y")
			one, other := countingWrapper(store), countingWrapper(store)

			raced := store.once(t, "k"+string(id), func() error { return between.run(other, id) })
			require.NoError(t, one.InsertEntry(context.Background(), linkName(id, "/d/a", 3)))
			require.True(t, *raced)
			require.Equal(t, between.want, namesCounted(t, store.stubFilerStore, id))
			// the count is the client's 3 where that is more than the names: too high only leaks
			count := between.want
			if count < 3 {
				count = 3
			}
			require.Equal(t, count, linkCount(t, store.stubFilerStore, id))
		})
	}
}

// A mount flushes a file through one of its names, a create over the name,
// while another filer deletes that name: its row goes after this filer looked
// the name up and found it one of the file's, and its link comes off after
// this filer's write (dropLink). The write stores the name again. The store
// refuses the commit over the delete of the row, and run again the write
// finds the name gone and counts it. Kept at the old count, the name was one
// more than the count once the delete took its link off.
func TestNameDeletedByAnotherFilerIsCountedWhenStoredAgain(t *testing.T) {
	store := newVersionedStore()
	id := linkedFile(t, store.stubFilerStore, "/d/x", "/d/y")
	one, other := countingWrapper(store), countingWrapper(store)
	ctx := context.Background()

	raced := store.once(t, "r/d/y", func() error { return store.DeleteEntry(ctx, "/d/y") })
	require.NoError(t, one.InsertEntry(ctx, linkName(id, "/d/y", 2)))
	require.True(t, *raced)
	require.NoError(t, other.DeleteHardLink(ctx, id))
	require.Equal(t, int32(2), linkCount(t, store.stubFilerStore, id))
	checkNames(t, one, 2, "/d/x", "/d/y")
}

// A name in a path-specific store is not in the record's transaction. The
// change of the record that is refused and run again must not find the name
// its first run stored: it is stored after the count is settled, and counted
// once.
func TestNameInAPathSpecificStoreIsCountedOnce(t *testing.T) {
	store := newVersionedStore()
	id := linkedFile(t, store.stubFilerStore, "/d/x", "/d/y")
	elsewhere := newStubFilerStore()
	one, other := countingWrapper(store), countingWrapper(store)
	one.AddPathSpecificStore("/s/", "s", elsewhere)
	other.AddPathSpecificStore("/s/", "s", elsewhere)

	raced := store.once(t, "k"+string(id), func() error {
		return other.InsertEntry(context.Background(), linkName(id, "/d/b", 3))
	})
	require.NoError(t, one.InsertEntry(context.Background(), linkName(id, "/s/a", 3)))
	require.True(t, *raced)
	require.Equal(t, int32(4), linkCount(t, store.stubFilerStore, id))
	checkNames(t, one, 4, "/d/x", "/d/y", "/d/b", "/s/a")

	require.NoError(t, one.InsertEntryKnownAbsent(context.Background(), linkName(id, "/s/c", 3)))
	require.Equal(t, int32(5), linkCount(t, store.stubFilerStore, id))
}

// insertHookStore runs onInsert before it stores an entry.
type insertHookStore struct {
	*stubFilerStore
	onInsert func(*Entry)
	inserts  atomic.Int32
}

func (s *insertHookStore) InsertEntry(ctx context.Context, entry *Entry) error {
	if s.onInsert != nil {
		s.onInsert(entry)
	}
	defer s.inserts.Add(1)
	return s.stubFilerStore.InsertEntry(ctx, entry)
}

// Two creates of one new name in a path-specific store on this filer: the
// second waits for the first one's name, finds it, and does not count it
// again. Counted twice, the file outlived its last name.
func TestNameInAPathSpecificStoreCreatedTwiceIsCountedOnce(t *testing.T) {
	store := newStubFilerStore()
	id := linkedFile(t, store, "/d/x", "/d/y")
	elsewhere := &insertHookStore{stubFilerStore: newStubFilerStore()}
	one := countingWrapper(store)
	one.AddPathSpecificStore("/s/", "s", elsewhere)
	ctx := context.Background()

	var second sync.Once
	elsewhere.onInsert = func(*Entry) {
		second.Do(func() {
			done := make(chan error, 1)
			go func() { done <- one.InsertEntry(ctx, linkName(id, "/s/a", 3)) }()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
	require.NoError(t, one.InsertEntry(ctx, linkName(id, "/s/a", 3)))
	require.Eventually(t, func() bool { return elsewhere.inserts.Load() == 2 }, time.Second, 10*time.Millisecond)
	checkNames(t, one, 3, "/d/x", "/d/y", "/s/a")

	for _, name := range []util.FullPath{"/d/x", "/d/y", "/s/a"} {
		require.NoError(t, one.DeleteEntry(ctx, name))
	}
	require.Equal(t, int32(0), linkCount(t, store, id))
}

// A name in a path-specific store of the same kind is looked up there, not
// through the default store's transaction: written again, it keeps the count.
func TestNameInAPathSpecificStoreOfTheSameKindIsFoundThere(t *testing.T) {
	store := newVersionedStore()
	id := linkedFile(t, store.stubFilerStore, "/d/x", "/d/y")
	elsewhere := newVersionedStore()
	one := countingWrapper(store)
	one.AddPathSpecificStore("/s/", "s", elsewhere)
	ctx := context.Background()

	require.NoError(t, one.InsertEntry(ctx, linkName(id, "/s/a", 3)))
	require.Equal(t, int32(3), linkCount(t, store.stubFilerStore, id))
	require.NoError(t, one.UpdateEntry(ctx, linkName(id, "/s/a", 3)))
	require.Equal(t, int32(3), linkCount(t, store.stubFilerStore, id))
}

// Inside a transaction the caller opened, the record is changed in that
// transaction: its commit is the caller's to run again.
func TestLinkCountInsideACallersTransactionOpensNone(t *testing.T) {
	store := newVersionedStore()
	id := linkedFile(t, store.stubFilerStore, "/d/x", "/d/y")
	f := &Filer{Store: countingWrapper(store)}

	ctx, err := f.BeginTransaction(context.Background())
	require.NoError(t, err)
	require.NoError(t, f.Store.InsertEntry(ctx, linkName(id, "/d/a", 3)))
	require.Equal(t, int32(2), linkCount(t, store.stubFilerStore, id), "written before the caller committed")
	require.NoError(t, f.CommitTransaction(ctx))
	require.Equal(t, int32(3), linkCount(t, store.stubFilerStore, id))
}
