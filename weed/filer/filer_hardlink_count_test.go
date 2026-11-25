package filer

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/require"
)

// A filer's store counts the names of a hard-linked file itself (countLink).
// These tests send it what clients send, counts worked out from an entry
// another client has changed since, and check the record against the names.

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
// the other way around. Two names are counted either way, and one when the
// second write never comes.
func TestFirstLinkCountsTheNamesThatAreThere(t *testing.T) {
	plain := func(store *stubFilerStore) {
		store.entries["/d/x"] = &Entry{FullPath: "/d/x", Attr: Attr{Mode: 0o644}, Chunks: []*filer_pb.FileChunk{{FileId: linkedChunk, Size: 4}}}
	}
	t.Run("old name first", func(t *testing.T) {
		store := newStubFilerStore()
		plain(store)
		wrapper := countingWrapper(store)
		ctx := context.Background()
		id := NewHardLinkId()

		require.NoError(t, wrapper.UpdateEntry(ctx, linkName(id, "/d/x", 2)))
		require.Equal(t, int32(1), linkCount(t, store, id))
		require.NoError(t, wrapper.InsertEntry(ctx, linkName(id, "/d/y", 2)))
		require.Equal(t, int32(2), linkCount(t, store, id))
		checkNames(t, wrapper, 2, "/d/x", "/d/y")
	})
	t.Run("new name first", func(t *testing.T) {
		store := newStubFilerStore()
		plain(store)
		wrapper := countingWrapper(store)
		ctx := context.Background()
		id := NewHardLinkId()

		require.NoError(t, wrapper.InsertEntryKnownAbsent(ctx, linkName(id, "/d/y", 2)))
		require.Equal(t, int32(1), linkCount(t, store, id))
		require.NoError(t, wrapper.UpdateEntry(ctx, linkName(id, "/d/x", 2)))
		require.Equal(t, int32(2), linkCount(t, store, id))
		checkNames(t, wrapper, 2, "/d/x", "/d/y")
	})
	t.Run("the new name is not made", func(t *testing.T) {
		store := newStubFilerStore()
		plain(store)
		wrapper := countingWrapper(store)
		ctx := context.Background()
		id := NewHardLinkId()

		require.NoError(t, wrapper.UpdateEntry(ctx, linkName(id, "/d/x", 2)))
		// the client takes the link id off the name again
		require.NoError(t, wrapper.UpdateEntry(ctx, &Entry{FullPath: "/d/x", Attr: Attr{Mode: 0o644}, Chunks: []*filer_pb.FileChunk{{FileId: linkedChunk, Size: 4}}}))
		require.Equal(t, int32(0), linkCount(t, store, id), "the record outlived the one name that had it")
	})
}

// A rename stores the file under the new name (WithMovedLink) and deletes the
// old one (OP "MV"): the name moves, link and all, and the count stays what
// it is at every step, so that the rename's event carries it.
func TestRenameOfALinkedNameKeepsTheCount(t *testing.T) {
	store := newStubFilerStore()
	id := linkedFile(t, store, "/d/x", "/d/y")
	wrapper := countingWrapper(store)
	ctx := context.Background()

	moved := linkName(id, "/e/z", 9)
	require.NoError(t, wrapper.InsertEntry(WithMovedLink(ctx), moved))
	require.Equal(t, int32(2), linkCount(t, store, id))
	require.Equal(t, int32(2), moved.HardLinkCounter, "the count the rename's event carries")
	require.NoError(t, wrapper.DeleteEntry(context.WithValue(ctx, "OP", "MV"), "/d/x"))
	require.Equal(t, int32(2), linkCount(t, store, id))
	checkNames(t, wrapper, 2, "/d/y", "/e/z")

	moved = linkName(id, "/e/w", 9)
	require.NoError(t, wrapper.InsertEntryKnownAbsent(WithMovedLink(ctx), moved))
	z, err := wrapper.FindEntry(ctx, "/e/z")
	require.NoError(t, err)
	require.NoError(t, wrapper.DeleteOneEntry(context.WithValue(ctx, "OP", "MV"), z))
	require.Equal(t, int32(2), linkCount(t, store, id))
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
// a file's only name, whose record goes with the delete and is made again.
func TestReplayedUpdateKeepsTheCount(t *testing.T) {
	for _, names := range [][]util.FullPath{{"/d/x"}, {"/d/x", "/d/y"}} {
		t.Run(fmt.Sprint(len(names)), func(t *testing.T) {
			store := newStubFilerStore()
			id := linkedFile(t, store, names...)
			wrapper := countingWrapper(store)
			ctx := context.Background()

			require.NoError(t, wrapper.DeleteEntry(ctx, "/d/x"))
			require.NoError(t, wrapper.InsertEntry(ctx, linkName(id, "/d/x", 7)))
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
	return context.WithValue(ctx, versionedTxnKey{}, &versionedTxn{start: s.clock, rows: map[string]*Entry{}, keys: map[string][]byte{}}), nil
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
	row, err := s.stubFilerStore.FindEntry(ctx, p)
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
// between the first one's read of the record and its write. The store refuses
// the first one's commit, it counts again from the record as it is now, and
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
			require.Equal(t, between.want, linkCount(t, store.stubFilerStore, id))
			checkNames(t, one, between.want, "/d/x", "/d/a")
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
