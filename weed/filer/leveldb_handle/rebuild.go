package leveldb_handle

import (
	"encoding/binary"
	"io"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	leveldb_errors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/table"
)

const internalKeyTrailer = 8

// rebuildDamagedTables rewrites every table that has corrupted blocks
// without them, before leveldb.Recover runs. Recover would rebuild such a
// table itself, but it orders internal keys with the user comparer, so a
// table holding several versions of one key fails to rebuild, and the index
// keys and filter it writes do not match lookups. A table with nothing
// readable is left for Recover to drop, and so is every damaged table under
// StrictRecovery.
func rebuildDamagedTables(stor storage.Storage, o *opt.Options) error {
	if o.GetStrict(opt.StrictRecovery) {
		return nil
	}
	lock, err := stor.Lock()
	if err != nil {
		return err
	}
	defer lock.Unlock()
	fds, err := stor.List(storage.TypeTable)
	if err != nil {
		return err
	}
	to := &opt.Options{
		Comparer:    internalComparer{o.GetComparer()},
		Compression: o.GetCompression(),
		BlockSize:   o.GetBlockSize(),
		Strict:      opt.DefaultStrict &^ opt.StrictReader,
	}
	for _, fd := range fds {
		if err := rebuildTable(stor, fd, to); err != nil {
			return err
		}
	}
	return nil
}

func rebuildTable(stor storage.Storage, fd storage.FileDesc, o *opt.Options) error {
	r, err := stor.Open(fd)
	if err != nil {
		return err
	}
	readerOpen := true
	defer func() {
		if readerOpen {
			r.Close()
		}
	}()
	size, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	tr, err := table.NewReader(r, size, fd, nil, nil, o)
	if err != nil {
		return err
	}
	entries, damaged, err := scanTable(tr, nil)
	if err != nil || !damaged || entries == 0 {
		return err
	}

	tmp := storage.FileDesc{Type: storage.TypeTemp, Num: fd.Num}
	w, err := stor.Create(tmp)
	if err != nil {
		return err
	}
	tw := table.NewWriter(w, o)
	if _, _, err = scanTable(tr, tw); err == nil {
		err = tw.Close()
	}
	if err == nil {
		err = w.Sync()
	}
	if closeErr := w.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		readerOpen = false
		r.Close()
		err = stor.Rename(tmp, fd)
	}
	if err != nil {
		stor.Remove(tmp)
	}
	return err
}

// scanTable counts the readable entries of tr, appending them to tw if set,
// and reports whether a block was corrupted.
func scanTable(tr *table.Reader, tw *table.Writer) (entries int, damaged bool, err error) {
	iter := tr.NewIterator(nil, nil)
	defer iter.Release()
	if setter, ok := iter.(iterator.ErrorCallbackSetter); ok {
		setter.SetErrorCallback(func(err error) {
			if leveldb_errors.IsCorrupted(err) {
				damaged = true
			}
		})
	}
	for iter.Next() {
		if len(iter.Key()) < internalKeyTrailer {
			continue
		}
		entries++
		if tw != nil {
			if err := tw.Append(iter.Key(), iter.Value()); err != nil {
				return entries, damaged, err
			}
		}
	}
	if err := iter.Error(); err != nil {
		if !leveldb_errors.IsCorrupted(err) {
			return entries, damaged, err
		}
		damaged = true
	}
	return entries, damaged, nil
}

// internalComparer orders goleveldb internal keys, a user key followed by
// a little-endian sequence and kind, newest version first. It keeps full
// index keys instead of shortening them.
type internalComparer struct {
	comparer.Comparer
}

func (c internalComparer) Compare(a, b []byte) int {
	if r := c.Comparer.Compare(a[:len(a)-internalKeyTrailer], b[:len(b)-internalKeyTrailer]); r != 0 {
		return r
	}
	m := binary.LittleEndian.Uint64(a[len(a)-internalKeyTrailer:])
	n := binary.LittleEndian.Uint64(b[len(b)-internalKeyTrailer:])
	switch {
	case m > n:
		return -1
	case m < n:
		return 1
	}
	return 0
}

func (internalComparer) Separator(dst, a, b []byte) []byte { return nil }

func (internalComparer) Successor(dst, b []byte) []byte { return nil }
