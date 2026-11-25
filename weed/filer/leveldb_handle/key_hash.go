package leveldb_handle

import (
	"errors"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
)

var sha256KeyMarker = []byte("\x00seaweedfs.directory_key_hash.sha256")

// CheckKeyHash refuses databases whose directory keys were hashed differently.
// Under digest sha256 directory keys hash with SHA-256 instead of MD5, so an
// empty database opened that way is marked, and a database holding keys of
// the other kind is refused.
func CheckKeyHash(dbs []*DB, sha256Keys, readOnly bool) error {
	var unmarked []*DB
	for _, db := range dbs {
		marked, empty, err := keyHashState(db)
		if err != nil {
			return fmt.Errorf("%s: %w", db.name, err)
		}
		switch {
		case marked && !sha256Keys:
			return fmt.Errorf("%s was created with digest sha256 and can only be opened with it (-digest=sha256 or GODEBUG=fips140=only)", db.name)
		case !marked && sha256Keys && !empty:
			return fmt.Errorf("%s hashes directory keys with MD5, which digest sha256 does not; copy the metadata with fs.meta.save from a filer with -digest=md5 and fs.meta.load into a new store", db.name)
		case !marked && sha256Keys:
			unmarked = append(unmarked, db)
		}
	}
	if readOnly {
		return nil
	}
	for _, db := range unmarked {
		if err := db.Put(sha256KeyMarker, nil, nil); err != nil {
			return fmt.Errorf("%s: %w", db.name, err)
		}
	}
	return nil
}

func keyHashState(db *DB) (marked, empty bool, err error) {
	if _, err = db.Get(sha256KeyMarker, nil); err == nil {
		return true, false, nil
	}
	if !errors.Is(err, leveldb.ErrNotFound) {
		return false, false, err
	}
	iter := db.NewIterator(nil, nil)
	defer iter.Release()
	empty = !iter.First()
	return false, empty, iter.Error()
}
