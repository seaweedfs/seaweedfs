package main

import (
	"flag"
	"log"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	dir = flag.String("dir", ".", "data directory to store leveldb files")
)

func main() {

	flag.Parse()

	opts := &opt.Options{
		BlockCacheCapacity:            32 * 1024 * 1024, // default value is 8MiB
		WriteBuffer:                   16 * 1024 * 1024, // default value is 4MiB
		CompactionTableSizeMultiplier: 10,
		OpenFilesCacheCapacity:        -1,
	}

	db, err := leveldb.OpenFile(*dir, opts)
	if errors.IsCorrupted(err) {
		db, err = leveldb.RecoverFile(*dir, opts)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := db.CompactRange(util.Range{}); err != nil {
		log.Fatal(err)
	}
}
