package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	dir     = flag.String("dir", "./t", "directory to store level db files")
	useHash = flag.Bool("isHash", false, "hash the path as the key")
	dbCount = flag.Int("dbCount", 1, "the number of leveldb")
)

func main() {

	flag.Parse()

	totalTenants := 300
	totalYears := 3

	opts := &opt.Options{
		BlockCacheCapacity:            32 * 1024 * 1024, // default value is 8MiB
		WriteBuffer:                   16 * 1024 * 1024, // default value is 4MiB
		CompactionTableSizeMultiplier: 4,
	}

	var dbs []*leveldb.DB
	var chans []chan string
	for d := 0; d < *dbCount; d++ {
		dbFolder := fmt.Sprintf("%s/%02d", *dir, d)
		os.MkdirAll(dbFolder, 0755)
		db, err := leveldb.OpenFile(dbFolder, opts)
		if err != nil {
			log.Printf("filer store open dir %s: %v", *dir, err)
			return
		}
		dbs = append(dbs, db)
		chans = append(chans, make(chan string, 1024))
	}

	var wg sync.WaitGroup
	for d := 0; d < *dbCount; d++ {
		wg.Add(1)
		go func(d int) {
			defer wg.Done()

			ch := chans[d]
			db := dbs[d]

			for p := range ch {
				if *useHash {
					insertAsHash(db, p)
				} else {
					insertAsFullPath(db, p)
				}
			}
		}(d)
	}

	counter := int64(0)
	lastResetTime := time.Now()

	r := rand.New(rand.NewSource(35))

	for y := 0; y < totalYears; y++ {
		for m := 0; m < 12; m++ {
			for d := 0; d < 31; d++ {
				for h := 0; h < 24; h++ {
					for min := 0; min < 60; min++ {
						for i := 0; i < totalTenants; i++ {
							p := fmt.Sprintf("tenent%03d/%4d/%02d/%02d/%02d/%02d", i, 2015+y, 1+m, 1+d, h, min)

							x := r.Intn(*dbCount)

							chans[x] <- p

							counter++
						}

						t := time.Now()
						if lastResetTime.Add(time.Second).Before(t) {
							p := fmt.Sprintf("%4d/%02d/%02d/%02d/%02d", 2015+y, 1+m, 1+d, h, min)
							fmt.Printf("%s = %4d put/sec\n", p, counter)
							counter = 0
							lastResetTime = t
						}
					}
				}
			}
		}
	}

	for d := 0; d < *dbCount; d++ {
		close(chans[d])
	}

	wg.Wait()

}

func insertAsFullPath(db *leveldb.DB, p string) {
	_, getErr := db.Get([]byte(p), nil)
	if getErr == leveldb.ErrNotFound {
		putErr := db.Put([]byte(p), []byte(p), nil)
		if putErr != nil {
			log.Printf("failed to put %s", p)
		}
	}
}

func insertAsHash(db *leveldb.DB, p string) {
	key := fmt.Sprintf("%d:%s", hashToLong(p), p)
	_, getErr := db.Get([]byte(key), nil)
	if getErr == leveldb.ErrNotFound {
		putErr := db.Put([]byte(key), []byte(p), nil)
		if putErr != nil {
			log.Printf("failed to put %s", p)
		}
	}
}

func hashToLong(dir string) (v int64) {
	h := md5.New()
	io.WriteString(h, dir)

	b := h.Sum(nil)

	v += int64(b[0])
	v <<= 8
	v += int64(b[1])
	v <<= 8
	v += int64(b[2])
	v <<= 8
	v += int64(b[3])
	v <<= 8
	v += int64(b[4])
	v <<= 8
	v += int64(b[5])
	v <<= 8
	v += int64(b[6])
	v <<= 8
	v += int64(b[7])

	return
}
