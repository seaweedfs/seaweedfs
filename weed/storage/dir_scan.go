package storage

import (
	"errors"
	"io"
	"os"
)

// dirScanBatch bounds how many entries a directory walk holds at once. A disk
// holding millions of volumes has a file per volume for each of .dat, .idx and
// .vif, and os.ReadDir builds — and sorts — a slice of all of them before the
// caller sees the first entry. Every startup scan then costs hundreds of MB of
// peak heap that the runtime is slow to hand back, which is most of the gap
// between a volume server's live heap and its resident set.
const dirScanBatch = 1024

// eachDirEntry calls visit for every entry in dir, in whatever order the
// filesystem returns them, and stops early once visit returns false. Callers
// that need a defined order sort the few entries they keep.
func eachDirEntry(dir string, visit func(entry os.DirEntry) bool) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	for {
		entries, readErr := f.ReadDir(dirScanBatch)
		for _, entry := range entries {
			if !visit(entry) {
				return nil
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		}
	}
}
