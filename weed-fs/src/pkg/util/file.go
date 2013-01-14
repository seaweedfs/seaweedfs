package util

import (
	"errors"
	"log"
	"os"
)

// sets file (fh if not nil, otherwise fileName) permission to mask
// it will
//   AND with the permission iff direction < 0
//   OR with the permission iff direction > 0
//   otherwise it will SET the permission to the mask
func SetFilePerm(fh *os.File, fileName string, mask os.FileMode, direction int8) (err error) {
	var stat os.FileInfo
	if fh == nil {
		stat, err = os.Stat(fileName)
	} else {
		stat, err = fh.Stat()
	}
	if err != nil {
		return err
	}

	mode := stat.Mode() & ^os.ModePerm
	// log.Printf("mode1=%d mask=%d", mode, mask)
	if direction == 0 {
		mode |= mask
	} else if direction > 0 {
		mode |= stat.Mode().Perm() | mask
	} else {
		mode |= stat.Mode().Perm() & mask
	}
	log.Printf("pmode=%d operm=%d => nmode=%d nperm=%d",
		stat.Mode(), stat.Mode()&os.ModePerm,
		mode, mode&os.ModePerm)
	if mode == 0 {
		return errors.New("Zero FileMode")
	}
	if fh == nil {
		err = os.Chmod(fileName, mode)
	} else {
		err = fh.Chmod(mode)
	}
	return err
}

// returns whether  the filename exists - errors doesn't mean not exists!
func FileExists(fileName string) bool {
	if _, e := os.Stat(fileName); e != nil && os.IsNotExist(e) {
		return false
	}
	return true
}

// returns whether the filename is POSSIBLY writable
//- whether it has some kind of writable bit set
func FileIsWritable(fileName string) bool {
	if stat, e := os.Stat(fileName); e == nil {
		return stat.Mode().Perm()&0222 > 0
	}
	return false
}
