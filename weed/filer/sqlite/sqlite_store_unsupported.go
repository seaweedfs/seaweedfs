// +build !linux,!darwin,!windows,!s390,!ppc64le,!mips64

// limited GOOS due to modernc.org/libc/unistd

package sqlite

func init() {
	// filer.Stores = append(filer.Stores, &SqliteStore{})
}
