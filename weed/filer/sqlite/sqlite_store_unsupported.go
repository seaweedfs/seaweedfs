// +build !linux,!darwin,!windows,!s390,!ppc64le

// limited GOOS due to modernc.org/libc/unistd

package sqlite

func init() {
	// filer.Stores = append(filer.Stores, &SqliteStore{})
}
