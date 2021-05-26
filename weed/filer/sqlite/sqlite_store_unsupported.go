// +build !linux,!darwin,!windows

// limited GOOS due to modernc.org/libc/unistd

package sqlite

func init() {
	// filer.Stores = append(filer.Stores, &SqliteStore{})
}
