//go:build !linux && !darwin
// +build !linux,!darwin

package backend

// Using default sync operation
func (df *DiskFile) Sync() error {
	return df.File.Sync()
}
