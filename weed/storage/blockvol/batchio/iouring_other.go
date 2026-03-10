//go:build !linux || no_iouring

package batchio

// NewIOUring returns ErrIOUringUnavailable on non-Linux platforms
// or when io_uring is disabled via the no_iouring build tag.
func NewIOUring(ringSize uint) (BatchIO, error) {
	return nil, ErrIOUringUnavailable
}
