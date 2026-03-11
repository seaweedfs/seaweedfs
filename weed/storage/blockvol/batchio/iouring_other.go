//go:build !linux || (!iouring_iceber && !iouring_giouring && !iouring_raw)

package batchio

// NewIOUring returns ErrIOUringUnavailable when no io_uring backend is
// compiled in. On Linux, use one of these build tags to enable a backend:
//   - iouring_iceber   (iceber/iouring-go library)
//   - iouring_giouring (pawelgaczynski/giouring library)
//   - iouring_raw      (raw syscall wrappers, zero deps)
func NewIOUring(ringSize uint) (BatchIO, error) {
	return nil, ErrIOUringUnavailable
}
