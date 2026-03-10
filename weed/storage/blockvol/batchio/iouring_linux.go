//go:build linux && !no_iouring

package batchio

import (
	"fmt"
	"os"
	"syscall"

	"github.com/iceber/iouring-go"
)

// ioUringBatchIO implements BatchIO using Linux io_uring.
// Requires kernel 5.6+ (linked fsync: 5.10+).
type ioUringBatchIO struct {
	ring *iouring.IOURing
}

// NewIOUring creates a BatchIO backed by io_uring with the given ring size.
// If io_uring is unavailable (kernel too old, seccomp, etc.), returns
// NewStandard() with a nil error — silent fallback.
func NewIOUring(ringSize uint) (BatchIO, error) {
	ring, err := iouring.New(ringSize)
	if err != nil {
		// Kernel doesn't support io_uring — fall back silently.
		return NewStandard(), nil
	}
	return &ioUringBatchIO{ring: ring}, nil
}

func (u *ioUringBatchIO) PreadBatch(fd *os.File, ops []Op) error {
	if len(ops) == 0 {
		return nil
	}

	requests := make([]iouring.PrepRequest, len(ops))
	for i := range ops {
		requests[i] = iouring.Pread(int(fd.Fd()), ops[i].Buf, uint64(ops[i].Offset))
	}

	results, err := u.ring.SubmitRequests(requests, nil)
	if err != nil {
		return fmt.Errorf("iouring PreadBatch submit: %w", err)
	}

	// Wait for all completions and check results.
	for i, res := range results {
		<-res.Done()
		n, err := res.ReturnInt()
		if err != nil {
			return fmt.Errorf("iouring PreadBatch op[%d]: %w", i, err)
		}
		if n < len(ops[i].Buf) {
			return fmt.Errorf("iouring PreadBatch op[%d]: short read %d/%d", i, n, len(ops[i].Buf))
		}
	}
	return nil
}

func (u *ioUringBatchIO) PwriteBatch(fd *os.File, ops []Op) error {
	if len(ops) == 0 {
		return nil
	}

	requests := make([]iouring.PrepRequest, len(ops))
	for i := range ops {
		requests[i] = iouring.Pwrite(int(fd.Fd()), ops[i].Buf, uint64(ops[i].Offset))
	}

	results, err := u.ring.SubmitRequests(requests, nil)
	if err != nil {
		return fmt.Errorf("iouring PwriteBatch submit: %w", err)
	}

	for i, res := range results {
		<-res.Done()
		n, err := res.ReturnInt()
		if err != nil {
			return fmt.Errorf("iouring PwriteBatch op[%d]: %w", i, err)
		}
		if n < len(ops[i].Buf) {
			return fmt.Errorf("iouring PwriteBatch op[%d]: short write %d/%d", i, n, len(ops[i].Buf))
		}
	}
	return nil
}

func (u *ioUringBatchIO) Fsync(fd *os.File) error {
	req := iouring.Fdatasync(int(fd.Fd()))
	result, err := u.ring.SubmitRequest(req, nil)
	if err != nil {
		return fmt.Errorf("iouring Fsync submit: %w", err)
	}
	<-result.Done()
	_, err = result.ReturnInt()
	if err != nil {
		return fmt.Errorf("iouring Fsync: %w", err)
	}
	return nil
}

func (u *ioUringBatchIO) LinkedWriteFsync(fd *os.File, buf []byte, offset int64) error {
	// Try linked SQE chain: pwrite → fdatasync (one io_uring_enter).
	// This requires IOSQE_IO_LINK support (kernel 5.3+) and linked fsync (5.10+).
	// If the ring doesn't support it, fall back to sequential.
	writeReq := iouring.Pwrite(int(fd.Fd()), buf, uint64(offset))
	fsyncReq := iouring.Fdatasync(int(fd.Fd()))

	// SubmitRequests with linked flag.
	results, err := u.ring.SubmitRequests(
		[]iouring.PrepRequest{writeReq, fsyncReq},
		nil,
	)
	if err != nil {
		// Fallback to sequential if linking not supported.
		if errno, ok := err.(syscall.Errno); ok && errno == syscall.EINVAL {
			if _, werr := fd.WriteAt(buf, offset); werr != nil {
				return werr
			}
			return fd.Sync()
		}
		return fmt.Errorf("iouring LinkedWriteFsync submit: %w", err)
	}

	for i, res := range results {
		<-res.Done()
		_, err := res.ReturnInt()
		if err != nil {
			return fmt.Errorf("iouring LinkedWriteFsync op[%d]: %w", i, err)
		}
	}
	return nil
}

func (u *ioUringBatchIO) Close() error {
	if u.ring != nil {
		return u.ring.Close()
	}
	return nil
}
