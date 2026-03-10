//go:build linux && !no_iouring

package batchio

import (
	"fmt"
	"os"

	"github.com/iceber/iouring-go"
)

// ioUringBatchIO implements BatchIO using Linux io_uring.
// Requires kernel 5.6+ (linked fsync: 5.10+).
type ioUringBatchIO struct {
	ring     *iouring.IOURing
	ringSize int // max SQEs per submission
}

// NewIOUring creates a BatchIO backed by io_uring with the given ring size.
// Returns ErrIOUringUnavailable if io_uring cannot be initialized.
func NewIOUring(ringSize uint) (BatchIO, error) {
	ring, err := iouring.New(ringSize)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrIOUringUnavailable, err)
	}
	return &ioUringBatchIO{ring: ring, ringSize: int(ringSize)}, nil
}

func (u *ioUringBatchIO) PreadBatch(fd *os.File, ops []Op) error {
	if len(ops) == 0 {
		return nil
	}
	// Submit in chunks that fit the ring to avoid "too many requests" rejection.
	for start := 0; start < len(ops); start += u.ringSize {
		end := start + u.ringSize
		if end > len(ops) {
			end = len(ops)
		}
		if err := u.preadChunk(fd, ops[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (u *ioUringBatchIO) preadChunk(fd *os.File, ops []Op) error {
	requests := make([]iouring.PrepRequest, len(ops))
	// fd.Fd() is safe: the os.File stays live through this call.
	fdInt := int(fd.Fd())
	for i := range ops {
		requests[i] = iouring.Pread(fdInt, ops[i].Buf, uint64(ops[i].Offset))
	}

	resultSet, err := u.ring.SubmitRequests(requests, nil)
	if err != nil {
		return fmt.Errorf("iouring PreadBatch submit: %w", err)
	}

	// Wait for all completions before checking individual results.
	<-resultSet.Done()
	for i, req := range resultSet.Requests() {
		n, err := req.ReturnInt()
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
	// Submit in chunks that fit the ring to avoid "too many requests" rejection.
	for start := 0; start < len(ops); start += u.ringSize {
		end := start + u.ringSize
		if end > len(ops) {
			end = len(ops)
		}
		if err := u.pwriteChunk(fd, ops[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (u *ioUringBatchIO) pwriteChunk(fd *os.File, ops []Op) error {
	requests := make([]iouring.PrepRequest, len(ops))
	fdInt := int(fd.Fd())
	for i := range ops {
		requests[i] = iouring.Pwrite(fdInt, ops[i].Buf, uint64(ops[i].Offset))
	}

	resultSet, err := u.ring.SubmitRequests(requests, nil)
	if err != nil {
		return fmt.Errorf("iouring PwriteBatch submit: %w", err)
	}

	// Wait for all completions before checking individual results.
	<-resultSet.Done()
	for i, req := range resultSet.Requests() {
		n, err := req.ReturnInt()
		if err != nil {
			return fmt.Errorf("iouring PwriteBatch op[%d]: %w", i, err)
		}
		if n < len(ops[i].Buf) {
			return fmt.Errorf("iouring PwriteBatch op[%d]: short write %d/%d", i, n, len(ops[i].Buf))
		}
	}
	return nil
}

// Fsync issues fdatasync via io_uring. fdatasync flushes data to disk without
// updating file metadata (size, mtime). This is sufficient for pre-allocated
// extent files where metadata does not change during writes, and matches the
// standard path behavior (see standard_linux.go).
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

// LinkedWriteFsync submits a pwrite + fdatasync as a linked SQE chain.
// The kernel executes them in order in a single io_uring_enter() call.
// Requires kernel 5.10+ for linked fsync. Falls back to sequential on error.
func (u *ioUringBatchIO) LinkedWriteFsync(fd *os.File, buf []byte, offset int64) error {
	fdInt := int(fd.Fd())
	writeReq := iouring.Pwrite(fdInt, buf, uint64(offset))
	fsyncReq := iouring.Fdatasync(fdInt)

	// SubmitLinkRequests sets IOSQE_IO_LINK on all SQEs except the last,
	// ensuring the fdatasync executes only after the pwrite completes.
	resultSet, err := u.ring.SubmitLinkRequests(
		[]iouring.PrepRequest{writeReq, fsyncReq},
		nil,
	)
	if err != nil {
		// Fallback to sequential if linked submission fails.
		if _, werr := fd.WriteAt(buf, offset); werr != nil {
			return werr
		}
		return fdatasync(fd)
	}

	// Wait for all linked ops to complete, then check results.
	<-resultSet.Done()
	for i, req := range resultSet.Requests() {
		_, rerr := req.ReturnInt()
		if rerr != nil {
			return fmt.Errorf("iouring LinkedWriteFsync op[%d]: %w", i, rerr)
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
