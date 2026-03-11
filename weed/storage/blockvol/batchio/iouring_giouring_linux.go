//go:build linux && iouring_giouring

package batchio

import (
	"fmt"
	"os"
	"unsafe"

	"github.com/pawelgaczynski/giouring"
)

func init() { IOUringImpl = "giouring" }

// giouringBatchIO implements BatchIO using giouring (direct liburing port).
// No goroutines or channels — direct SQE/CQE ring manipulation.
// Requires kernel 6.0+.
type giouringBatchIO struct {
	ring     *giouring.Ring
	ringSize int
}

// NewIOUring creates a BatchIO backed by giouring with the given ring size.
// Returns ErrIOUringUnavailable if io_uring cannot be initialized.
func NewIOUring(ringSize uint) (BatchIO, error) {
	ring, err := giouring.CreateRing(uint32(ringSize))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrIOUringUnavailable, err)
	}
	return &giouringBatchIO{ring: ring, ringSize: int(ringSize)}, nil
}

func (g *giouringBatchIO) PreadBatch(fd *os.File, ops []Op) error {
	if len(ops) == 0 {
		return nil
	}
	for start := 0; start < len(ops); start += g.ringSize {
		end := start + g.ringSize
		if end > len(ops) {
			end = len(ops)
		}
		if err := g.preadChunk(fd, ops[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (g *giouringBatchIO) preadChunk(fd *os.File, ops []Op) error {
	fdInt := int(fd.Fd())
	for i := range ops {
		sqe := g.ring.GetSQE()
		if sqe == nil {
			return fmt.Errorf("iouring PreadBatch: SQ full at op %d", i)
		}
		sqe.PrepareRead(fdInt, uintptr(unsafe.Pointer(&ops[i].Buf[0])), uint32(len(ops[i].Buf)), uint64(ops[i].Offset))
		sqe.SetData64(uint64(i))
	}

	_, err := g.ring.SubmitAndWait(uint32(len(ops)))
	if err != nil {
		return fmt.Errorf("iouring PreadBatch submit: %w", err)
	}

	for i := range ops {
		cqe, err := g.ring.WaitCQE()
		if err != nil {
			return fmt.Errorf("iouring PreadBatch wait op[%d]: %w", i, err)
		}
		if cqe.Res < 0 {
			g.ring.CQESeen(cqe)
			return fmt.Errorf("iouring PreadBatch op[%d]: %w", i, errFromRes(cqe.Res))
		}
		if int(cqe.Res) < len(ops[cqe.UserData].Buf) {
			g.ring.CQESeen(cqe)
			return fmt.Errorf("iouring PreadBatch op[%d]: short read %d/%d", cqe.UserData, cqe.Res, len(ops[cqe.UserData].Buf))
		}
		g.ring.CQESeen(cqe)
	}
	return nil
}

func (g *giouringBatchIO) PwriteBatch(fd *os.File, ops []Op) error {
	if len(ops) == 0 {
		return nil
	}
	for start := 0; start < len(ops); start += g.ringSize {
		end := start + g.ringSize
		if end > len(ops) {
			end = len(ops)
		}
		if err := g.pwriteChunk(fd, ops[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (g *giouringBatchIO) pwriteChunk(fd *os.File, ops []Op) error {
	fdInt := int(fd.Fd())
	for i := range ops {
		sqe := g.ring.GetSQE()
		if sqe == nil {
			return fmt.Errorf("iouring PwriteBatch: SQ full at op %d", i)
		}
		sqe.PrepareWrite(fdInt, uintptr(unsafe.Pointer(&ops[i].Buf[0])), uint32(len(ops[i].Buf)), uint64(ops[i].Offset))
		sqe.SetData64(uint64(i))
	}

	_, err := g.ring.SubmitAndWait(uint32(len(ops)))
	if err != nil {
		return fmt.Errorf("iouring PwriteBatch submit: %w", err)
	}

	for i := range ops {
		cqe, err := g.ring.WaitCQE()
		if err != nil {
			return fmt.Errorf("iouring PwriteBatch wait op[%d]: %w", i, err)
		}
		if cqe.Res < 0 {
			g.ring.CQESeen(cqe)
			return fmt.Errorf("iouring PwriteBatch op[%d]: %w", cqe.UserData, errFromRes(cqe.Res))
		}
		if int(cqe.Res) < len(ops[cqe.UserData].Buf) {
			g.ring.CQESeen(cqe)
			return fmt.Errorf("iouring PwriteBatch op[%d]: short write %d/%d", cqe.UserData, cqe.Res, len(ops[cqe.UserData].Buf))
		}
		g.ring.CQESeen(cqe)
	}
	return nil
}

// Fsync issues fdatasync via io_uring.
func (g *giouringBatchIO) Fsync(fd *os.File) error {
	sqe := g.ring.GetSQE()
	if sqe == nil {
		return fmt.Errorf("iouring Fsync: SQ full")
	}
	sqe.PrepareFsync(int(fd.Fd()), giouring.FsyncDatasync)

	_, err := g.ring.SubmitAndWait(1)
	if err != nil {
		return fmt.Errorf("iouring Fsync submit: %w", err)
	}

	cqe, err := g.ring.WaitCQE()
	if err != nil {
		return fmt.Errorf("iouring Fsync wait: %w", err)
	}
	defer g.ring.CQESeen(cqe)
	if cqe.Res < 0 {
		return fmt.Errorf("iouring Fsync: %w", errFromRes(cqe.Res))
	}
	return nil
}

// LinkedWriteFsync submits pwrite + fdatasync as a linked SQE chain.
func (g *giouringBatchIO) LinkedWriteFsync(fd *os.File, buf []byte, offset int64) error {
	fdInt := int(fd.Fd())

	// SQE 1: pwrite with IO_LINK flag
	sqe1 := g.ring.GetSQE()
	if sqe1 == nil {
		return fmt.Errorf("iouring LinkedWriteFsync: SQ full (write)")
	}
	sqe1.PrepareWrite(fdInt, uintptr(unsafe.Pointer(&buf[0])), uint32(len(buf)), uint64(offset))
	sqe1.SetFlags(uint32(giouring.SqeIOLink))

	// SQE 2: fdatasync (no link flag — last in chain)
	sqe2 := g.ring.GetSQE()
	if sqe2 == nil {
		return fmt.Errorf("iouring LinkedWriteFsync: SQ full (fsync)")
	}
	sqe2.PrepareFsync(fdInt, giouring.FsyncDatasync)

	_, err := g.ring.SubmitAndWait(2)
	if err != nil {
		// Fallback to sequential.
		if _, werr := fd.WriteAt(buf, offset); werr != nil {
			return werr
		}
		return fdatasync(fd)
	}

	// Collect both CQEs.
	for i := 0; i < 2; i++ {
		cqe, err := g.ring.WaitCQE()
		if err != nil {
			return fmt.Errorf("iouring LinkedWriteFsync wait op[%d]: %w", i, err)
		}
		if cqe.Res < 0 {
			g.ring.CQESeen(cqe)
			return fmt.Errorf("iouring LinkedWriteFsync op[%d]: %w", i, errFromRes(cqe.Res))
		}
		g.ring.CQESeen(cqe)
	}
	return nil
}

func (g *giouringBatchIO) Close() error {
	if g.ring != nil {
		g.ring.QueueExit()
	}
	return nil
}

// errFromRes converts a negative io_uring result code to a Go error.
func errFromRes(res int32) error {
	return fmt.Errorf("errno %d", -res)
}
