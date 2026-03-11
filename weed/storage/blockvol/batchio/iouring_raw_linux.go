//go:build linux && !no_iouring && !iouring_iceber && !iouring_giouring

package batchio

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

// Raw io_uring syscall numbers.
const (
	sysIOUringSetup    = 425
	sysIOUringEnter    = 426
	sysIOUringRegister = 427
)

// io_uring opcodes.
const (
	opNop      = 0
	opReadv    = 1
	opWritev   = 2
	opFsync    = 3
	opRead     = 22
	opWrite    = 23
)

// io_uring SQE flags.
const (
	sqeFlagIOLink = 1 << 2
)

// io_uring fsync flags.
const (
	fsyncDatasync = 1 << 0
)

// io_uring_enter flags.
const (
	enterGetEvents = 1 << 0
)

// io_uring setup offsets (from kernel include/uapi/linux/io_uring.h).
const (
	offSQDropped  = 0
	offSQFlags    = 4
	offSQArrayOff = 8
)

// sqe is the submission queue entry (64 bytes).
type sqe struct {
	opcode     uint8
	flags      uint8
	ioprio     uint16
	fd         int32
	off        uint64
	addr       uint64
	len        uint32
	opcFlags   uint32
	userData   uint64
	bufIG      uint16
	personality uint16
	spliceFdIn int32
	addr3      uint64
	_pad       [8]byte
}

// cqe is the completion queue entry (16 bytes).
type cqe struct {
	userData uint64
	res      int32
	flags    uint32
}

// ioUringParams is passed to io_uring_setup.
type ioUringParams struct {
	sqEntries    uint32
	cqEntries    uint32
	flags        uint32
	sqThreadCPU  uint32
	sqThreadIdle uint32
	features     uint32
	wqFd         uint32
	resv         [3]uint32
	sqOff        sqRingOffsets
	cqOff        cqRingOffsets
}

type sqRingOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	flags       uint32
	dropped     uint32
	array       uint32
	resv1       uint32
	userAddr    uint64
}

type cqRingOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	overflow    uint32
	cqes        uint32
	flags       uint32
	resv1       uint32
	userAddr    uint64
}

// rawRing is a minimal io_uring ring for batch I/O.
type rawRing struct {
	fd       int
	ringSize int

	// SQ ring mapped memory
	sqRingPtr uintptr
	sqRingLen int
	sqHead    *uint32
	sqTail    *uint32
	sqMask    uint32
	sqArray   *uint32 // sqArray[0] through sqArray[entries-1]

	// SQE array
	sqePtr uintptr
	sqeLen int
	sqes   *sqe // base of SQE array

	// CQ ring mapped memory
	cqRingPtr uintptr
	cqRingLen int
	cqHead    *uint32
	cqTail    *uint32
	cqMask    uint32
	cqes      *cqe // base of CQE array

	mu sync.Mutex // serializes submit+wait cycles
}

// rawBatchIO implements BatchIO using raw io_uring syscalls.
// No external dependencies. ~200 LOC of direct kernel interaction.
type rawBatchIO struct {
	ring     *rawRing
	ringSize int
}

// NewIOUring creates a BatchIO backed by raw io_uring syscalls.
// Returns ErrIOUringUnavailable if io_uring cannot be initialized.
func NewIOUring(ringSize uint) (BatchIO, error) {
	ring, err := newRawRing(int(ringSize))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrIOUringUnavailable, err)
	}
	return &rawBatchIO{ring: ring, ringSize: int(ringSize)}, nil
}

func newRawRing(entries int) (*rawRing, error) {
	var params ioUringParams
	fd, _, errno := syscall.Syscall(sysIOUringSetup, uintptr(entries), uintptr(unsafe.Pointer(&params)), 0)
	if errno != 0 {
		return nil, fmt.Errorf("io_uring_setup: %v", errno)
	}

	r := &rawRing{
		fd:       int(fd),
		ringSize: int(params.sqEntries),
	}

	// Map SQ ring.
	sqRingSize := int(params.sqOff.array + params.sqEntries*4)
	sqRingPtr, _, errno := syscall.Syscall6(syscall.SYS_MMAP, 0, uintptr(sqRingSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE,
		fd, 0)
	if errno != 0 {
		syscall.Close(r.fd)
		return nil, fmt.Errorf("mmap sq ring: %v", errno)
	}
	r.sqRingPtr = sqRingPtr
	r.sqRingLen = sqRingSize
	r.sqHead = (*uint32)(unsafe.Pointer(sqRingPtr + uintptr(params.sqOff.head)))
	r.sqTail = (*uint32)(unsafe.Pointer(sqRingPtr + uintptr(params.sqOff.tail)))
	r.sqMask = *(*uint32)(unsafe.Pointer(sqRingPtr + uintptr(params.sqOff.ringMask)))
	r.sqArray = (*uint32)(unsafe.Pointer(sqRingPtr + uintptr(params.sqOff.array)))

	// Map SQE array.
	sqeSize := int(params.sqEntries) * int(unsafe.Sizeof(sqe{}))
	sqePtr, _, errno := syscall.Syscall6(syscall.SYS_MMAP, 0, uintptr(sqeSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE,
		fd, 0x10000000) // IORING_OFF_SQES
	if errno != 0 {
		syscall.Munmap(unsafeSlice(sqRingPtr, sqRingSize))
		syscall.Close(r.fd)
		return nil, fmt.Errorf("mmap sqes: %v", errno)
	}
	r.sqePtr = sqePtr
	r.sqeLen = sqeSize
	r.sqes = (*sqe)(unsafe.Pointer(sqePtr))

	// Map CQ ring.
	cqRingSize := int(params.cqOff.cqes + params.cqEntries*uint32(unsafe.Sizeof(cqe{})))
	cqRingPtr, _, errno := syscall.Syscall6(syscall.SYS_MMAP, 0, uintptr(cqRingSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE,
		fd, 0x8000000) // IORING_OFF_CQ_RING
	if errno != 0 {
		syscall.Munmap(unsafeSlice(sqePtr, sqeSize))
		syscall.Munmap(unsafeSlice(sqRingPtr, sqRingSize))
		syscall.Close(r.fd)
		return nil, fmt.Errorf("mmap cq ring: %v", errno)
	}
	r.cqRingPtr = cqRingPtr
	r.cqRingLen = cqRingSize
	r.cqHead = (*uint32)(unsafe.Pointer(cqRingPtr + uintptr(params.cqOff.head)))
	r.cqTail = (*uint32)(unsafe.Pointer(cqRingPtr + uintptr(params.cqOff.tail)))
	r.cqMask = *(*uint32)(unsafe.Pointer(cqRingPtr + uintptr(params.cqOff.ringMask)))
	r.cqes = (*cqe)(unsafe.Pointer(cqRingPtr + uintptr(params.cqOff.cqes)))

	return r, nil
}

func (r *rawRing) close() {
	syscall.Munmap(unsafeSlice(r.cqRingPtr, r.cqRingLen))
	syscall.Munmap(unsafeSlice(r.sqePtr, r.sqeLen))
	syscall.Munmap(unsafeSlice(r.sqRingPtr, r.sqRingLen))
	syscall.Close(r.fd)
}

// getSQE returns a pointer to the next SQE slot, or nil if full.
func (r *rawRing) getSQE(idx int) *sqe {
	return (*sqe)(unsafe.Pointer(r.sqePtr + uintptr(idx)*unsafe.Sizeof(sqe{})))
}

// sqArraySlot returns a pointer to sqArray[idx].
func (r *rawRing) sqArraySlot(idx int) *uint32 {
	return (*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(r.sqArray)) + uintptr(idx)*4))
}

// getCQE returns a pointer to cqes[idx].
func (r *rawRing) getCQE(idx uint32) *cqe {
	return (*cqe)(unsafe.Pointer(uintptr(unsafe.Pointer(r.cqes)) + uintptr(idx)*unsafe.Sizeof(cqe{})))
}

// submitAndWait submits n SQEs and waits for n CQEs. Returns CQE results.
func (r *rawRing) submitAndWait(n int) ([]cqe, error) {
	// Set SQ array indices and advance tail.
	tail := *r.sqTail
	for i := 0; i < n; i++ {
		*r.sqArraySlot(int(tail+uint32(i)) & int(r.sqMask)) = (tail + uint32(i)) & r.sqMask
	}
	// Memory barrier: ensure SQE writes are visible before updating tail.
	*r.sqTail = tail + uint32(n)

	// io_uring_enter: submit and wait.
	_, _, errno := syscall.Syscall6(sysIOUringEnter, uintptr(r.fd),
		uintptr(n), uintptr(n), enterGetEvents, 0, 0)
	if errno != 0 {
		return nil, fmt.Errorf("io_uring_enter: %v", errno)
	}

	// Read CQEs.
	results := make([]cqe, n)
	head := *r.cqHead
	for i := 0; i < n; i++ {
		c := r.getCQE(head & r.cqMask)
		results[i] = *c
		head++
	}
	*r.cqHead = head

	return results, nil
}

func (b *rawBatchIO) PreadBatch(fd *os.File, ops []Op) error {
	if len(ops) == 0 {
		return nil
	}
	b.ring.mu.Lock()
	defer b.ring.mu.Unlock()

	for start := 0; start < len(ops); start += b.ringSize {
		end := start + b.ringSize
		if end > len(ops) {
			end = len(ops)
		}
		if err := b.preadChunk(fd, ops[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (b *rawBatchIO) preadChunk(fd *os.File, ops []Op) error {
	fdInt := int(fd.Fd())
	for i := range ops {
		s := b.ring.getSQE(i)
		*s = sqe{} // zero
		s.opcode = opRead
		s.fd = int32(fdInt)
		s.addr = uint64(uintptr(unsafe.Pointer(&ops[i].Buf[0])))
		s.len = uint32(len(ops[i].Buf))
		s.off = uint64(ops[i].Offset)
		s.userData = uint64(i)
	}

	results, err := b.ring.submitAndWait(len(ops))
	if err != nil {
		return fmt.Errorf("iouring PreadBatch: %w", err)
	}

	for _, r := range results {
		if r.res < 0 {
			return fmt.Errorf("iouring PreadBatch op[%d]: errno %d", r.userData, -r.res)
		}
		idx := r.userData
		if int(r.res) < len(ops[idx].Buf) {
			return fmt.Errorf("iouring PreadBatch op[%d]: short read %d/%d", idx, r.res, len(ops[idx].Buf))
		}
	}
	return nil
}

func (b *rawBatchIO) PwriteBatch(fd *os.File, ops []Op) error {
	if len(ops) == 0 {
		return nil
	}
	b.ring.mu.Lock()
	defer b.ring.mu.Unlock()

	for start := 0; start < len(ops); start += b.ringSize {
		end := start + b.ringSize
		if end > len(ops) {
			end = len(ops)
		}
		if err := b.pwriteChunk(fd, ops[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (b *rawBatchIO) pwriteChunk(fd *os.File, ops []Op) error {
	fdInt := int(fd.Fd())
	for i := range ops {
		s := b.ring.getSQE(i)
		*s = sqe{}
		s.opcode = opWrite
		s.fd = int32(fdInt)
		s.addr = uint64(uintptr(unsafe.Pointer(&ops[i].Buf[0])))
		s.len = uint32(len(ops[i].Buf))
		s.off = uint64(ops[i].Offset)
		s.userData = uint64(i)
	}

	results, err := b.ring.submitAndWait(len(ops))
	if err != nil {
		return fmt.Errorf("iouring PwriteBatch: %w", err)
	}

	for _, r := range results {
		if r.res < 0 {
			return fmt.Errorf("iouring PwriteBatch op[%d]: errno %d", r.userData, -r.res)
		}
		idx := r.userData
		if int(r.res) < len(ops[idx].Buf) {
			return fmt.Errorf("iouring PwriteBatch op[%d]: short write %d/%d", idx, r.res, len(ops[idx].Buf))
		}
	}
	return nil
}

func (b *rawBatchIO) Fsync(fd *os.File) error {
	b.ring.mu.Lock()
	defer b.ring.mu.Unlock()

	s := b.ring.getSQE(0)
	*s = sqe{}
	s.opcode = opFsync
	s.fd = int32(fd.Fd())
	s.opcFlags = fsyncDatasync

	results, err := b.ring.submitAndWait(1)
	if err != nil {
		return fmt.Errorf("iouring Fsync: %w", err)
	}
	if results[0].res < 0 {
		return fmt.Errorf("iouring Fsync: errno %d", -results[0].res)
	}
	return nil
}

func (b *rawBatchIO) LinkedWriteFsync(fd *os.File, buf []byte, offset int64) error {
	b.ring.mu.Lock()
	defer b.ring.mu.Unlock()

	fdInt := int32(fd.Fd())

	// SQE 0: pwrite with IO_LINK
	s0 := b.ring.getSQE(0)
	*s0 = sqe{}
	s0.opcode = opWrite
	s0.flags = sqeFlagIOLink
	s0.fd = fdInt
	s0.addr = uint64(uintptr(unsafe.Pointer(&buf[0])))
	s0.len = uint32(len(buf))
	s0.off = uint64(offset)

	// SQE 1: fdatasync
	s1 := b.ring.getSQE(1)
	*s1 = sqe{}
	s1.opcode = opFsync
	s1.fd = fdInt
	s1.opcFlags = fsyncDatasync

	results, err := b.ring.submitAndWait(2)
	if err != nil {
		// Fallback to sequential.
		if _, werr := fd.WriteAt(buf, offset); werr != nil {
			return werr
		}
		return fdatasync(fd)
	}

	for i, r := range results {
		if r.res < 0 {
			return fmt.Errorf("iouring LinkedWriteFsync op[%d]: errno %d", i, -r.res)
		}
	}
	return nil
}

func (b *rawBatchIO) Close() error {
	if b.ring != nil {
		b.ring.close()
	}
	return nil
}

// unsafeSlice creates a byte slice from a pointer and length for munmap.
func unsafeSlice(ptr uintptr, length int) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(ptr)), length)
}
