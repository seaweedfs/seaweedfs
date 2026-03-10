package batchio

import "os"

// standardBatchIO implements BatchIO with sequential os.File calls.
// This is functionally identical to calling ReadAt/WriteAt/Sync directly.
type standardBatchIO struct{}

// NewStandard returns a BatchIO that uses sequential pread/pwrite/fdatasync.
// This is the default (and only) implementation on non-Linux platforms.
func NewStandard() BatchIO {
	return &standardBatchIO{}
}

func (s *standardBatchIO) PreadBatch(fd *os.File, ops []Op) error {
	for i := range ops {
		if _, err := fd.ReadAt(ops[i].Buf, ops[i].Offset); err != nil {
			return err
		}
	}
	return nil
}

func (s *standardBatchIO) PwriteBatch(fd *os.File, ops []Op) error {
	for i := range ops {
		if _, err := fd.WriteAt(ops[i].Buf, ops[i].Offset); err != nil {
			return err
		}
	}
	return nil
}

func (s *standardBatchIO) Fsync(fd *os.File) error {
	return fd.Sync()
}

func (s *standardBatchIO) LinkedWriteFsync(fd *os.File, buf []byte, offset int64) error {
	if _, err := fd.WriteAt(buf, offset); err != nil {
		return err
	}
	return fd.Sync()
}

func (s *standardBatchIO) Close() error {
	return nil
}
