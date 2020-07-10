package memory_map

import (
	"os"
	"time"
)

var (
// _ backend.BackendStorageFile = &MemoryMappedFile{} // remove this to break import cycle
)

type MemoryMappedFile struct {
	mm *MemoryMap
}

func NewMemoryMappedFile(f *os.File, memoryMapSizeMB uint32) *MemoryMappedFile {
	mmf := &MemoryMappedFile{
		mm: new(MemoryMap),
	}
	mmf.mm.CreateMemoryMap(f, 1024*1024*uint64(memoryMapSizeMB))
	return mmf
}

func (mmf *MemoryMappedFile) ReadAt(p []byte, off int64) (n int, err error) {
	readBytes, e := mmf.mm.ReadMemory(uint64(off), uint64(len(p)))
	if e != nil {
		return 0, e
	}
	// TODO avoid the extra copy
	copy(p, readBytes)
	return len(readBytes), nil
}

func (mmf *MemoryMappedFile) WriteAt(p []byte, off int64) (n int, err error) {
	mmf.mm.WriteMemory(uint64(off), uint64(len(p)), p)
	return len(p), nil
}

func (mmf *MemoryMappedFile) Truncate(off int64) error {
	return nil
}

func (mmf *MemoryMappedFile) Close() error {
	mmf.mm.DeleteFileAndMemoryMap()
	return nil
}

func (mmf *MemoryMappedFile) GetStat() (datSize int64, modTime time.Time, err error) {
	stat, e := mmf.mm.File.Stat()
	if e == nil {
		return mmf.mm.End_of_file + 1, stat.ModTime(), nil
	}
	return 0, time.Time{}, err
}

func (mmf *MemoryMappedFile) Name() string {
	return mmf.mm.File.Name()
}

func (mm *MemoryMappedFile) Sync() error {
	return nil
}
