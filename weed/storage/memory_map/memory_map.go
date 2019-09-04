// +build !windows

package memory_map

import (
	"fmt"
	"os"
)

type MemoryBuffer struct {
	aligned_length uint64
	length         uint64
	aligned_ptr    uintptr
	ptr            uintptr
	Buffer         []byte
}

type MemoryMap struct {
	File                   *os.File
	file_memory_map_handle uintptr
	write_map_views        []MemoryBuffer
	max_length             uint64
	End_of_file            int64
}

var FileMemoryMap = make(map[string]*MemoryMap)

func (mMap *MemoryMap) WriteMemory(offset uint64, length uint64, data []byte) {

}

func (mMap *MemoryMap) ReadMemory(offset uint64, length uint64) (MemoryBuffer, error) {
	return MemoryBuffer{}, fmt.Errorf("Memory Map not implemented for this platform")
}

func (mem_buffer *MemoryBuffer) ReleaseMemory() {

}

func (mMap *MemoryMap) DeleteFileAndMemoryMap() {

}
