// +build windows

package memory_map

import (
	"os"
	"reflect"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
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
	End_Of_File            int64
}

var FileMemoryMap = make(map[string]*MemoryMap)

type DWORD = uint32
type WORD = uint16

var (
	procGetSystemInfo = syscall.NewLazyDLL("kernel32.dll").NewProc("GetSystemInfo")
)

var system_info, err = getSystemInfo()

var chunk_size = uint64(system_info.dwAllocationGranularity) * 512

func (mMap *MemoryMap) CreateMemoryMap(file *os.File, maxlength uint64) {

	maxlength_high := uint32(maxlength >> 32)
	maxlength_low := uint32(maxlength & 0xFFFFFFFF)
	file_memory_map_handle, err := windows.CreateFileMapping(windows.Handle(file.Fd()), nil, windows.PAGE_READWRITE, maxlength_high, maxlength_low, nil)

	if err == nil {
		mMap.File = file
		mMap.file_memory_map_handle = uintptr(file_memory_map_handle)
		mMap.write_map_views = make([]MemoryBuffer, 0, maxlength/chunk_size)
		mMap.max_length = maxlength
		mMap.End_Of_File = -1
	}
}

func (mMap *MemoryMap) DeleteFileAndMemoryMap() {
	windows.CloseHandle(windows.Handle(mMap.file_memory_map_handle))
	windows.CloseHandle(windows.Handle(mMap.File.Fd()))

	for _, view := range mMap.write_map_views {
		view.ReleaseMemory()
	}

	mMap.write_map_views = nil
	mMap.max_length = 0
}

func min(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

func (mMap *MemoryMap) WriteMemory(offset uint64, length uint64, data []byte) {

	for {
		if ((offset+length)/chunk_size)+1 > uint64(len(mMap.write_map_views)) {
			allocateChunk(mMap)
		} else {
			break
		}
	}

	remaining_length := length
	slice_index := offset / chunk_size
	slice_offset := offset - (slice_index * chunk_size)
	data_offset := uint64(0)

	for {
		write_end := min((remaining_length + slice_offset), chunk_size)
		copy(mMap.write_map_views[slice_index].Buffer[slice_offset:write_end], data[data_offset:])
		remaining_length -= (write_end - slice_offset)
		data_offset += (write_end - slice_offset)

		if remaining_length > 0 {
			slice_index += 1
			slice_offset = 0
		} else {
			break
		}
	}

	if mMap.End_Of_File < int64(offset+length-1) {
		mMap.End_Of_File = int64(offset + length - 1)
	}
}

func (mMap *MemoryMap) ReadMemory(offset uint64, length uint64) (MemoryBuffer, error) {
	return allocate(windows.Handle(mMap.file_memory_map_handle), offset, length, false)
}

func (mem_buffer *MemoryBuffer) ReleaseMemory() {
	windows.UnmapViewOfFile(mem_buffer.aligned_ptr)

	mem_buffer.ptr = 0
	mem_buffer.aligned_ptr = 0
	mem_buffer.length = 0
	mem_buffer.aligned_length = 0
	mem_buffer.Buffer = nil
}

func allocateChunk(mMap *MemoryMap) {

	start := uint64(len(mMap.write_map_views)) * chunk_size
	mem_buffer, err := allocate(windows.Handle(mMap.file_memory_map_handle), start, chunk_size, true)

	if err == nil {
		mMap.write_map_views = append(mMap.write_map_views, mem_buffer)
	}
}

func allocate(hMapFile windows.Handle, offset uint64, length uint64, write bool) (MemoryBuffer, error) {

	mem_buffer := MemoryBuffer{}

	dwSysGran := system_info.dwAllocationGranularity

	start := (offset / uint64(dwSysGran)) * uint64(dwSysGran)
	diff := offset - start
	aligned_length := diff + length

	offset_high := uint32(start >> 32)
	offset_low := uint32(start & 0xFFFFFFFF)

	access := windows.FILE_MAP_READ

	if write {
		access = windows.FILE_MAP_WRITE
	}

	addr_ptr, errno := windows.MapViewOfFile(hMapFile,
		uint32(access), // read/write permission
		offset_high,
		offset_low,
		uintptr(aligned_length))

	if addr_ptr == 0 {
		return mem_buffer, errno
	}

	mem_buffer.aligned_ptr = addr_ptr
	mem_buffer.aligned_length = aligned_length
	mem_buffer.ptr = addr_ptr + uintptr(diff)
	mem_buffer.length = length

	slice_header := (*reflect.SliceHeader)(unsafe.Pointer(&mem_buffer.Buffer))
	slice_header.Data = addr_ptr + uintptr(diff)
	slice_header.Len = int(length)
	slice_header.Cap = int(length)

	return mem_buffer, nil
}

// typedef struct _SYSTEM_INFO {
//   union {
//     DWORD  dwOemId;
//     struct {
//       WORD wProcessorArchitecture;
//       WORD wReserved;
//     };
//   };
//   DWORD     dwPageSize;
//   LPVOID    lpMinimumApplicationAddress;
//   LPVOID    lpMaximumApplicationAddress;
//   DWORD_PTR dwActiveProcessorMask;
//   DWORD     dwNumberOfProcessors;
//   DWORD     dwProcessorType;
//   DWORD     dwAllocationGranularity;
//   WORD      wProcessorLevel;
//   WORD      wProcessorRevision;
// } SYSTEM_INFO;
// https://msdn.microsoft.com/en-us/library/ms724958(v=vs.85).aspx
type _SYSTEM_INFO struct {
	dwOemId                     DWORD
	dwPageSize                  DWORD
	lpMinimumApplicationAddress uintptr
	lpMaximumApplicationAddress uintptr
	dwActiveProcessorMask       uintptr
	dwNumberOfProcessors        DWORD
	dwProcessorType             DWORD
	dwAllocationGranularity     DWORD
	wProcessorLevel             WORD
	wProcessorRevision          WORD
}

// void WINAPI GetSystemInfo(
//   _Out_ LPSYSTEM_INFO lpSystemInfo
// );
// https://msdn.microsoft.com/en-us/library/ms724381(VS.85).aspx
func getSystemInfo() (_SYSTEM_INFO, error) {
	var si _SYSTEM_INFO
	_, _, err := procGetSystemInfo.Call(
		uintptr(unsafe.Pointer(&si)),
	)
	if err != syscall.Errno(0) {
		return si, err
	}
	return si, nil
}
