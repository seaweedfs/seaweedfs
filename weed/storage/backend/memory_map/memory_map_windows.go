//go:build windows
// +build windows

package memory_map

import (
	"os"
	"reflect"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

type DWORDLONG = uint64
type DWORD = uint32
type WORD = uint16

var (
	modkernel32 = syscall.NewLazyDLL("kernel32.dll")

	procGetSystemInfo            = modkernel32.NewProc("GetSystemInfo")
	procGlobalMemoryStatusEx     = modkernel32.NewProc("GlobalMemoryStatusEx")
	procGetProcessWorkingSetSize = modkernel32.NewProc("GetProcessWorkingSetSize")
	procSetProcessWorkingSetSize = modkernel32.NewProc("SetProcessWorkingSetSize")
)

var currentProcess, _ = windows.GetCurrentProcess()
var currentMinWorkingSet uint64 = 0
var currentMaxWorkingSet uint64 = 0
var _ = getProcessWorkingSetSize(uintptr(currentProcess), &currentMinWorkingSet, &currentMaxWorkingSet)

var systemInfo, _ = getSystemInfo()
var chunkSize = uint64(systemInfo.dwAllocationGranularity) * 128

var memoryStatusEx, _ = globalMemoryStatusEx()
var maxMemoryLimitBytes = uint64(float64(memoryStatusEx.ullTotalPhys) * 0.8)

func (mMap *MemoryMap) CreateMemoryMap(file *os.File, maxLength uint64) {

	chunks := (maxLength / chunkSize)
	if chunks*chunkSize < maxLength {
		chunks = chunks + 1
	}

	alignedMaxLength := chunks * chunkSize

	maxLength_high := uint32(alignedMaxLength >> 32)
	maxLength_low := uint32(alignedMaxLength & 0xFFFFFFFF)
	file_memory_map_handle, err := windows.CreateFileMapping(windows.Handle(file.Fd()), nil, windows.PAGE_READWRITE, maxLength_high, maxLength_low, nil)

	if err == nil {
		mMap.File = file
		mMap.file_memory_map_handle = uintptr(file_memory_map_handle)
		mMap.write_map_views = make([]MemoryBuffer, 0, alignedMaxLength/chunkSize)
		mMap.max_length = alignedMaxLength
		mMap.End_of_file = -1
	}
}

func (mMap *MemoryMap) DeleteFileAndMemoryMap() {
	//First we close the file handles first to delete the file,
	//Then we unmap the memory to ensure the unmapping process doesn't write the data to disk
	windows.CloseHandle(windows.Handle(mMap.file_memory_map_handle))
	windows.CloseHandle(windows.Handle(mMap.File.Fd()))

	for _, view := range mMap.write_map_views {
		view.releaseMemory()
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
		if ((offset+length)/chunkSize)+1 > uint64(len(mMap.write_map_views)) {
			allocateChunk(mMap)
		} else {
			break
		}
	}

	remaining_length := length
	sliceIndex := offset / chunkSize
	sliceOffset := offset - (sliceIndex * chunkSize)
	dataOffset := uint64(0)

	for {
		writeEnd := min((remaining_length + sliceOffset), chunkSize)
		copy(mMap.write_map_views[sliceIndex].Buffer[sliceOffset:writeEnd], data[dataOffset:])
		remaining_length -= (writeEnd - sliceOffset)
		dataOffset += (writeEnd - sliceOffset)

		if remaining_length > 0 {
			sliceIndex += 1
			sliceOffset = 0
		} else {
			break
		}
	}

	if mMap.End_of_file < int64(offset+length-1) {
		mMap.End_of_file = int64(offset + length - 1)
	}
}

func (mMap *MemoryMap) ReadMemory(offset uint64, length uint64) (dataSlice []byte, err error) {
	dataSlice = make([]byte, length)
	mBuffer, err := allocate(windows.Handle(mMap.file_memory_map_handle), offset, length, false)
	copy(dataSlice, mBuffer.Buffer)
	mBuffer.releaseMemory()
	return dataSlice, err
}

func (mBuffer *MemoryBuffer) releaseMemory() {

	windows.VirtualUnlock(mBuffer.aligned_ptr, uintptr(mBuffer.aligned_length))
	windows.UnmapViewOfFile(mBuffer.aligned_ptr)

	currentMinWorkingSet -= mBuffer.aligned_length
	currentMaxWorkingSet -= mBuffer.aligned_length

	if currentMinWorkingSet < maxMemoryLimitBytes {
		var _ = setProcessWorkingSetSize(uintptr(currentProcess), currentMinWorkingSet, currentMaxWorkingSet)
	}

	mBuffer.ptr = 0
	mBuffer.aligned_ptr = 0
	mBuffer.length = 0
	mBuffer.aligned_length = 0
	mBuffer.Buffer = nil
}

func allocateChunk(mMap *MemoryMap) {
	start := uint64(len(mMap.write_map_views)) * chunkSize
	mBuffer, err := allocate(windows.Handle(mMap.file_memory_map_handle), start, chunkSize, true)

	if err == nil {
		mMap.write_map_views = append(mMap.write_map_views, mBuffer)
	}
}

func allocate(hMapFile windows.Handle, offset uint64, length uint64, write bool) (MemoryBuffer, error) {

	mBuffer := MemoryBuffer{}

	//align memory allocations to the minium virtual memory allocation size
	dwSysGran := systemInfo.dwAllocationGranularity

	start := (offset / uint64(dwSysGran)) * uint64(dwSysGran)
	diff := offset - start
	aligned_length := diff + length

	offset_high := uint32(start >> 32)
	offset_low := uint32(start & 0xFFFFFFFF)

	access := windows.FILE_MAP_READ

	if write {
		access = windows.FILE_MAP_WRITE
	}

	currentMinWorkingSet += aligned_length
	currentMaxWorkingSet += aligned_length

	if currentMinWorkingSet < maxMemoryLimitBytes {
		// increase the process working set size to hint to windows memory manager to
		// prioritise keeping this memory mapped in physical memory over other standby memory
		var _ = setProcessWorkingSetSize(uintptr(currentProcess), currentMinWorkingSet, currentMaxWorkingSet)
	}

	addr_ptr, errno := windows.MapViewOfFile(hMapFile,
		uint32(access), // read/write permission
		offset_high,
		offset_low,
		uintptr(aligned_length))

	if addr_ptr == 0 {
		return mBuffer, errno
	}

	if currentMinWorkingSet < maxMemoryLimitBytes {
		windows.VirtualLock(mBuffer.aligned_ptr, uintptr(mBuffer.aligned_length))
	}

	mBuffer.aligned_ptr = addr_ptr
	mBuffer.aligned_length = aligned_length
	mBuffer.ptr = addr_ptr + uintptr(diff)
	mBuffer.length = length

	slice_header := (*reflect.SliceHeader)(unsafe.Pointer(&mBuffer.Buffer))
	slice_header.Data = addr_ptr + uintptr(diff)
	slice_header.Len = int(length)
	slice_header.Cap = int(length)

	return mBuffer, nil
}

//typedef struct _MEMORYSTATUSEX {
//	DWORD     dwLength;
//	DWORD     dwMemoryLoad;
//	DWORDLONG ullTotalPhys;
//	DWORDLONG ullAvailPhys;
//	DWORDLONG ullTotalPageFile;
//	DWORDLONG ullAvailPageFile;
//	DWORDLONG ullTotalVirtual;
//	DWORDLONG ullAvailVirtual;
//	DWORDLONG ullAvailExtendedVirtual;
//  } MEMORYSTATUSEX, *LPMEMORYSTATUSEX;
//https://docs.microsoft.com/en-gb/windows/win32/api/sysinfoapi/ns-sysinfoapi-memorystatusex

type _MEMORYSTATUSEX struct {
	dwLength                DWORD
	dwMemoryLoad            DWORD
	ullTotalPhys            DWORDLONG
	ullAvailPhys            DWORDLONG
	ullTotalPageFile        DWORDLONG
	ullAvailPageFile        DWORDLONG
	ullTotalVirtual         DWORDLONG
	ullAvailVirtual         DWORDLONG
	ullAvailExtendedVirtual DWORDLONG
}

// BOOL GlobalMemoryStatusEx(
//
//	LPMEMORYSTATUSEX lpBuffer
//
// );
// https://docs.microsoft.com/en-gb/windows/win32/api/sysinfoapi/nf-sysinfoapi-globalmemorystatusex
func globalMemoryStatusEx() (_MEMORYSTATUSEX, error) {
	var mem_status _MEMORYSTATUSEX

	mem_status.dwLength = uint32(unsafe.Sizeof(mem_status))
	_, _, err := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&mem_status)))

	if err != syscall.Errno(0) {
		return mem_status, err
	}
	return mem_status, nil
}

//	typedef struct _SYSTEM_INFO {
//	  union {
//	    DWORD  dwOemId;
//	    struct {
//	      WORD wProcessorArchitecture;
//	      WORD wReserved;
//	    };
//	  };
//	  DWORD     dwPageSize;
//	  LPVOID    lpMinimumApplicationAddress;
//	  LPVOID    lpMaximumApplicationAddress;
//	  DWORD_PTR dwActiveProcessorMask;
//	  DWORD     dwNumberOfProcessors;
//	  DWORD     dwProcessorType;
//	  DWORD     dwAllocationGranularity;
//	  WORD      wProcessorLevel;
//	  WORD      wProcessorRevision;
//	} SYSTEM_INFO;
//
// https://docs.microsoft.com/en-gb/windows/win32/api/sysinfoapi/ns-sysinfoapi-system_info
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
//
//	_Out_ LPSYSTEM_INFO lpSystemInfo
//
// );
// https://docs.microsoft.com/en-us/windows/win32/api/sysinfoapi/nf-sysinfoapi-getsysteminfo
func getSystemInfo() (_SYSTEM_INFO, error) {
	var si _SYSTEM_INFO
	_, _, err := procGetSystemInfo.Call(uintptr(unsafe.Pointer(&si)))
	if err != syscall.Errno(0) {
		return si, err
	}
	return si, nil
}

// BOOL GetProcessWorkingSetSize(
//   HANDLE  hProcess,
//   PSIZE_T lpMinimumWorkingSetSize,
//   PSIZE_T lpMaximumWorkingSetSize
// );
// https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-getprocessworkingsetsize

func getProcessWorkingSetSize(process uintptr, dwMinWorkingSet *uint64, dwMaxWorkingSet *uint64) error {
	r1, _, err := syscall.Syscall(procGetProcessWorkingSetSize.Addr(), 3, process, uintptr(unsafe.Pointer(dwMinWorkingSet)), uintptr(unsafe.Pointer(dwMaxWorkingSet)))
	if r1 == 0 {
		if err != syscall.Errno(0) {
			return err
		}
	}
	return nil
}

// BOOL SetProcessWorkingSetSize(
//   HANDLE hProcess,
//   SIZE_T dwMinimumWorkingSetSize,
//   SIZE_T dwMaximumWorkingSetSize
// );
// https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-setprocessworkingsetsize

func setProcessWorkingSetSize(process uintptr, dwMinWorkingSet uint64, dwMaxWorkingSet uint64) error {
	r1, _, err := syscall.Syscall(procSetProcessWorkingSetSize.Addr(), 3, process, uintptr(dwMinWorkingSet), uintptr(dwMaxWorkingSet))
	if r1 == 0 {
		if err != syscall.Errno(0) {
			return err
		}
	}
	return nil
}
