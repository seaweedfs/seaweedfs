# POSIX Compliance Implementation Roadmap

This document tracks the implementation status of POSIX features in the SeaweedFS FUSE mount compliance test suite and provides a roadmap for completing comprehensive POSIX compliance testing.

## Overview

The POSIX compliance test suite currently has several tests that are skipped due to requiring platform-specific implementations. This roadmap outlines the steps needed to implement these tests and achieve comprehensive POSIX compliance validation.

## Current Status

### ✅ Implemented Features
- Basic file operations (create, read, write, delete)
- Directory operations (mkdir, rmdir, rename)
- File permissions and ownership
- Symbolic and hard links
- Basic I/O operations (seek, append, positioned I/O)
- File descriptors and atomic operations
- Concurrent access patterns
- Error handling compliance
- Timestamp operations

### 🚧 Partially Implemented
- Cross-platform compatibility (basic implementation with platform-specific access time handling)

### ❌ Missing Implementations

#### 1. Extended Attributes (xattr)
**Priority: High**
**Platforms: Linux, macOS, FreeBSD**

**Current Status:** All xattr tests are skipped
- `TestExtendedAttributes/SetExtendedAttribute`
- `TestExtendedAttributes/ListExtendedAttributes` 
- `TestExtendedAttributes/RemoveExtendedAttribute`

**Implementation Plan:**
```go
// Linux implementation
//go:build linux
func setXattr(path, name string, value []byte) error {
    return syscall.Setxattr(path, name, value, 0)
}

// macOS implementation  
//go:build darwin
func setXattr(path, name string, value []byte) error {
    return syscall.Setxattr(path, name, value, 0, 0)
}
```

**Required Files:**
- `xattr_linux.go` - Linux syscall implementations
- `xattr_darwin.go` - macOS syscall implementations  
- `xattr_freebsd.go` - FreeBSD syscall implementations
- `xattr_unsupported.go` - Fallback for unsupported platforms

#### 2. Memory Mapping (mmap)
**Priority: High**
**Platforms: All POSIX systems**

**Current Status:** All mmap tests are skipped
- `TestMemoryMapping/MemoryMappedRead`
- `TestMemoryMapping/MemoryMappedWrite`

**Implementation Plan:**
```go
func testMmap(t *testing.T, filePath string) {
    fd, err := syscall.Open(filePath, syscall.O_RDWR, 0)
    require.NoError(t, err)
    defer syscall.Close(fd)
    
    // Platform-specific mmap implementation
    data, err := syscall.Mmap(fd, 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
    require.NoError(t, err)
    defer syscall.Munmap(data)
}
```

**Required Files:**
- `mmap_unix.go` - Unix-like systems implementation
- `mmap_windows.go` - Windows implementation (if needed)

#### 3. Vectored I/O (readv/writev)
**Priority: Medium**
**Platforms: All POSIX systems**

**Current Status:** Vectored I/O test is skipped
- `TestAdvancedIO/VectoredIO`

**Implementation Plan:**
```go
func testVectoredIO(t *testing.T, filePath string) {
    // Use syscall.Syscall for readv/writev
    // Linux: SYS_READV, SYS_WRITEV
    // Other platforms: platform-specific syscall numbers
}
```

#### 4. Direct I/O
**Priority: Medium**  
**Platforms: Linux, some Unix variants**

**Current Status:** Direct I/O test is skipped
- `TestDirectIO/DirectIO`

**Implementation Plan:**
```go
//go:build linux
func testDirectIO(t *testing.T, filePath string) {
    fd, err := syscall.Open(filePath, syscall.O_RDWR|syscall.O_DIRECT, 0)
    // Test with aligned buffers
}
```

#### 5. File Preallocation (fallocate)
**Priority: Medium**
**Platforms: Linux, some Unix variants**

**Current Status:** fallocate test is skipped
- `TestFilePreallocation/Fallocate`

**Implementation Plan:**
```go
//go:build linux
func testFallocate(t *testing.T, filePath string) {
    fd, err := syscall.Open(filePath, syscall.O_RDWR, 0)
    err = syscall.Syscall6(syscall.SYS_FALLOCATE, uintptr(fd), 0, 0, uintptr(size), 0, 0)
}
```

#### 6. Zero-Copy Transfer (sendfile)
**Priority: Low**
**Platforms: Linux, FreeBSD, some Unix variants**

**Current Status:** sendfile test is skipped
- `TestZeroCopyTransfer/Sendfile`

**Implementation Plan:**
```go
//go:build linux
func testSendfile(t *testing.T, srcPath, dstPath string) {
    // Use syscall.Syscall for sendfile
    // Platform-specific implementations
}
```

#### 7. File Sealing (Linux-specific)
**Priority: Low**
**Platforms: Linux only**

**Current Status:** File sealing test is skipped
- `TestFileSealing`

**Implementation Plan:**
```go
//go:build linux
func testFileSealing(t *testing.T) {
    // Use fcntl with F_ADD_SEALS, F_GET_SEALS
    // Test various seal types: F_SEAL_WRITE, F_SEAL_SHRINK, etc.
}
```

## Implementation Strategy

### Phase 1: Core Features (High Priority)
1. **Extended Attributes** - Essential for many applications
2. **Memory Mapping** - Critical for performance-sensitive applications

### Phase 2: Advanced I/O (Medium Priority)  
3. **Vectored I/O** - Important for efficient bulk operations
4. **Direct I/O** - Needed for database and high-performance applications
5. **File Preallocation** - Important for preventing fragmentation

### Phase 3: Specialized Features (Low Priority)
6. **Zero-Copy Transfer** - Performance optimization feature
7. **File Sealing** - Security feature, Linux-specific

## Platform Support Matrix

| Feature | Linux | macOS | FreeBSD | Windows | Notes |
|---------|-------|-------|---------|---------|-------|
| Extended Attributes | ✅ | ✅ | ✅ | ❌ | Different syscall signatures |
| Memory Mapping | ✅ | ✅ | ✅ | ✅ | Standard POSIX |
| Vectored I/O | ✅ | ✅ | ✅ | ❌ | readv/writev syscalls |
| Direct I/O | ✅ | ❌ | ✅ | ❌ | O_DIRECT flag |
| fallocate | ✅ | ❌ | ❌ | ❌ | Linux-specific |
| sendfile | ✅ | ❌ | ✅ | ❌ | Platform-specific |
| File Sealing | ✅ | ❌ | ❌ | ❌ | Linux-only |

## Development Guidelines

### 1. Platform-Specific Implementation Pattern
```go
// feature_linux.go
//go:build linux
package fuse
func platformSpecificFunction() { /* Linux implementation */ }

// feature_darwin.go  
//go:build darwin
package fuse
func platformSpecificFunction() { /* macOS implementation */ }

// feature_unsupported.go
//go:build !linux && !darwin
package fuse
func platformSpecificFunction() { /* Skip or error */ }
```

### 2. Test Structure Pattern
```go
func (s *POSIXExtendedTestSuite) TestFeature(t *testing.T) {
    if !isFeatureSupported() {
        t.Skip("Feature not supported on this platform")
        return
    }
    
    t.Run("FeatureTest", func(t *testing.T) {
        // Actual test implementation
    })
}
```

### 3. Error Handling
- Use platform-specific error checking
- Provide meaningful error messages
- Gracefully handle unsupported features

## Testing Strategy

### 1. Continuous Integration
- Run tests on multiple platforms (Linux, macOS)
- Use build tags to enable/disable platform-specific tests
- Ensure graceful degradation on unsupported platforms

### 2. Feature Detection
- Implement runtime feature detection where possible
- Skip tests gracefully when features are unavailable
- Log clear messages about skipped functionality

### 3. Documentation
- Document platform-specific behavior
- Provide examples for each implemented feature
- Maintain compatibility matrices

## Contributing

When implementing these features:

1. **Start with high-priority items** (Extended Attributes, Memory Mapping)
2. **Follow the platform-specific pattern** outlined above
3. **Add comprehensive tests** for each feature
4. **Update this roadmap** as features are implemented
5. **Document any platform-specific quirks** or limitations

## Future Enhancements

Beyond the current roadmap, consider:
- **File locking** (flock, fcntl locks)
- **Asynchronous I/O** (aio_read, aio_write)
- **File change notifications** (inotify, kqueue)
- **POSIX ACLs** (Access Control Lists)
- **Sparse file operations**
- **File hole punching**

## References

- [POSIX.1-2017 Standard](https://pubs.opengroup.org/onlinepubs/9699919799/)
- [Linux Programmer's Manual](https://man7.org/linux/man-pages/)
- [FreeBSD System Calls](https://www.freebsd.org/cgi/man.cgi)
- [macOS System Calls](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/)
