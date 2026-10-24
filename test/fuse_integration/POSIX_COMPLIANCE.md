# SeaweedFS FUSE POSIX Compliance Testing

## Overview

This comprehensive test suite provides full POSIX compliance testing for SeaweedFS FUSE mounts. It includes basic POSIX operations, extended features, external test suite integration, and performance benchmarking to ensure SeaweedFS meets filesystem standards.

## 🎯 Key Features

### ✅ **Comprehensive Test Coverage**
- **Basic POSIX Operations**: File/directory create, read, write, delete, rename
- **Advanced Features**: Extended attributes, file locking, memory mapping *(see roadmap)*
- **I/O Operations**: Synchronous/asynchronous I/O, direct I/O, vectored I/O *(see roadmap)*
- **Error Handling**: Comprehensive error condition testing
- **Concurrent Operations**: Multi-threaded stress testing
- **External Integration**: pjdfstest, nfstest, FIO integration

> **📋 Implementation Status**: Some advanced features are currently skipped pending platform-specific implementations. See [`POSIX_IMPLEMENTATION_ROADMAP.md`](./POSIX_IMPLEMENTATION_ROADMAP.md) for a detailed implementation plan and current status.

### 📊 **Performance Analysis**
- **Benchmarking**: Built-in performance benchmarks
- **Profiling**: CPU and memory profiling support
- **Coverage Analysis**: Code coverage reporting
- **Stress Testing**: High-load concurrent operation testing

### 🔧 **External Tool Integration**
- **pjdfstest**: Industry-standard POSIX filesystem test suite
- **nfstest_posix**: Network filesystem POSIX API verification
- **FIO**: Flexible I/O performance testing
- **Custom Litmus Tests**: Focused edge case testing

## 📋 Test Categories

### 1. Basic POSIX Compliance (`posix_compliance_test.go`)

#### File Operations
- ✅ File creation with O_CREAT, O_EXCL flags
- ✅ File truncation and size management
- ✅ File deletion and unlinking
- ✅ Atomic rename operations
- ✅ File permission management

#### Directory Operations
- ✅ Directory creation and removal
- ✅ Directory listing and traversal
- ✅ Directory rename operations
- ✅ Non-empty directory handling

#### Symlink Operations
- ✅ Symbolic link creation and resolution
- ✅ Broken symlink handling
- ✅ Symlink target verification
- ✅ Symlink permission handling

#### Permission Tests
- ✅ File permission setting and verification
- ✅ Directory permission enforcement
- ✅ Permission inheritance
- ✅ Access control validation

#### Timestamp Tests
- ✅ Access time (atime) updates
- ✅ Modification time (mtime) tracking
- ✅ Change time (ctime) management
- ✅ Timestamp precision and setting

#### I/O Operations
- ✅ Read/write operations
- ✅ File seeking and positioning
- ✅ Append mode operations
- ✅ Buffer management

#### File Descriptors
- ✅ File descriptor duplication
- ✅ File descriptor flags
- ✅ Close-on-exec handling
- ✅ File descriptor limits

#### Atomic Operations
- ✅ Atomic file operations
- ✅ Rename atomicity
- ✅ Link/unlink atomicity

#### Concurrent Access
- ✅ Multi-reader scenarios
- ✅ Concurrent write handling
- ✅ Reader-writer coordination
- ✅ Race condition prevention

#### Error Handling
- ✅ ENOENT (file not found) handling
- ✅ EACCES (permission denied) handling
- ✅ EBADF (bad file descriptor) handling
- ✅ ENOTEMPTY (directory not empty) handling
- ✅ Error code compliance

### 2. Extended POSIX Features (`posix_extended_test.go`)

#### Extended Attributes
- ✅ Setting extended attributes (setxattr)
- ✅ Getting extended attributes (getxattr)
- ✅ Listing extended attributes (listxattr)
- ✅ Removing extended attributes (removexattr)
- ✅ Extended attribute namespaces

#### File Locking
- ✅ Advisory locking (fcntl)
- ✅ Exclusive locks (F_WRLCK)
- ✅ Shared locks (F_RDLCK)
- ✅ Lock conflict detection
- ✅ Lock release and cleanup

#### Advanced I/O
- ✅ Vectored I/O (readv/writev)
- ✅ Positioned I/O (pread/pwrite)
- ✅ Asynchronous I/O patterns
- ✅ Scatter-gather operations

#### Sparse Files
- ✅ Sparse file creation
- ✅ Hole detection and handling
- ✅ Sparse file efficiency
- ✅ Seek beyond EOF

#### Large Files (>2GB)
- ✅ Large file operations
- ✅ 64-bit offset handling
- ✅ Large file seeking
- ✅ Large file truncation

#### Memory Mapping
- ✅ File memory mapping (mmap)
- ✅ Memory-mapped I/O
- ✅ Memory synchronization (msync)
- ✅ Memory unmapping (munmap)
- ✅ Shared/private mappings

#### Direct I/O
- ✅ Direct I/O operations (O_DIRECT)
- ✅ Buffer alignment requirements
- ✅ Direct I/O performance
- ✅ Bypass buffer cache

#### File Preallocation
- ✅ Space preallocation (fallocate)
- ✅ File hole punching
- ✅ Space reservation
- ✅ Allocation efficiency

#### Zero-Copy Operations
- ✅ Zero-copy file transfer (sendfile)
- ✅ Efficient data movement
- ✅ Cross-filesystem transfers

### 3. External Test Suite Integration (`posix_external_test.go`)

#### pjdfstest Integration
- ✅ Comprehensive POSIX filesystem test suite
- ✅ Industry-standard compliance verification
- ✅ Automated test execution
- ✅ Result analysis and reporting

#### nfstest_posix Integration
- ✅ POSIX API verification
- ✅ Network filesystem compliance
- ✅ API behavior validation
- ✅ Comprehensive coverage

#### Custom Litmus Tests
- ✅ Atomic rename verification
- ✅ Link count accuracy
- ✅ Symlink cycle detection
- ✅ Concurrent operation safety
- ✅ Directory consistency

#### Stress Testing
- ✅ High-frequency operations
- ✅ Concurrent access patterns
- ✅ Resource exhaustion scenarios
- ✅ Error recovery testing

#### Edge Case Testing
- ✅ Empty filenames
- ✅ Very long filenames
- ✅ Special characters
- ✅ Deep directory nesting
- ✅ Path length limits

## 🚀 Quick Start

### Prerequisites

```bash
# Install required tools
sudo apt-get install fuse           # Linux
brew install macfuse               # macOS

# Install Go 1.21+
go version

# Build SeaweedFS
cd /path/to/seaweedfs
make
```

### Basic Usage

```bash
# Navigate to test directory
cd test/fuse_integration

# Check prerequisites
make -f posix_Makefile check-prereqs

# Run basic POSIX compliance tests
make -f posix_Makefile test-posix-basic

# Run complete test suite
make -f posix_Makefile test-posix-full

# Generate compliance report
make -f posix_Makefile generate-report
```

## 🔧 Advanced Usage

### Test Categories

#### Critical Tests Only
```bash
# Run only critical POSIX compliance tests (faster)
make -f posix_Makefile test-posix-critical
```

#### Extended Feature Tests
```bash
# Test advanced POSIX features
make -f posix_Makefile test-posix-extended
```

#### External Tool Integration
```bash
# Setup and run external test suites
make -f posix_Makefile setup-external-tools
make -f posix_Makefile test-posix-external
```

#### Stress Testing
```bash
# Run stress tests for concurrent operations
make -f posix_Makefile test-posix-stress
```

### Performance Analysis

#### Benchmarking
```bash
# Run performance benchmarks
make -f posix_Makefile benchmark-posix
```

#### Profiling
```bash
# Profile CPU and memory usage
make -f posix_Makefile profile-posix

# View CPU profile
go tool pprof reports/posix.cpu.prof
```

#### Coverage Analysis
```bash
# Generate test coverage report
make -f posix_Makefile coverage-posix
# View: reports/posix_coverage.html
```

#### FIO Performance Testing
```bash
# Run FIO-based I/O performance tests
make -f posix_Makefile test-fio-posix
```

### External Test Suites

#### pjdfstest
```bash
# Setup and run pjdfstest
make -f posix_Makefile setup-pjdfstest
make -f posix_Makefile test-pjdfstest
```

#### nfstest_posix
```bash
# Install and run nfstest_posix
make -f posix_Makefile setup-nfstest
make -f posix_Makefile test-nfstest-posix
```

## 📊 Understanding Results

### Test Status Indicators

- ✅ **PASS**: Test completed successfully
- ❌ **FAIL**: Test failed - indicates non-compliance
- ⚠️ **SKIP**: Test skipped - feature not supported
- 🔄 **RETRY**: Test retried due to transient failure

### Report Formats

#### HTML Report (`reports/posix_compliance_report.html`)
- Interactive web-based report
- Test category breakdown
- Detailed results with navigation
- Visual status indicators

#### Text Summary (`reports/posix_compliance_summary.txt`)
- Concise text-based summary
- Key findings and recommendations
- Command-line friendly format

#### JSON Report (`reports/posix_compliance_report.json`)
- Machine-readable results
- Integration with CI/CD pipelines
- Automated result processing

### Log Files

- `posix_basic_results.log`: Basic POSIX test detailed output
- `posix_extended_results.log`: Extended feature test output
- `posix_external_results.log`: External test suite output
- `posix_benchmark_results.log`: Performance benchmark results

## 🐛 Debugging Failed Tests

### Common Issues

#### Permission Denied Errors
```bash
# Ensure proper FUSE permissions
sudo usermod -a -G fuse $USER
# Re-login or run: newgrp fuse

# Mount with -allowOthers if other users need access (test config MountOptions)
```

#### Extended Attributes Not Supported
```bash
# Check if xattr is supported by underlying filesystem
getfattr --version
# May need to skip xattr tests on certain filesystems
```

#### File Locking Issues
```bash
# Verify file locking support
# Some network filesystems may not support full locking
```

#### Memory Issues
```bash
# Increase available memory for tests
# Large file tests may require significant RAM
```

### Debug Mode

```bash
# Test targets already run go test -v and keep logs under reports/
make -f posix_Makefile test-posix-basic

# Run against an existing external mount instead of a fresh cluster
make -f posix_Makefile test-posix-basic TEST_MOUNT_POINT=/path/to/mount

# Extend the 45-minute per-suite timeout
make -f posix_Makefile test-posix-full TEST_TIMEOUT=90m
```

### Manual Test Execution

```bash
# Run specific test
cd test/fuse_integration
go test -v -run 'TestPOSIXCompliance/FileOperations' .

# Run with custom timeout
go test -v -timeout 30m -run TestPOSIXExtended .
```

## 🔄 CI/CD Integration

### GitHub Actions

```yaml
name: POSIX Compliance Tests

on: [push, pull_request]

jobs:
  posix-compliance:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-go@v4
      with:
        go-version: '1.21'
    
    - name: Install FUSE
      run: sudo apt-get update && sudo apt-get install -y fuse
    
    - name: Build SeaweedFS
      run: make
    
    - name: Run POSIX Compliance Tests
      run: |
        cd test/fuse_integration
        make -f posix_Makefile ci-posix-tests
    
    - name: Upload Test Results
      uses: actions/upload-artifact@v4
      if: always()
      with:
        name: posix-test-results
        path: test/fuse_integration/reports/
```

### Jenkins Pipeline

```groovy
pipeline {
    agent any
    stages {
        stage('Setup') {
            steps {
                sh 'make'
            }
        }
        stage('POSIX Compliance') {
            steps {
                dir('test/fuse_integration') {
                    sh 'make -f posix_Makefile ci-posix-tests'
                }
            }
            post {
                always {
                    archiveArtifacts 'test/fuse_integration/reports/**'
                    publishHTML([
                        allowMissing: false,
                        alwaysLinkToLastBuild: true,
                        keepAll: true,
                        reportDir: 'test/fuse_integration/reports',
                        reportFiles: 'posix_compliance_report.html',
                        reportName: 'POSIX Compliance Report'
                    ])
                }
            }
        }
    }
}
```

## 🎯 POSIX Compliance Checklist

### Critical POSIX Features
- [ ] File creation, reading, writing, deletion
- [ ] Directory operations (create, remove, list)
- [ ] File permissions and ownership
- [ ] Symbolic links
- [ ] Hard links
- [ ] File timestamps
- [ ] Error handling compliance
- [ ] Atomic operations

### Advanced POSIX Features
- [ ] Extended attributes
- [ ] File locking (advisory)
- [ ] Memory-mapped I/O
- [ ] Vectored I/O (readv/writev)
- [ ] Positioned I/O (pread/pwrite)
- [ ] Direct I/O
- [ ] File preallocation
- [ ] Sparse files

### Performance Requirements
- [ ] Reasonable performance under load
- [ ] Concurrent access handling
- [ ] Memory usage optimization
- [ ] CPU efficiency
- [ ] I/O throughput

### Reliability Requirements
- [ ] Data consistency
- [ ] Error recovery
- [ ] Race condition prevention
- [ ] Resource cleanup
- [ ] Crash recovery

## 📈 Performance Benchmarks

### Expected Performance Characteristics

| Operation | Expected Range | Notes |
|-----------|----------------|-------|
| File Create | 1000-10000 ops/sec | Depends on chunk size |
| File Read | 100-1000 MB/sec | Network limited |
| File Write | 50-500 MB/sec | Depends on replication |
| Directory Ops | 500-5000 ops/sec | Metadata operations |
| Concurrent Ops | Scales with cores | Up to hardware limits |

### Benchmarking Commands

```bash
# Basic I/O performance
make -f posix_Makefile benchmark-posix

# FIO comprehensive testing
make -f posix_Makefile test-fio-posix

# Custom benchmark with specific parameters
go test -v -bench=. -benchtime=30s posix_compliance_test.go
```

## 🔍 Troubleshooting

### Common Test Failures

#### 1. Mount Permission Issues
```bash
# Error: Permission denied mounting
# Solution: Check FUSE permissions — add the user to the fuse group and
# re-login rather than widening permissions on the device node
sudo usermod -a -G fuse $USER
```

#### 2. Extended Attribute Tests Fail
```bash
# Error: Operation not supported (ENOTSUP)
# Solution: Check filesystem xattr support
mount | grep $(df . | tail -1 | awk '{print $1}')
# May need to remount with xattr support
```

#### 3. File Locking Tests Fail
```bash
# Error: Function not implemented (ENOSYS)
# Solution: Some network filesystems don't support locking
# This may be expected behavior
```

#### 4. Large File Tests Fail
```bash
# Error: No space left on device
# Solution: Ensure sufficient disk space
df -h /tmp  # Check temp directory space
# May need to clean up or use different temp directory
```

#### 5. Performance Tests Timeout
```bash
# Error: Test timeout exceeded
# Solution: Increase timeout or reduce test scope
go test -timeout 60m -run TestPOSIXCompliance
```

### Debug Information Collection

```bash
# Collect system information
uname -a > debug_info.txt
mount >> debug_info.txt
df -h >> debug_info.txt
free -h >> debug_info.txt

# Collect SeaweedFS information
./weed version >> debug_info.txt
ps aux | grep weed >> debug_info.txt

# Collect test logs
cp reports/*.log debug_logs/
```

## 📚 Additional Resources

### POSIX Standards
- [POSIX.1-2017 (IEEE Std 1003.1-2017)](https://pubs.opengroup.org/onlinepubs/9699919799/)
- [Single UNIX Specification](https://www.opengroup.org/openbrand/register/)

### External Test Suites
- [pjdfstest](https://github.com/pjd/pjdfstest) - Comprehensive POSIX filesystem tests
- [nfstest](https://nfstest.readthedocs.io/) - Network filesystem testing
- [FIO](https://fio.readthedocs.io/) - Flexible I/O tester

### SeaweedFS Documentation
- [SeaweedFS Wiki](https://github.com/seaweedfs/seaweedfs/wiki)
- [FUSE Mount Documentation](https://github.com/seaweedfs/seaweedfs/wiki/FUSE-Mount)
- [Performance Tuning Guide](https://github.com/seaweedfs/seaweedfs/wiki/Optimization)

## 🤝 Contributing

### Adding New Tests

1. **Basic Tests**: Add to `posix_compliance_test.go`
2. **Extended Tests**: Add to `posix_extended_test.go`
3. **External Integration**: Add to `posix_external_test.go`

### Test Guidelines

```go
func (s *POSIXTestSuite) TestNewFeature(t *testing.T) {
    mountPoint := s.framework.GetMountPoint()
    
    t.Run("SubTestName", func(t *testing.T) {
        // Test setup
        testFile := filepath.Join(mountPoint, "test.txt")
        
        // Test execution
        err := someOperation(testFile)
        
        // Assertions
        require.NoError(t, err)
        
        // Cleanup (if needed)
        defer os.Remove(testFile)
    })
}
```

### Submitting Improvements

1. Fork the repository
2. Create a feature branch
3. Add comprehensive tests
4. Ensure all existing tests pass
5. Submit a pull request with detailed description

## 📞 Support

For questions, issues, or contributions:

- **GitHub Issues**: [SeaweedFS Issues](https://github.com/seaweedfs/seaweedfs/issues)
- **Discussions**: [SeaweedFS Discussions](https://github.com/seaweedfs/seaweedfs/discussions)
- **Documentation**: [SeaweedFS Wiki](https://github.com/seaweedfs/seaweedfs/wiki)

---

*This comprehensive POSIX compliance test suite ensures SeaweedFS FUSE mounts meet industry standards for filesystem behavior, performance, and reliability.*
