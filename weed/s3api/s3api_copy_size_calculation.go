package s3api

import (
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// CopySizeCalculator handles size calculations for different copy scenarios
type CopySizeCalculator struct {
	srcSize      int64
	srcEncrypted bool
	dstEncrypted bool
	srcType      EncryptionType
	dstType      EncryptionType
	isCompressed bool
}

// EncryptionType represents different encryption types
type EncryptionType int

const (
	EncryptionTypeNone EncryptionType = iota
	EncryptionTypeSSEC
	EncryptionTypeSSEKMS
	EncryptionTypeSSES3
)

// NewCopySizeCalculator creates a new size calculator for copy operations
func NewCopySizeCalculator(entry *filer_pb.Entry, r *http.Request) *CopySizeCalculator {
	calc := &CopySizeCalculator{
		srcSize:      int64(entry.Attributes.FileSize),
		isCompressed: isCompressedEntry(entry),
	}

	// Determine source encryption type
	calc.srcType, calc.srcEncrypted = getSourceEncryptionType(entry.Extended)

	// Determine destination encryption type
	calc.dstType, calc.dstEncrypted = getDestinationEncryptionType(r)

	return calc
}

// CalculateTargetSize calculates the expected size of the target object
func (calc *CopySizeCalculator) CalculateTargetSize() int64 {
	// For compressed objects, size calculation is complex
	if calc.isCompressed {
		return -1 // Indicates unknown size
	}

	switch {
	case !calc.srcEncrypted && !calc.dstEncrypted:
		// Plain → Plain: no size change
		return calc.srcSize

	case !calc.srcEncrypted && calc.dstEncrypted:
		// Plain → Encrypted: no overhead since IV is in metadata
		return calc.srcSize

	case calc.srcEncrypted && !calc.dstEncrypted:
		// Encrypted → Plain: no overhead since IV is in metadata
		return calc.srcSize

	case calc.srcEncrypted && calc.dstEncrypted:
		// Encrypted → Encrypted: no overhead since IV is in metadata
		return calc.srcSize

	default:
		return calc.srcSize
	}
}

// CalculateActualSize calculates the actual unencrypted size of the content
func (calc *CopySizeCalculator) CalculateActualSize() int64 {
	// With IV in metadata, encrypted and unencrypted sizes are the same
	return calc.srcSize
}

// CalculateEncryptedSize calculates the encrypted size for the given encryption type
func (calc *CopySizeCalculator) CalculateEncryptedSize(encType EncryptionType) int64 {
	// With IV in metadata, encrypted size equals actual size
	return calc.CalculateActualSize()
}

// getSourceEncryptionType determines the encryption type of the source object
func getSourceEncryptionType(metadata map[string][]byte) (EncryptionType, bool) {
	if IsSSECEncrypted(metadata) {
		return EncryptionTypeSSEC, true
	}
	if IsSSEKMSEncrypted(metadata) {
		return EncryptionTypeSSEKMS, true
	}
	if IsSSES3EncryptedInternal(metadata) {
		return EncryptionTypeSSES3, true
	}
	return EncryptionTypeNone, false
}

// getDestinationEncryptionType determines the encryption type for the destination
func getDestinationEncryptionType(r *http.Request) (EncryptionType, bool) {
	if IsSSECRequest(r) {
		return EncryptionTypeSSEC, true
	}
	if IsSSEKMSRequest(r) {
		return EncryptionTypeSSEKMS, true
	}
	if IsSSES3RequestInternal(r) {
		return EncryptionTypeSSES3, true
	}
	return EncryptionTypeNone, false
}

// isCompressedEntry checks if the entry represents a compressed object
func isCompressedEntry(entry *filer_pb.Entry) bool {
	// Check for compression indicators in metadata
	if compressionType, exists := entry.Extended["compression"]; exists {
		return string(compressionType) != ""
	}

	// Check MIME type for compressed formats
	mimeType := entry.Attributes.Mime
	compressedMimeTypes := []string{
		"application/gzip",
		"application/x-gzip",
		"application/zip",
		"application/x-compress",
		"application/x-compressed",
	}

	for _, compressedType := range compressedMimeTypes {
		if mimeType == compressedType {
			return true
		}
	}

	return false
}

// SizeTransitionInfo provides detailed information about size changes during copy
type SizeTransitionInfo struct {
	SourceSize     int64
	TargetSize     int64
	ActualSize     int64
	SizeChange     int64
	SourceType     EncryptionType
	TargetType     EncryptionType
	IsCompressed   bool
	RequiresResize bool
}

// GetSizeTransitionInfo returns detailed size transition information
func (calc *CopySizeCalculator) GetSizeTransitionInfo() *SizeTransitionInfo {
	targetSize := calc.CalculateTargetSize()
	actualSize := calc.CalculateActualSize()

	info := &SizeTransitionInfo{
		SourceSize:     calc.srcSize,
		TargetSize:     targetSize,
		ActualSize:     actualSize,
		SizeChange:     targetSize - calc.srcSize,
		SourceType:     calc.srcType,
		TargetType:     calc.dstType,
		IsCompressed:   calc.isCompressed,
		RequiresResize: targetSize != calc.srcSize,
	}

	return info
}

// String returns a string representation of the encryption type
func (e EncryptionType) String() string {
	switch e {
	case EncryptionTypeNone:
		return "None"
	case EncryptionTypeSSEC:
		return s3_constants.SSETypeC
	case EncryptionTypeSSEKMS:
		return s3_constants.SSETypeKMS
	case EncryptionTypeSSES3:
		return s3_constants.SSETypeS3
	default:
		return "Unknown"
	}
}

// OptimizedSizeCalculation provides size calculations optimized for different scenarios
type OptimizedSizeCalculation struct {
	Strategy           UnifiedCopyStrategy
	SourceSize         int64
	TargetSize         int64
	ActualContentSize  int64
	EncryptionOverhead int64
	CanPreallocate     bool
	RequiresStreaming  bool
}

// CalculateOptimizedSizes calculates sizes optimized for the copy strategy
func CalculateOptimizedSizes(entry *filer_pb.Entry, r *http.Request, strategy UnifiedCopyStrategy) *OptimizedSizeCalculation {
	calc := NewCopySizeCalculator(entry, r)
	info := calc.GetSizeTransitionInfo()

	result := &OptimizedSizeCalculation{
		Strategy:          strategy,
		SourceSize:        info.SourceSize,
		TargetSize:        info.TargetSize,
		ActualContentSize: info.ActualSize,
		CanPreallocate:    !info.IsCompressed && info.TargetSize > 0,
		RequiresStreaming: info.IsCompressed || info.TargetSize < 0,
	}

	// Calculate encryption overhead for the target
	// With IV in metadata, all encryption overhead is 0
	result.EncryptionOverhead = 0

	// Adjust based on strategy
	switch strategy {
	case CopyStrategyDirect:
		// Direct copy: no size change
		result.TargetSize = result.SourceSize
		result.CanPreallocate = true

	case CopyStrategyKeyRotation:
		// Key rotation: size might change slightly due to different IVs
		if info.SourceType == EncryptionTypeSSEC && info.TargetType == EncryptionTypeSSEC {
			// SSE-C key rotation: same overhead
			result.TargetSize = result.SourceSize
		}
		result.CanPreallocate = true

	case CopyStrategyEncrypt, CopyStrategyDecrypt, CopyStrategyReencrypt:
		// Size changes based on encryption transition
		result.TargetSize = info.TargetSize
		result.CanPreallocate = !info.IsCompressed
	}

	return result
}
