package s3_constants

// Cryptographic constants
const (
	// AES block and key sizes
	AESBlockSize = 16 // 128 bits for AES block size (IV length)
	AESKeySize   = 32 // 256 bits for AES-256 keys

	// SSE algorithm identifiers
	SSEAlgorithmAES256 = "AES256"
	SSEAlgorithmKMS    = "aws:kms"

	// SSE type identifiers for response headers and internal processing
	SSETypeC   = "SSE-C"
	SSETypeKMS = "SSE-KMS"
	SSETypeS3  = "SSE-S3"

	// S3 multipart upload limits and offsets
	S3MaxPartSize = 5 * 1024 * 1024 * 1024 // 5GB - AWS S3 maximum part size limit

	// Multipart offset calculation for unique IV generation
	// Using 8GB offset between parts (larger than max part size) to prevent IV collisions
	// Critical for CTR mode encryption security in multipart uploads
	PartOffsetMultiplier = int64(1) << 33 // 8GB per part offset
)
