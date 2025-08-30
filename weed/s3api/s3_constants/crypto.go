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

	// KMS validation limits based on AWS KMS service constraints
	MaxKMSEncryptionContextPairs = 10  // Maximum number of encryption context key-value pairs
	MaxKMSKeyIDLength            = 500 // Maximum length for KMS key identifiers

	// S3 multipart upload limits based on AWS S3 service constraints
	MaxS3MultipartParts = 10000 // Maximum number of parts in a multipart upload (1-10,000)
)
