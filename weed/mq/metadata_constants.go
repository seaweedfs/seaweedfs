package mq

// Extended attribute keys for SeaweedMQ file metadata
// These constants are used across different packages (broker, logstore, kafka, query)
const (
	// Timestamp range metadata
	ExtendedAttrTimestampMin = "ts_min" // 8-byte binary (BigEndian) minimum timestamp in nanoseconds
	ExtendedAttrTimestampMax = "ts_max" // 8-byte binary (BigEndian) maximum timestamp in nanoseconds

	// Offset range metadata for Kafka integration
	ExtendedAttrOffsetMin = "offset_min" // 8-byte binary (BigEndian) minimum Kafka offset
	ExtendedAttrOffsetMax = "offset_max" // 8-byte binary (BigEndian) maximum Kafka offset

	// Buffer tracking metadata
	ExtendedAttrBufferStart = "buffer_start" // 8-byte binary (BigEndian) buffer start index

	// Source file tracking for parquet deduplication
	ExtendedAttrSources = "sources" // JSON-encoded list of source log files
)
