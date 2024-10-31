package stats

// This file contains metric names for all errors
// The naming convention is ErrorSomeThing = "error.some.thing"
const (
	// volume server
	WriteToLocalDisk            = "writeToLocalDisk"
	WriteToReplicas             = "writeToReplicas"
	ErrorSizeMismatchOffsetSize = "errorSizeMismatchOffsetSize"
	ErrorSizeMismatch           = "errorSizeMismatch"
	ErrorCRC                    = "errorCRC"
	ErrorIndexOutOfRange        = "errorIndexOutOfRange"
	ErrorGetNotFound            = "errorGetNotFound"
	ErrorGetInternal            = "errorGetInternal"

	// master topology
	ErrorWriteToLocalDisk = "errorWriteToLocalDisk"
	ErrorUnmarshalPairs   = "errorUnmarshalPairs"
	ErrorWriteToReplicas  = "errorWriteToReplicas"

	// master client
	FailedToKeepConnected = "failedToKeepConnected"
	FailedToSend          = "failedToSend"
	FailedToReceive       = "failedToReceive"
	RedirectedToLeader    = "redirectedToLeader"
	OnPeerUpdate          = "onPeerUpdate"
	Failed                = "failed"

	// filer handler
	DirList            = "dirList"
	ContentSaveToFiler = "contentSaveToFiler"
	AutoChunk          = "autoChunk"
	ChunkProxy         = "chunkProxy"
	ChunkAssign        = "chunkAssign"
	ChunkUpload        = "chunkUpload"
	ChunkMerge         = "chunkMerge"

	ChunkDoUploadRetry       = "chunkDoUploadRetry"
	ChunkUploadRetry         = "chunkUploadRetry"
	ChunkAssignRetry         = "chunkAssignRetry"
	ErrorReadNotFound        = "read.notfound"
	ErrorReadInternal        = "read.internal.error"
	ErrorWriteEntry          = "write.entry.failed"
	RepeatErrorUploadContent = "upload.content.repeat.failed"
	ErrorChunkAssign         = "chunkAssign.failed"
	ErrorReadChunk           = "read.chunk.failed"
	ErrorReadCache           = "read.cache.failed"
	ErrorReadStream          = "read.stream.failed"

	// s3 handler
	ErrorCompletedNoSuchUpload      = "errorCompletedNoSuchUpload"
	ErrorCompleteEntityTooSmall     = "errorCompleteEntityTooSmall"
	ErrorCompletedPartEmpty         = "errorCompletedPartEmpty"
	ErrorCompletedPartNumber        = "errorCompletedPartNumber"
	ErrorCompletedPartNotFound      = "errorCompletedPartNotFound"
	ErrorCompletedEtagInvalid       = "errorCompletedEtagInvalid"
	ErrorCompletedEtagMismatch      = "errorCompletedEtagMismatch"
	ErrorCompletedPartEntryMismatch = "errorCompletedPartEntryMismatch"
)
