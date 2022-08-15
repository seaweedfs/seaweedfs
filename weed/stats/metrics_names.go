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
	DirList                  = "dirList"
	ContentSaveToFiler       = "contentSaveToFiler"
	AutoChunk                = "autoChunk"
	ChunkProxy               = "chunkProxy"
	ChunkAssign              = "chunkAssign"
	ChunkUpload              = "chunkUpload"
	ChunkDoUploadRetry       = "chunkDoUploadRetry"
	ChunkUploadRetry         = "chunkUploadRetry"
	ChunkAssignRetry         = "chunkAssignRetry"
	ErrorReadNotFound        = "read.notfound"
	ErrorReadInternal        = "read.internal.error"
	ErrorWriteEntry          = "write.entry.failed"
	RepeatErrorUploadContent = "upload.content.repeat.failed"
	ErrorReadCache           = "read.cache.failed"
	ErrorReadStream          = "read.stream.failed"
)
