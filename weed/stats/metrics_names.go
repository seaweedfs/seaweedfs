package stats

// This file contains metric names for all errors
// The naming convention is ErrorSomeThing = "error.some.thing"
const (
	// volume server
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
	RedirectedToleader    = "redirectedToleader"
	OnPeerUpdate          = "onPeerUpdate"
	Failed                = "failed"

	// filer handler
	ErrorReadNotFound = "read.notfound"
	ErrorReadInternal = "read.internalerror"
	ErrorWriteEntry   = "write.entry.failed"
	ErrorReadCache    = "read.cache.failed"
	ErrorReadStream   = "read.stream.failed"
)
