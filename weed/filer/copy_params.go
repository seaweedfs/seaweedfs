package filer

const (
	CopyQueryParamFrom             = "cp.from"
	CopyQueryParamOverwrite        = "overwrite"
	CopyQueryParamDataOnly         = "dataOnly"
	CopyQueryParamRequestID        = "copy.requestId"
	CopyQueryParamSourceInode      = "copy.srcInode"
	CopyQueryParamSourceMtime      = "copy.srcMtime"
	CopyQueryParamSourceSize       = "copy.srcSize"
	CopyQueryParamDestinationInode = "copy.dstInode"
	CopyQueryParamDestinationMtime = "copy.dstMtime"
	CopyQueryParamDestinationSize  = "copy.dstSize"

	CopyResponseHeaderCommitted = "X-SeaweedFS-Copy-Committed"
	CopyResponseHeaderRequestID = "X-SeaweedFS-Copy-Request-ID"
)
