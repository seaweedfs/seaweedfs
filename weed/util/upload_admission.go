package util

// UploadAdmits reports whether a request of size bytes may start while
// inFlight bytes of uploads are already admitted under limit.
//
// A limit of 0 means unlimited. A request is always admitted when nothing
// else is in flight (inFlight == 0), even if size alone exceeds limit;
// otherwise an oversized request would never start. Otherwise the request
// is admitted only if inFlight+size stays within limit.
func UploadAdmits(inFlight, size, limit int64) bool {
	if limit == 0 || inFlight == 0 {
		return true
	}
	return inFlight+size <= limit
}

// CountedUploadSize returns the number of bytes an upload charges against
// the admission limit. A known contentLength (>= 0) is charged as-is. An
// unknown length (< 0, e.g. chunked transfer) is charged unknownCharge so
// that such requests still count toward the limit.
func CountedUploadSize(contentLength, unknownCharge int64) int64 {
	if contentLength >= 0 {
		return contentLength
	}
	return unknownCharge
}
