package util

// GatewayUploadCharge returns the bytes a gateway (S3 or filer) upload reserves
// against its in-flight upload budget.
//
// A request never holds more than its chunk buffers: at most four chunks, the
// per-request cap in the chunked uploader. So a known length is charged
// min(length, 4*chunk), not its declared size, or a multi-GB PUT would wait for
// an idle gateway. An unknown length (chunked transfer) counts as one chunk.
// When the budget limit is positive the charge is also capped at half of it
// (and at least 1 byte), as the volume server does: charged the whole limit, a
// request would only be admitted on an idle server; charged half, it is admitted
// while at most half the budget is in use. With limit 0 the charge is still
// computed, because the byte counters are kept for metrics and circuit breakers.
func GatewayUploadCharge(declared, chunk, limit int64) int64 {
	charge := min(CountedUploadSize(declared, chunk), 4*chunk)
	if limit > 0 {
		charge = min(charge, max(limit/2, 1))
	}
	return charge
}
