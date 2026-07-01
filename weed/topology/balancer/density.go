package balancer

// zeroVolumeDensityWeight is the ratio given to a server holding no data, so it
// still ranks below any loaded server (which has ratio >= 1) but is distinguished
// from a server with unknown capacity.
const zeroVolumeDensityWeight = 0.5

// UsedVolumeEquivalents converts the bytes a server holds into whole-volume
// equivalents (volumeBytes / volumeSizeLimit). Returns 0 when the limit is unset.
func UsedVolumeEquivalents(volumeBytes, volumeSizeLimitBytes uint64) uint64 {
	if volumeSizeLimitBytes == 0 {
		return 0
	}
	return volumeBytes / volumeSizeLimitBytes
}

// VolumeDensity is the bytes-aware capacity view balancing ranks servers by:
// used = volumeBytes / volumeSizeLimit, capacity = maxVolumeCount - used. Using
// real data (not just volume count) means an over-configured maxVolumeCount can't
// make a byte-full server look empty. capacity may be <= 0 for a server already
// past its configured slots.
func VolumeDensity(maxVolumeCount int64, volumeBytes, volumeSizeLimitBytes uint64) (capacity float64, usedVolumes uint64) {
	usedVolumes = UsedVolumeEquivalents(volumeBytes, volumeSizeLimitBytes)
	return float64(maxVolumeCount - int64(usedVolumes)), usedVolumes
}

// DensityRatio is a server's load: usedVolumes / capacity, higher = fuller. An
// empty server gets a small positive ratio so it sorts below loaded servers.
// Returns 0 when capacity is 0 (no room / unknown).
func DensityRatio(capacity float64, usedVolumes uint64) float64 {
	if capacity == 0 {
		return 0
	}
	if usedVolumes == 0 {
		return zeroVolumeDensityWeight / capacity
	}
	return float64(usedVolumes) / capacity
}

// DensityNextRatio is a server's load after one more volume lands on it:
// (usedVolumes+1) / capacity. Used to test whether a move would overshoot.
func DensityNextRatio(capacity float64, usedVolumes uint64) float64 {
	if capacity == 0 {
		return 0
	}
	return float64(usedVolumes+1) / capacity
}
