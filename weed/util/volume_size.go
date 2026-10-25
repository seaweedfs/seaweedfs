package util

const (
	// DefaultVolumeSizeLimitMB is 30 GiB expressed in the MiB units used by
	// volumeSizeLimitMB. This also aligns the default volume size with three
	// complete 10 GiB erasure-coding data rows.
	DefaultVolumeSizeLimitMB = 30 * KiByte
	// MaxVolumeSizeLimitMB expresses VolumeSizeLimitGB in MiB: 30 GiB for
	// 4-byte offsets and 8,000 GiB for 5-byte offsets.
	MaxVolumeSizeLimitMB = VolumeSizeLimitGB * KiByte
)
