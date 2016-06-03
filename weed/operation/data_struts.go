package operation

type JoinResult struct {
	VolumeSizeLimit uint64 `json:"VolumeSizeLimit,omitempty"`
	SecretKey       string `json:"secretKey,omitempty"`
	Error           string `json:"error,omitempty"`
}
