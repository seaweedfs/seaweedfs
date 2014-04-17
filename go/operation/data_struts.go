package operation

import ()

type JoinResult struct {
	VolumeSizeLimit uint64 `json:"VolumeSizeLimit,omitempty"`
	Error           string `json:"error,omitempty"`
}
