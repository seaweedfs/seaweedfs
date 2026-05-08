//go:build !s3tests

package dispatcher

import "time"

const (
	defaultDispatchTick   = 5 * time.Second
	defaultCheckpointTick = 30 * time.Second
)
