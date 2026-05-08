//go:build s3tests

package dispatcher

import "time"

// Under the s3tests build tag the engine treats one "Day" as 10 seconds
// (util.LifeCycleInterval), so the dispatcher must run far below that to
// notice a freshly-due action inside the upstream s3-tests 30s polling
// window. Production timings live in dispatch_ticks_default.go.
const (
	defaultDispatchTick   = 500 * time.Millisecond
	defaultCheckpointTick = 2 * time.Second
)
