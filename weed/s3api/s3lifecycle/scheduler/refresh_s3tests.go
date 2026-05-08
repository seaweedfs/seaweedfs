//go:build s3tests

package scheduler

import "time"

// Under the s3tests build tag rules are PUT and expected to fire within
// seconds, so the engine snapshot is rebuilt aggressively.
const defaultRefreshInterval = 2 * time.Second
