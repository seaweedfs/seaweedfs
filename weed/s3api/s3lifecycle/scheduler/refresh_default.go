//go:build !s3tests

package scheduler

import "time"

const defaultRefreshInterval = 5 * time.Minute
