//go:build !s3tests
// +build !s3tests

package util

import "time"

const LifeCycleInterval = 24 * time.Hour
