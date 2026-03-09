// Package handlers is an aggregator that blank-imports every plugin worker
// handler subpackage so their init() functions register with the handler
// registry. Import this package instead of individual subpackages when you
// need all handlers available.
package handlers

import (
	_ "github.com/seaweedfs/seaweedfs/weed/plugin/worker/iceberg" // register iceberg_maintenance handler
)
