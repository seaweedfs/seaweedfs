// Package handlers is an aggregator that blank-imports every plugin worker
// handler subpackage so their init() functions register with the handler
// registry. Import this package instead of individual subpackages when you
// need all handlers available.
package handlers

import (
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"         // register volume_balance handler
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/ec_balance"      // register ec_balance handler
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"  // register erasure_coding handler
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/iceberg"         // register iceberg_maintenance handler
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"          // register vacuum handler
)
