package shell

import (
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
)

var (
	// Default maximum parallelization/concurrency for commands supporting it.
	DefaultMaxParallelization = 10
	// Default number of volumes EC encode/decode process per batch.
	DefaultEcBatchSize = 10
	// CollectionDefault is the special keyword to match empty collection names.
	CollectionDefault = wildcard.CollectionDefault
)

// ErrorWaitGroup lives in weed/util so packages outside the shell can share it.
type ErrorWaitGroup = util.ErrorWaitGroup
type ErrorWaitGroupTask = util.ErrorWaitGroupTask

var NewErrorWaitGroup = util.NewErrorWaitGroup
var executeParallelTaskGroups = util.ExecuteParallelTaskGroups
