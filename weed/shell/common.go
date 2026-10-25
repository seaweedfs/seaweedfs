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

// resolveCollectionName maps the shared "_default" keyword to the empty-named
// collection for commands whose -collection flag names one collection rather
// than filtering by a pattern.
func resolveCollectionName(name string) string {
	if name == CollectionDefault {
		return ""
	}
	return name
}

// ErrorWaitGroup lives in weed/util so packages outside the shell can share it.
type ErrorWaitGroup = util.ErrorWaitGroup
type ErrorWaitGroupTask = util.ErrorWaitGroupTask

var NewErrorWaitGroup = util.NewErrorWaitGroup
var executeParallelTaskGroups = util.ExecuteParallelTaskGroups
