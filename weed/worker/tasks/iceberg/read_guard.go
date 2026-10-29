package iceberg

import (
	"fmt"
	"runtime/debug"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// recoverRead turns a panic into *errp, naming what was being read (a file,
// or a table operation).
// parquet-go panics on more than a file it cannot open: a corrupt page under
// a valid footer panics in its row decoder. Nothing above a maintenance job
// recovers, so such a panic would end the process hosting the worker; with
// this the job fails for that file instead. Use as
// `defer recoverRead(&err, what)` in a function with a named error result.
func recoverRead(errp *error, what string) {
	if r := recover(); r != nil {
		glog.Errorf("iceberg maintenance: recovered panic in %s: %v\n%s", what, r, debug.Stack())
		*errp = fmt.Errorf("%s: panic: %v", what, r)
	}
}

// guardRead runs fn, turning a panic into an error naming what.
func guardRead[T any](what string, fn func() (T, error)) (v T, err error) {
	defer recoverRead(&err, what)
	return fn()
}
