package replication

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
)

// CheckDigestsMatch refuses replication between filers that use different
// digests: replication copies entry digests and ETags as they are, so one side
// would hold digests of the other kind. A filer that reports none predates the
// setting and cannot be checked.
func CheckDigestsMatch(filerA, digestA, filerB, digestB string) error {
	switch {
	case digestA == "" || digestB == "":
		glog.Warningf("cannot check that filers %s and %s use the same digest: one of them does not report it", filerA, filerB)
	case digestA != digestB:
		return fmt.Errorf("filer %s uses digest %s and filer %s uses digest %s; replication would copy digests of one kind into the other", filerA, digestA, filerB, digestB)
	}
	return nil
}

// digestTarget is a sink that writes into a filer and can say which digest
// that filer uses.
type digestTarget interface {
	TargetFiler() string
	TargetDigest() (string, error)
}

// CheckSinkDigest refuses a sink that writes into a filer whose digest differs
// from the source filer's. Sinks that do not write into a filer (S3, cloud
// storage, local files) are not checked: they hold no filer digests.
func CheckSinkDigest(sourceFiler string, sourceDigest func() (string, error), dataSink sink.ReplicationSink) error {
	target, ok := dataSink.(digestTarget)
	if !ok {
		return nil
	}
	digestA, err := sourceDigest()
	if err != nil {
		return fmt.Errorf("read digest of source filer %s: %w", sourceFiler, err)
	}
	digestB, err := target.TargetDigest()
	if err != nil {
		return fmt.Errorf("read digest of target filer %s: %w", target.TargetFiler(), err)
	}
	return CheckDigestsMatch(sourceFiler, digestA, target.TargetFiler(), digestB)
}
