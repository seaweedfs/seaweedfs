package replication

import (
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
)

// DigestMismatchError reports two filers that use different digests. It is a
// configuration error a restart cannot fix, unlike a filer that cannot be
// reached.
type DigestMismatchError struct {
	FilerA, DigestA, FilerB, DigestB string
}

func (e *DigestMismatchError) Error() string {
	return fmt.Sprintf("filer %s uses digest %s and filer %s uses digest %s; replication would copy digests of one kind into the other", e.FilerA, e.DigestA, e.FilerB, e.DigestB)
}

// IsDigestMismatch reports whether err is or wraps a DigestMismatchError.
func IsDigestMismatch(err error) bool {
	var mismatch *DigestMismatchError
	return errors.As(err, &mismatch)
}

// CheckDigestsMatch refuses replication between filers that use different
// digests, with a DigestMismatchError: replication copies entry digests and
// ETags as they are, so one side would hold digests of the other kind. A
// filer that reports none predates the setting and cannot be checked.
func CheckDigestsMatch(filerA, digestA, filerB, digestB string) error {
	switch {
	case digestA == "" || digestB == "":
		glog.Warningf("cannot check that filers %s and %s use the same digest: one of them does not report it", filerA, filerB)
	case digestA != digestB:
		return &DigestMismatchError{FilerA: filerA, DigestA: digestA, FilerB: filerB, DigestB: digestB}
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
// from the source filer's, with a DigestMismatchError; a filer that cannot be
// reached gives an ordinary error the caller can retry. Sinks that do not
// write into a filer (S3, cloud storage, local files) are not checked: they
// hold no filer digests.
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
