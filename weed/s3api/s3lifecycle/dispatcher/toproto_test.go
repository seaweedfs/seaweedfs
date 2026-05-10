package dispatcher

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
	"github.com/stretchr/testify/assert"
)

// toProtoActionKind / toProtoIdentity are the worker-side CAS-witness
// converters; getting them wrong silently breaks the LifecycleDelete
// flow because the server's identity check always fails. These tests
// pin the full mapping so a new ActionKind added in s3lifecycle but
// forgotten here can't slip through as ACTION_KIND_UNSPECIFIED.

func TestToProtoActionKind_AllKnownKindsMap(t *testing.T) {
	cases := []struct {
		in   s3lifecycle.ActionKind
		want s3_lifecycle_pb.ActionKind
	}{
		{s3lifecycle.ActionKindExpirationDays, s3_lifecycle_pb.ActionKind_EXPIRATION_DAYS},
		{s3lifecycle.ActionKindExpirationDate, s3_lifecycle_pb.ActionKind_EXPIRATION_DATE},
		{s3lifecycle.ActionKindNoncurrentDays, s3_lifecycle_pb.ActionKind_NONCURRENT_DAYS},
		{s3lifecycle.ActionKindNewerNoncurrent, s3_lifecycle_pb.ActionKind_NEWER_NONCURRENT},
		{s3lifecycle.ActionKindAbortMPU, s3_lifecycle_pb.ActionKind_ABORT_MPU},
		{s3lifecycle.ActionKindExpiredDeleteMarker, s3_lifecycle_pb.ActionKind_EXPIRED_DELETE_MARKER},
	}
	for _, c := range cases {
		t.Run(c.in.String(), func(t *testing.T) {
			assert.Equal(t, c.want, toProtoActionKind(c.in))
		})
	}
}

func TestToProtoActionKind_UnspecifiedAndUnknownFallToUnspecified(t *testing.T) {
	// Both the in-package zero value and a future kind not listed in
	// the switch must collapse to ACTION_KIND_UNSPECIFIED so the server
	// can reject with FATAL rather than silently dispatch a wrong kind.
	assert.Equal(t, s3_lifecycle_pb.ActionKind_ACTION_KIND_UNSPECIFIED,
		toProtoActionKind(s3lifecycle.ActionKindUnspecified))
	assert.Equal(t, s3_lifecycle_pb.ActionKind_ACTION_KIND_UNSPECIFIED,
		toProtoActionKind(s3lifecycle.ActionKind(999)))
}

func TestToProtoIdentity_NilReturnsNil(t *testing.T) {
	// LifecycleDelete treats a nil ExpectedIdentity as "no CAS"; the
	// converter must preserve that signal rather than emit an empty
	// non-nil identity that would force a re-fetch + comparison.
	assert.Nil(t, toProtoIdentity(nil))
}

func TestToProtoIdentity_AllFieldsCopied(t *testing.T) {
	in := &router.EntryIdentity{
		MtimeNs:      1700000000_000_000_123,
		Size:         4096,
		HeadFid:      "1,abc",
		ExtendedHash: []byte{0xde, 0xad, 0xbe, 0xef},
	}
	out := toProtoIdentity(in)
	if out == nil {
		t.Fatal("non-nil input must produce non-nil output")
	}
	assert.Equal(t, in.MtimeNs, out.MtimeNs)
	assert.Equal(t, in.Size, out.Size)
	assert.Equal(t, in.HeadFid, out.HeadFid)
	assert.Equal(t, in.ExtendedHash, out.ExtendedHash)
}

func TestToProtoIdentity_EmptyFieldsSurvive(t *testing.T) {
	// A bootstrap-fresh entry can land at the dispatcher with zero
	// MtimeNs / Size / no HeadFid — the converter must still produce a
	// non-nil identity so the server treats it as a real CAS witness
	// rather than the no-CAS sentinel.
	in := &router.EntryIdentity{}
	out := toProtoIdentity(in)
	if out == nil {
		t.Fatal("zero-valued identity must still produce non-nil")
	}
	assert.Zero(t, out.MtimeNs)
	assert.Zero(t, out.Size)
	assert.Empty(t, out.HeadFid)
	assert.Empty(t, out.ExtendedHash)
}
