package weed_server

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestScopeUnaryRenameAndLockBoundaries(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))

	expectScopeDenied(t, h, "AtomicRenameEntry", "cross-prefix unary rename", func() error {
		_, err := c.AtomicRenameEntry(scopeCtx(t), &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: aliceWS,
			OldName:      "old",
			NewDirectory: bobWS,
			NewName:      "new",
		})
		return err
	})
	expectScopeDenied(t, h, "PosixLock", "cross-prefix unary lock", func() error {
		_, err := c.PosixLock(scopeCtx(t), &filer_pb.PosixLockRequest{Key: scopePosixLockKeyPrefix + bobWS + "/file"})
		return err
	})
}

func TestScopeRejectsMovedMutations(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))

	expectScopeDenied(t, h, "DeleteEntry", "moved delete", func() error {
		_, err := c.DeleteEntry(scopeCtx(t), &filer_pb.DeleteEntryRequest{
			Directory: aliceWS,
			Name:      "file",
			IsMoved:   true,
		})
		return err
	})
	expectScopeDenied(t, h, "PosixLock", "moved lock", func() error {
		_, err := c.PosixLock(scopeCtx(t), &filer_pb.PosixLockRequest{
			Key:     scopePosixLockKeyPrefix + aliceWS + "/file",
			IsMoved: true,
		})
		return err
	})
	expectScopeDenied(t, h, "PosixLock", "cooling-probe lock", func() error {
		_, err := c.PosixLock(scopeCtx(t), &filer_pb.PosixLockRequest{
			Key:          scopePosixLockKeyPrefix + aliceWS + "/file",
			CoolingProbe: true,
		})
		return err
	})
}

func TestScopeRejectsConditionsOnRemoteRenameSource(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))
	guess := []byte("/buckets/other/file")
	notEqual := &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{{
		Kind: filer_pb.WriteCondition_IF_EXTENDED_NOT_EQUAL, ExtKey: filer.ExtRemoteRenameSourceKey, ExtValue: string(guess),
	}}}

	expectScopeDenied(t, h, "CreateEntry", "create conditioned on the marker", func() error {
		_, err := c.CreateEntry(scopeCtx(t), &filer_pb.CreateEntryRequest{
			Directory: aliceWS, Entry: &filer_pb.Entry{Name: "file"}, Condition: notEqual,
		})
		return err
	})
	expectScopeDenied(t, h, "UpdateEntry", "update conditioned on the marker", func() error {
		_, err := c.UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{
			Directory: aliceWS, Entry: &filer_pb.Entry{Name: "file"}, Condition: notEqual,
		})
		return err
	})
	expectScopeDenied(t, h, "UpdateEntry", "update expecting the marker", func() error {
		_, err := c.UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{
			Directory: aliceWS, Entry: &filer_pb.Entry{Name: "file"},
			ExpectedExtended: map[string][]byte{filer.ExtRemoteRenameSourceKey: guess},
		})
		return err
	})
	expectScopeDenied(t, h, "DeleteEntry", "delete expecting the marker", func() error {
		_, err := c.DeleteEntry(scopeCtx(t), &filer_pb.DeleteEntryRequest{
			Directory: aliceWS, Name: "file",
			ExpectedExtended: map[string][]byte{filer.ExtRemoteRenameSourceKey: guess},
		})
		return err
	})
}
