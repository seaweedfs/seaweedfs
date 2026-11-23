package weed_server

import (
	"testing"
	"time"

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
