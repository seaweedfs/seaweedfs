package weed_server

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc/codes"
)

func TestScopeProtectsFilerConfAndItsAncestors(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(scopedToken("alice", time.Hour, []string{"/"}, nil))

	expectScopeDenied(t, h, "CreateEntry", "create filer.conf", func() error {
		_, err := c.CreateEntry(scopeCtx(t), &filer_pb.CreateEntryRequest{
			Directory: scopeFilerConfDir,
			Entry:     &filer_pb.Entry{Name: scopeFilerConfName},
		})
		return err
	})
	expectScopeDenied(t, h, "UpdateEntry", "update filer.conf", func() error {
		_, err := c.UpdateEntry(scopeCtx(t), &filer_pb.UpdateEntryRequest{
			Directory: scopeFilerConfDir,
			Entry:     &filer_pb.Entry{Name: scopeFilerConfName},
		})
		return err
	})
	expectScopeDenied(t, h, "AppendToEntry", "append filer.conf", func() error {
		_, err := c.AppendToEntry(scopeCtx(t), &filer_pb.AppendToEntryRequest{
			Directory: scopeFilerConfDir,
			EntryName: scopeFilerConfName,
		})
		return err
	})
	expectScopeDenied(t, h, "TouchAccessTime", "touch filer.conf", func() error {
		_, err := c.TouchAccessTime(scopeCtx(t), &filer_pb.TouchAccessTimeRequest{
			Directory: scopeFilerConfDir,
			Name:      scopeFilerConfName,
		})
		return err
	})
	expectScopeDenied(t, h, "DeleteEntry", "delete filer.conf ancestor", func() error {
		_, err := c.DeleteEntry(scopeCtx(t), &filer_pb.DeleteEntryRequest{
			Directory: "/etc",
			Name:      "seaweedfs",
		})
		return err
	})
	expectScopeDenied(t, h, "AtomicRenameEntry", "rename filer.conf ancestor", func() error {
		_, err := c.AtomicRenameEntry(scopeCtx(t), &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: "/etc",
			OldName:      "seaweedfs",
			NewDirectory: "/tmp",
			NewName:      "seaweedfs",
		})
		return err
	})
	expectScopeDenied(t, h, "AtomicRenameEntry", "rename into filer.conf descendant", func() error {
		_, err := c.AtomicRenameEntry(scopeCtx(t), &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: "/tmp",
			OldName:      "ordinary",
			NewDirectory: scopeFilerConfDir + "/" + scopeFilerConfName,
			NewName:      "child",
		})
		return err
	})

	// The configuration directory itself remains usable for ordinary metadata.
	expectScopeForwarded(t, h, "CreateEntry", "create ordinary metadata beside filer.conf", func() error {
		_, err := c.CreateEntry(scopeCtx(t), &filer_pb.CreateEntryRequest{
			Directory: scopeFilerConfDir,
			Entry:     &filer_pb.Entry{Name: "ordinary.json"},
		})
		return err
	})
}

func TestScopeStreamsProtectFilerConfDelegatedMutations(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(scopedToken("alice", time.Hour, []string{"/"}, nil))

	beforeRename := len(h.fake.received("StreamRenameEntry"))
	rename, err := c.StreamRenameEntry(scopeCtx(t), &filer_pb.StreamRenameEntryRequest{
		OldDirectory: "/etc",
		OldName:      "seaweedfs",
		NewDirectory: "/tmp",
		NewName:      "moved",
	})
	if err == nil {
		_, err = rename.Recv()
	}
	expectScopeCode(t, "stream rename filer.conf ancestor", err, codes.PermissionDenied)
	if got := len(h.fake.received("StreamRenameEntry")); got != beforeRename {
		t.Fatalf("refused stream rename reached handler: %d calls before=%d", got, beforeRename)
	}

	stream, err := c.StreamMutateEntry(scopeCtx(t))
	if err != nil {
		t.Fatalf("open mutate stream: %v", err)
	}
	if err := stream.Send(&filer_pb.StreamMutateEntryRequest{
		RequestId: 1,
		Request: &filer_pb.StreamMutateEntryRequest_CreateRequest{CreateRequest: &filer_pb.CreateEntryRequest{
			Directory: scopeFilerConfDir,
			Entry:     &filer_pb.Entry{Name: scopeFilerConfName},
		}},
	}); err != nil {
		t.Fatalf("send filer.conf create: %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive filer.conf refusal: %v", err)
	}
	if response.GetErrno() != scopeErrnoEACCES || response.GetError() == "" || !response.GetIsLast() {
		t.Fatalf("filer.conf stream refusal=%v, want EACCES", response)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close mutate stream: %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("mutate stream after close = %v, want EOF", err)
	}
	if got := len(h.fake.received("StreamMutateEntry")); got != 0 {
		t.Fatalf("refused filer.conf mutation reached handler: %d calls", got)
	}
}
