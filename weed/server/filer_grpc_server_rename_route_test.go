package weed_server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// A file created with IF_NOT_EXISTS lands on the write owner. A rename sent
// to another filer right after must move it there, even though that filer's
// own store has not seen the create yet.
func TestStreamRenameEntryMissReachesOwnerStore(t *testing.T) {
	c := newTwoFilers(t)
	storeB := separateStores(c)
	sharedWriteRing(c)
	p := c.keyOwnedBy(t, c.addrB, c.addrB)
	dir, name := p.DirAndName()
	ctx := context.Background()

	create := &filer_pb.CreateEntryRequest{Directory: dir, Entry: &filer_pb.Entry{
		Name: name, Attributes: &filer_pb.FuseAttributes{Mtime: 1700000000, FileMode: 0644},
	}, Condition: &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{{Kind: filer_pb.WriteCondition_IF_NOT_EXISTS}}}}
	if resp, err := c.fsA.CreateEntry(ctx, create); err != nil || resp.GetError() != "" {
		t.Fatalf("create via A: resp=%v err=%v", resp, err)
	}
	if _, err := c.store.FindEntry(ctx, p); err != filer_pb.ErrNotFound {
		t.Fatalf("A's store must not have the entry yet: %v", err)
	}

	stream := &renameTestStream{}
	err := c.fsA.StreamRenameEntry(&filer_pb.StreamRenameEntryRequest{
		OldDirectory: dir, OldName: name, NewDirectory: dir, NewName: name + ".renamed",
	}, stream)
	if err != nil {
		t.Fatalf("rename via A: %v", err)
	}
	if _, err := storeB.FindEntry(ctx, p); err != filer_pb.ErrNotFound {
		t.Fatalf("the owner kept the source after the rename: %v", err)
	}
	if _, err := storeB.FindEntry(ctx, p+".renamed"); err != nil {
		t.Fatalf("the owner has no renamed entry: %v", err)
	}
	if len(stream.sent) == 0 {
		t.Fatal("the owner's rename events were not relayed to the caller")
	}
}

func TestAtomicRenameEntryMissReachesOwnerStore(t *testing.T) {
	c := newTwoFilers(t)
	storeB := separateStores(c)
	sharedWriteRing(c)
	p := c.keyOwnedBy(t, c.addrB, c.addrB)
	dir, name := p.DirAndName()
	ctx := context.Background()

	create := &filer_pb.CreateEntryRequest{Directory: dir, OExcl: true, Entry: &filer_pb.Entry{
		Name: name, Attributes: &filer_pb.FuseAttributes{Mtime: 1700000000, FileMode: 0644},
	}}
	if resp, err := c.fsA.CreateEntry(ctx, create); err != nil || resp.GetError() != "" {
		t.Fatalf("create via A: resp=%v err=%v", resp, err)
	}
	if _, err := c.fsA.AtomicRenameEntry(ctx, &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: dir, OldName: name, NewDirectory: dir, NewName: name + ".renamed",
	}); err != nil {
		t.Fatalf("rename via A: %v", err)
	}
	if _, err := storeB.FindEntry(ctx, p); err != filer_pb.ErrNotFound {
		t.Fatalf("the owner kept the source after the rename: %v", err)
	}
	if _, err := storeB.FindEntry(ctx, p+".renamed"); err != nil {
		t.Fatalf("the owner has no renamed entry: %v", err)
	}
}

// A source missing on the owner too is still reported as not found.
func TestStreamRenameEntryMissOnOwnerNotFound(t *testing.T) {
	c := newTwoFilers(t)
	separateStores(c)
	sharedWriteRing(c)
	p := c.keyOwnedBy(t, c.addrB, c.addrB)
	dir, name := p.DirAndName()

	err := c.fsA.StreamRenameEntry(&filer_pb.StreamRenameEntryRequest{
		OldDirectory: dir, OldName: name, NewDirectory: dir, NewName: name + ".renamed",
	}, &renameTestStream{})
	if err == nil || !strings.Contains(err.Error(), filer_pb.ErrNotFound.Error()) {
		t.Fatalf("rename of an absent entry: %v", err)
	}
}

// While B's ring still gives the path to A, B refuses the rename A forwards
// rather than answer for a store the creates may not have reached.
func TestForwardedStreamRenameEntryRefusedByNonOwner(t *testing.T) {
	slack := notWriteOwnerRetrySlack
	notWriteOwnerRetrySlack = 200 * time.Millisecond
	t.Cleanup(func() { notWriteOwnerRetrySlack = slack })

	c := newTwoFilers(t)
	storeB := separateStores(c)
	setRing(c.fsA, 0, []pb.ServerAddress{c.addrA, c.addrB})
	setRing(c.fsB, time.Minute, []pb.ServerAddress{c.addrA}, []pb.ServerAddress{c.addrA, c.addrB})
	p := c.keyOwnedBy(t, c.addrB, c.addrA)
	if err := storeB.InsertEntry(context.Background(), newFileEntry(string(p), 7)); err != nil {
		t.Fatalf("seed B: %v", err)
	}

	dir, name := p.DirAndName()
	err := c.fsA.StreamRenameEntry(&filer_pb.StreamRenameEntryRequest{
		OldDirectory: dir, OldName: name, NewDirectory: dir, NewName: name + ".renamed",
	}, &renameTestStream{})
	if !isNotWriteOwner(err) {
		t.Fatalf("B applied or failed otherwise a rename its ring gives to A: %v", err)
	}
	if _, err := storeB.FindEntry(context.Background(), p); err != nil {
		t.Fatalf("a refused forward moved the entry: %v", err)
	}
}
