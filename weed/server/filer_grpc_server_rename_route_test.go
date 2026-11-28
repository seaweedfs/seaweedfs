package weed_server

import (
	"context"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
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

// A missing source is ENOENT to the client, whether the miss is this
// filer's or the owner's relayed over gRPC.
func TestRenameErrnoMissingSource(t *testing.T) {
	c := newTwoFilers(t)
	separateStores(c)
	sharedWriteRing(c)
	for _, p := range []string{string(c.keyOwnedBy(t, c.addrA, c.addrA)), string(c.keyOwnedBy(t, c.addrB, c.addrB))} {
		dir, name := util.FullPath(p).DirAndName()
		err := c.fsA.StreamRenameEntry(&filer_pb.StreamRenameEntryRequest{
			OldDirectory: dir, OldName: name, NewDirectory: dir, NewName: name + ".renamed",
		}, &renameTestStream{})
		if got := renameErrno(err); got != int32(syscall.ENOENT) {
			t.Fatalf("rename of absent %s: errno %d for %v, want ENOENT", p, got, err)
		}
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

// A forwarded rename is answered only once this filer has replayed the
// owner's events through the rename: the target exists on the owner alone
// until then, and an unlink of a name this filer owns would find nothing.
func TestForwardedRenameWaitsForOwnerEvents(t *testing.T) {
	for _, tc := range []struct {
		name   string
		rename func(c *twoFilers, dir, name string) error
	}{
		{"stream", func(c *twoFilers, dir, name string) error {
			stream := &renameTestStream{}
			err := c.fsA.StreamRenameEntry(&filer_pb.StreamRenameEntryRequest{
				OldDirectory: dir, OldName: name, NewDirectory: dir, NewName: name + ".renamed",
			}, stream)
			for _, resp := range stream.sent {
				if resp.EventNotification == nil {
					t.Errorf("the owner's end-of-rename position reached the client: %v", resp)
				}
			}
			return err
		}},
		{"atomic", func(c *twoFilers, dir, name string) error {
			resp, err := c.fsA.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
				OldDirectory: dir, OldName: name, NewDirectory: dir, NewName: name + ".renamed",
			})
			if resp.GetTsNs() != 0 {
				t.Errorf("the owner's log position reached the client: %d", resp.GetTsNs())
			}
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTwoFilers(t)
			storeB := separateStores(c)
			sharedWriteRing(c)
			ma := filer.NewMetaAggregator(c.fsA.filer, c.addrA, c.dial)
			c.fsA.filer.MetaAggregator = ma
			t.Cleanup(ma.Shutdown)
			ma.TrackPeerForTesting(c.addrB)

			p := c.keyOwnedBy(t, c.addrB, c.addrB)
			dir, name := p.DirAndName()
			create := &filer_pb.CreateEntryRequest{Directory: dir, OExcl: true, Entry: &filer_pb.Entry{
				Name: name, Attributes: &filer_pb.FuseAttributes{Mtime: 1700000000, FileMode: 0644},
			}}
			if resp, err := c.fsA.CreateEntry(context.Background(), create); err != nil || resp.GetError() != "" {
				t.Fatalf("create via A: resp=%v err=%v", resp, err)
			}
			createdTsNs := c.fsB.filer.LocalMetaLogBuffer.LastTsNs.Load()
			ma.ReportPeerWatermarksForTesting(c.addrB, createdTsNs, 0)

			done := make(chan error, 1)
			go func() { done <- tc.rename(c, dir, name) }()
			select {
			case err := <-done:
				t.Fatalf("rename answered before A replayed the owner's rename events: %v", err)
			case <-time.After(300 * time.Millisecond):
			}
			if _, err := storeB.FindEntry(context.Background(), p+".renamed"); err != nil {
				t.Fatalf("the owner has no renamed entry: %v", err)
			}

			ma.ReportPeerWatermarksForTesting(c.addrB, c.fsB.filer.LocalMetaLogBuffer.LastTsNs.Load(), 0)
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("rename via A: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("rename still waiting after A replayed the owner's events")
			}
		})
	}
}

// An owner this filer no longer follows, its stream down long enough to be
// dropped, has replayed nothing here: the rename waits for it to come back.
func TestForwardedRenameWaitsForUntrackedOwner(t *testing.T) {
	c := newTwoFilers(t)
	storeB := separateStores(c)
	sharedWriteRing(c)
	ma := filer.NewMetaAggregator(c.fsA.filer, c.addrA, c.dial)
	c.fsA.filer.MetaAggregator = ma
	t.Cleanup(ma.Shutdown)

	p := c.keyOwnedBy(t, c.addrB, c.addrB)
	dir, name := p.DirAndName()
	create := &filer_pb.CreateEntryRequest{Directory: dir, OExcl: true, Entry: &filer_pb.Entry{
		Name: name, Attributes: &filer_pb.FuseAttributes{Mtime: 1700000000, FileMode: 0644},
	}}
	if resp, err := c.fsA.CreateEntry(context.Background(), create); err != nil || resp.GetError() != "" {
		t.Fatalf("create via A: resp=%v err=%v", resp, err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := c.fsA.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: dir, OldName: name, NewDirectory: dir, NewName: name + ".renamed",
		})
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("rename answered while A did not follow the owner: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	if _, err := storeB.FindEntry(context.Background(), p+".renamed"); err != nil {
		t.Fatalf("the owner has no renamed entry: %v", err)
	}

	ma.TrackPeerForTesting(c.addrB)
	ma.ReportPeerWatermarksForTesting(c.addrB, c.fsB.filer.LocalMetaLogBuffer.LastTsNs.Load(), 0)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("rename via A: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("rename still waiting after A replayed the owner's events")
	}
}

// A rename that must not replace its target is refused when this filer has
// the target, even though the owner has not replayed it.
func TestForwardedAtomicRenameEntryTargetExistsHere(t *testing.T) {
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
	target := p + ".target"
	if err := c.store.InsertEntry(ctx, newFileEntry(string(target), 7)); err != nil {
		t.Fatalf("seed A's target: %v", err)
	}

	_, err := c.fsA.AtomicRenameEntry(ctx, &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: dir, OldName: name, NewDirectory: dir, NewName: target.Name(), FailIfTargetExists: true,
	})
	if err == nil || !strings.Contains(err.Error(), filer_pb.ErrEntryAlreadyExists.Error()) {
		t.Fatalf("rename over a target A has: %v", err)
	}
	if _, err := storeB.FindEntry(ctx, p); err != nil {
		t.Fatalf("a refused rename moved the source on the owner: %v", err)
	}
	if _, err := storeB.FindEntry(ctx, target); err != filer_pb.ErrNotFound {
		t.Fatalf("a refused rename wrote the target on the owner: %v", err)
	}
}
