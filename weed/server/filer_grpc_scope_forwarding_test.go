package weed_server

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func sharedWriteRing(c *twoFilers) {
	servers := []pb.ServerAddress{c.addrA, c.addrB}
	setRing(c.fsA, 0, servers)
	setRing(c.fsB, 0, servers)
}

func scopedForwardCreateRequest(p util.FullPath) *filer_pb.CreateEntryRequest {
	dir, name := p.DirAndName()
	return &filer_pb.CreateEntryRequest{
		Directory:                dir,
		OExcl:                    true,
		SkipCheckParentDirectory: true,
		Entry: &filer_pb.Entry{
			Name: name,
			Attributes: &filer_pb.FuseAttributes{
				FileMode: 0644,
			},
		},
	}
}

func scopedForwardUpdateRequest(p util.FullPath, expected *filer_pb.Entry, value string) *filer_pb.UpdateEntryRequest {
	dir, _ := p.DirAndName()
	next := proto.Clone(expected).(*filer_pb.Entry)
	next.Extended = map[string][]byte{"v": []byte(value)}
	return &filer_pb.UpdateEntryRequest{
		Directory:        dir,
		Entry:            next,
		ExpectedExtended: map[string][]byte{"v": expected.Extended["v"]},
	}
}

func TestScopedConditionalWritesRefuseRemoteOwner(t *testing.T) {
	for _, tc := range []struct {
		name  string
		call  func(*twoFilers, util.FullPath) error
		check func(*testing.T, *twoFilers, util.FullPath)
	}{
		{
			name: "create",
			call: func(c *twoFilers, p util.FullPath) error {
				req := scopedForwardCreateRequest(p)
				_, err := c.fsA.CreateEntry(withScopeKeepOnly(context.Background()), req)
				if req.IsMoved {
					return status.Error(codes.Internal, "scoped create marked itself moved before refusal")
				}
				return err
			},
			check: func(t *testing.T, c *twoFilers, p util.FullPath) {
				if _, err := c.store.FindEntry(context.Background(), p); err == nil {
					t.Fatal("refused scoped create changed the shared store")
				}
				if updates, _ := c.callsB.counts(); updates != 0 {
					t.Fatalf("refused scoped create reached the owner %d time(s)", updates)
				}
			},
		},
		{
			name: "update",
			call: func(c *twoFilers, p util.FullPath) error {
				expected := c.seed(t, p)
				_, err := c.fsA.UpdateEntry(withScopeKeepOnly(context.Background()), scopedForwardUpdateRequest(p, expected, "changed"))
				return err
			},
			check: func(t *testing.T, c *twoFilers, p util.FullPath) {
				if got := c.stored(t, p); got != "seed" {
					t.Fatalf("refused scoped update changed the shared store to %q", got)
				}
				if updates, _ := c.callsB.counts(); updates != 0 {
					t.Fatalf("refused scoped update reached the owner %d time(s)", updates)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTwoFilers(t)
			sharedWriteRing(c)
			p := c.keyOwnedBy(t, c.addrB, c.addrB)
			err := tc.call(c, p)
			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf("scoped remote %s error = %v, want PermissionDenied", tc.name, err)
			}
			tc.check(t, c, p)
		})
	}
}

func TestScopedConditionalWritesApplyOnLocalOwner(t *testing.T) {
	for _, tc := range []struct {
		name  string
		call  func(*twoFilers, util.FullPath) error
		check func(*testing.T, *twoFilers, util.FullPath)
	}{
		{
			name: "create",
			call: func(c *twoFilers, p util.FullPath) error {
				_, err := c.fsA.CreateEntry(withScopeKeepOnly(context.Background()), scopedForwardCreateRequest(p))
				return err
			},
			check: func(t *testing.T, c *twoFilers, p util.FullPath) {
				if _, err := c.store.FindEntry(context.Background(), p); err != nil {
					t.Fatalf("scoped local create was not stored: %v", err)
				}
			},
		},
		{
			name: "update",
			call: func(c *twoFilers, p util.FullPath) error {
				expected := c.seed(t, p)
				_, err := c.fsA.UpdateEntry(withScopeKeepOnly(context.Background()), scopedForwardUpdateRequest(p, expected, "changed"))
				return err
			},
			check: func(t *testing.T, c *twoFilers, p util.FullPath) {
				if got := c.stored(t, p); got != "changed" {
					t.Fatalf("scoped local update stored %q, want changed", got)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTwoFilers(t)
			sharedWriteRing(c)
			p := c.keyOwnedBy(t, c.addrA, c.addrA)
			if err := tc.call(c, p); err != nil {
				t.Fatalf("scoped local %s: %v", tc.name, err)
			}
			tc.check(t, c, p)
			if updates, _ := c.callsB.counts(); updates != 0 {
				t.Fatalf("scoped local %s reached the other filer %d time(s)", tc.name, updates)
			}
		})
	}
}

func TestUnscopedConditionalUpdateStillForwards(t *testing.T) {
	c := newTwoFilers(t)
	sharedWriteRing(c)
	p := c.keyOwnedBy(t, c.addrB, c.addrB)
	expected := c.seed(t, p)

	if err := c.update(c.addrA, p, expected, "forwarded"); err != nil {
		t.Fatalf("unscoped conditional update: %v", err)
	}
	if got := c.stored(t, p); got != "forwarded" {
		t.Fatalf("forwarded update stored %q, want forwarded", got)
	}
	if updates, moved := c.callsB.counts(); updates != 1 || moved != 1 {
		t.Fatalf("owner received %d updates (%d moved), want one forwarded update", updates, moved)
	}
}
