package weed_server

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestScopeDeleteRefusesForwardingAndAllowsOwnerLocal(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		scoped, remote, denied bool
	}{
		{name: "scoped remote owner", scoped: true, remote: true, denied: true},
		{name: "scoped local owner", scoped: true},
		{name: "unscoped remote owner", remote: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTwoFilers(t)
			sharedWriteRing(c)
			owner := c.addrA
			if tc.remote {
				owner = c.addrB
			}
			p := c.keyOwnedBy(t, owner, owner)
			expected := c.seed(t, p)
			dir, name := p.DirAndName()
			req := &filer_pb.DeleteEntryRequest{
				Directory: dir, Name: name,
				ExpectedExtended: map[string][]byte{"v": expected.Extended["v"]},
			}
			ctx := context.Background()
			if tc.scoped {
				ctx = withScopeKeepOnly(ctx)
			}
			resp, err := c.fsA.DeleteEntry(ctx, req)
			if tc.denied {
				if status.Code(err) != codes.PermissionDenied || req.IsMoved {
					t.Fatalf("forwarding error=%v moved=%v, want refusal before forwarding", err, req.IsMoved)
				}
				if got := c.stored(t, p); got != "seed" {
					t.Fatalf("refused delete changed stored entry to %q", got)
				}
				return
			}
			if err != nil || resp.GetError() != "" {
				t.Fatalf("delete response=%v error=%v", resp, err)
			}
			if _, err := c.store.FindEntry(ctx, p); err != filer_pb.ErrNotFound {
				t.Fatalf("entry after allowed delete: error=%v, want not found", err)
			}
		})
	}
}
