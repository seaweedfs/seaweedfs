package weed_server

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

type scopeCountingBackend struct {
	scopeBackend
	lookups int
}

func (b *scopeCountingBackend) scopeFindEntry(ctx context.Context, path string) (*filer_pb.Entry, error) {
	b.lookups++
	return b.scopeBackend.scopeFindEntry(ctx, path)
}

func TestScopedChunklessAdmissionSkipsStoreLookup(t *testing.T) {
	for _, tc := range []struct {
		name   string
		method string
		entry  *filer_pb.Entry
	}{
		{name: "create", method: "CreateEntry", entry: &filer_pb.Entry{Name: "empty"}},
		{name: "mkdir", method: "CreateEntry", entry: &filer_pb.Entry{Name: "directory", IsDirectory: true}},
		{name: "metadata update", method: "UpdateEntry", entry: &filer_pb.Entry{Name: "metadata", Extended: map[string][]byte{"x-custom": []byte("value")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, scope := newScopeRetentionServer(t, newRenameTestStore())
			backend := &scopeCountingBackend{scopeBackend: scope.backend}
			scope.backend = backend
			var req any
			if tc.method == "CreateEntry" {
				req = &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: tc.entry, SkipCheckParentDirectory: true}
			} else {
				req = &filer_pb.UpdateEntryRequest{Directory: aliceWS, Entry: tc.entry}
			}
			called := false
			_, err := scope.unary(scopedRetentionContext(), req, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + tc.method}, func(context.Context, any) (any, error) {
				called = true
				return nil, nil
			})
			if err != nil || !called {
				t.Fatalf("admission error=%v called=%v, want admitted request", err, called)
			}
			if backend.lookups != 0 {
				t.Fatalf("admission store lookups=%d, want zero", backend.lookups)
			}
		})
	}
}
