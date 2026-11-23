package mount

import (
	"context"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type readAuthLookupFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	fid     string
	token   string
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (f *readAuthLookupFiler) LookupDirectoryEntries(ctx context.Context, req *filer_pb.LookupDirectoryEntriesRequest) (*filer_pb.LookupDirectoryEntriesResponse, error) {
	f.calls.Add(1)
	f.entered <- struct{}{}
	select {
	case <-f.release:
		return &filer_pb.LookupDirectoryEntriesResponse{ReadAuth: map[string]string{f.fid: f.token}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// parkedTokenCallers counts goroutines blocked inside ReadAuth.Token, either
// waiting on a token lookup or running one.
func parkedTokenCallers() int {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	parked := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		header, _, _ := strings.Cut(g, "\n")
		if strings.Contains(g, "(*ReadAuth).Token(") && (strings.Contains(header, "[select") || strings.Contains(header, "WaitGroup.Wait")) {
			parked++
		}
	}
	return parked
}

// A read cancelled while its token lookup is in flight must not leave the
// other reads waiting on that lookup without a token.
func TestReadAuthCancelledCallerKeepsSharedLookup(t *testing.T) {
	fid := "3,01637037d6"
	fake := &readAuthLookupFiler{
		fid:     fid,
		token:   makeUnsignedJwtWithExp(t, time.Now().Add(time.Minute).Unix()),
		entered: make(chan struct{}, 4),
		release: make(chan struct{}),
	}
	wfs := newLookupCacheTestWFS(t, 60)
	startFakeFiler(t, wfs, fake)
	ra := newReadAuth(wfs)
	ra.Track(util.FullPath("/dir/file"), []*filer_pb.FileChunk{{FileId: fid}})

	cancelledCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelled := make(chan string, 1)
	go func() { cancelled <- ra.Token(cancelledCtx, fid) }()
	select {
	case <-fake.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("token lookup never reached the filer")
	}

	waiter := make(chan string, 1)
	go func() { waiter <- ra.Token(context.Background(), fid) }()
	deadline := time.Now().Add(10 * time.Second)
	for parkedTokenCallers() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("second reader never joined the in-flight lookup")
		}
		runtime.Gosched()
	}

	cancel()
	select {
	case got := <-cancelled:
		if got != "" {
			t.Fatalf("cancelled caller got %q, want no token", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled caller kept waiting on the lookup")
	}
	close(fake.release)

	select {
	case got := <-waiter:
		if got != fake.token {
			t.Fatalf("waiting reader got %q, want the looked-up token", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("waiting reader never got a token")
	}
	if calls := fake.calls.Load(); calls != 1 {
		t.Fatalf("filer lookups = %d, want 1 shared lookup", calls)
	}
}
