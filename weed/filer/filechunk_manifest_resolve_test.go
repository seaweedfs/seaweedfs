package filer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type manifestReadFixture struct {
	server    *httptest.Server
	mu        sync.Mutex
	started   []string
	completed []string
	active    atomic.Int32
	maxActive atomic.Int32
	loads     atomic.Int32
	manifests map[string][]byte
	delays    map[string]time.Duration
	stopReads chan struct{}
}

func newManifestReadFixture(t testing.TB, manifests map[string][]*filer_pb.FileChunk, delays map[string]time.Duration) *manifestReadFixture {
	t.Helper()
	encoded := make(map[string][]byte, len(manifests))
	for id, chunks := range manifests {
		data, err := proto.Marshal(&filer_pb.FileChunkManifest{Chunks: chunks})
		require.NoError(t, err)
		encoded[id] = data
	}

	fixture := &manifestReadFixture{manifests: encoded, delays: delays, stopReads: make(chan struct{})}
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	fixture.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/")
		fixture.loads.Add(1)
		fixture.active.Add(1)
		fixture.updateMaxActive()
		fixture.mu.Lock()
		fixture.started = append(fixture.started, id)
		fixture.mu.Unlock()
		finishRead := func() {
			fixture.active.Add(-1)
			fixture.mu.Lock()
			fixture.completed = append(fixture.completed, id)
			fixture.mu.Unlock()
		}

		if delay := fixture.delays[id]; delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-r.Context().Done():
				timer.Stop()
				finishRead()
				return
			case <-fixture.stopReads:
				timer.Stop()
				finishRead()
				return
			case <-timer.C:
			}
		}
		data, ok := fixture.manifests[id]
		if !ok {
			http.NotFound(w, r)
			finishRead()
			return
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
		finishRead()
	}))
	fixture.server.Listener = listener
	fixture.server.Start()
	t.Cleanup(fixture.server.Close)
	return fixture
}

func (f *manifestReadFixture) updateMaxActive() {
	for {
		current := f.active.Load()
		maximum := f.maxActive.Load()
		if current <= maximum || f.maxActive.CompareAndSwap(maximum, current) {
			return
		}
	}
}

func (f *manifestReadFixture) lookup(ctx context.Context, fileID string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return []string{f.server.URL + "/" + fileID}, nil
}

func (f *manifestReadFixture) completionOrder() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.completed...)
}

func resolveTestManifest(id string, offset int64) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{FileId: id, IsChunkManifest: true, Offset: offset, Size: 100}
}

func resolveTestData(id string, offset int64) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{FileId: id, Offset: offset, Size: 10}
}

func TestResolveChunkManifestParallelReadsAreBounded(t *testing.T) {
	manifests := make(map[string][]*filer_pb.FileChunk)
	inputs := make([]*filer_pb.FileChunk, 0, 8)
	delays := make(map[string]time.Duration)
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("m%d", i)
		manifests[id] = []*filer_pb.FileChunk{resolveTestData(fmt.Sprintf("d%d", i), int64(i*10))}
		inputs = append(inputs, resolveTestManifest(id, int64(i*100)))
		delays[id] = 50 * time.Millisecond
	}
	fixture := newManifestReadFixture(t, manifests, delays)

	data, meta, err := ResolveChunkManifest(context.Background(), fixture.lookup, inputs, 0, 1000, nil)
	require.NoError(t, err)
	require.Len(t, data, len(inputs))
	require.Len(t, meta, len(inputs))
	require.GreaterOrEqual(t, fixture.maxActive.Load(), int32(2), "independent manifests should overlap")
	require.LessOrEqual(t, fixture.maxActive.Load(), int32(4), "manifest reads must be bounded")
}

func TestResolveChunkManifestPreservesInputOrderWhenReadsCompleteOutOfOrder(t *testing.T) {
	fixture := newManifestReadFixture(t,
		map[string][]*filer_pb.FileChunk{
			"slow": {resolveTestData("slow-data", 0)},
			"fast": {resolveTestData("fast-data", 100)},
		},
		map[string]time.Duration{"slow": 80 * time.Millisecond, "fast": 5 * time.Millisecond},
	)

	data, meta, err := ResolveChunkManifest(context.Background(), fixture.lookup, []*filer_pb.FileChunk{
		resolveTestManifest("slow", 0),
		resolveTestManifest("fast", 100),
	}, 0, 200, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"slow-data", "fast-data"}, fileIDs(data))
	require.Equal(t, []string{"slow", "fast"}, fileIDs(meta))
	require.Equal(t, []string{"fast", "slow"}, fixture.completionOrder())
}

func TestResolveChunkManifestNestedManifestsKeepDepthFirstOrder(t *testing.T) {
	fixture := newManifestReadFixture(t,
		map[string][]*filer_pb.FileChunk{
			"parent":  {resolveTestData("parent-data", 0), resolveTestManifest("nested", 10)},
			"sibling": {resolveTestData("sibling-data", 20)},
			"nested":  {resolveTestData("nested-data", 10)},
		},
		map[string]time.Duration{"parent": 60 * time.Millisecond, "sibling": 5 * time.Millisecond},
	)

	data, meta, err := ResolveChunkManifest(context.Background(), fixture.lookup, []*filer_pb.FileChunk{
		resolveTestManifest("parent", 0),
		resolveTestManifest("sibling", 20),
	}, 0, 100, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"parent-data", "nested-data", "sibling-data"}, fileIDs(data))
	require.Equal(t, []string{"parent", "nested", "sibling"}, fileIDs(meta))
	require.Equal(t, []string{"sibling", "parent", "nested"}, fixture.completionOrder())
}

func TestResolveChunkManifestFiltersBeforeReadingAndRecursively(t *testing.T) {
	fixture := newManifestReadFixture(t,
		map[string][]*filer_pb.FileChunk{
			"outside": {resolveTestData("never-read", 0)},
			"inside":  {resolveTestData("before", 0), resolveTestData("selected", 100), resolveTestData("after", 200)},
		}, nil)

	data, meta, err := ResolveChunkManifest(context.Background(), fixture.lookup, []*filer_pb.FileChunk{
		resolveTestManifest("outside", 0),
		resolveTestManifest("inside", 90),
	}, 100, 200, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"selected"}, fileIDs(data))
	require.Equal(t, []string{"inside"}, fileIDs(meta))
	require.Equal(t, int32(1), fixture.loads.Load(), "the non-overlapping parent manifest must not be read")
}

func TestResolveChunkManifestPropagatesReadAndFormatErrorsInInputOrder(t *testing.T) {
	fixture := newManifestReadFixture(t,
		map[string][]*filer_pb.FileChunk{
			"valid":  {resolveTestData("valid-data", 0)},
			"broken": nil,
		}, map[string]time.Duration{"broken": 50 * time.Millisecond})
	fixture.manifests["broken"] = []byte("not a protobuf manifest")

	data, meta, err := ResolveChunkManifest(context.Background(), fixture.lookup, []*filer_pb.FileChunk{
		resolveTestData("plain", 0),
		resolveTestManifest("valid", 10),
		resolveTestManifest("broken", 20),
	}, 0, 100, nil)
	require.Error(t, err)
	require.Nil(t, meta)
	require.Equal(t, []string{"plain", "valid-data"}, fileIDs(data))
	require.Contains(t, err.Error(), "fail to unmarshal manifest broken")

	lookupErr := errors.New("lookup failed")
	lookup := func(context.Context, string) ([]string, error) { return nil, lookupErr }
	_, _, err = ResolveChunkManifest(context.Background(), lookup, []*filer_pb.FileChunk{resolveTestManifest("lookup-error", 0)}, 0, 100, nil)
	require.ErrorIs(t, err, lookupErr)
}

func TestResolveChunkManifestCancellationStopsAllReads(t *testing.T) {
	fixture := newManifestReadFixture(t,
		map[string][]*filer_pb.FileChunk{
			"one": {resolveTestData("one-data", 0)},
			"two": {resolveTestData("two-data", 10)},
		},
		map[string]time.Duration{"one": time.Second, "two": time.Second},
	)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, _, err := ResolveChunkManifest(ctx, fixture.lookup, []*filer_pb.FileChunk{
			resolveTestManifest("one", 0),
			resolveTestManifest("two", 10),
		}, 0, 100, nil)
		result <- err
	}()

	deadline := time.Now().Add(time.Second)
	for fixture.loads.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(fixture.stopReads)
	cancel()
	err := <-result
	require.ErrorIs(t, err, context.Canceled)
	require.Eventually(t, func() bool {
		return fixture.active.Load() == 0
	}, time.Second, time.Millisecond, "all manifest readers must exit after ResolveChunkManifest returns")
}

type manifestFailureReadFixture struct {
	server       *httptest.Server
	fastRelease  chan struct{}
	slowStarted  chan struct{}
	fastStarted  chan struct{}
	slowCanceled chan struct{}
}

func newManifestFailureReadFixture(t testing.TB) *manifestFailureReadFixture {
	t.Helper()
	fixture := &manifestFailureReadFixture{
		fastRelease:  make(chan struct{}),
		slowStarted:  make(chan struct{}),
		fastStarted:  make(chan struct{}),
		slowCanceled: make(chan struct{}),
	}
	fixture.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch strings.TrimPrefix(r.URL.Path, "/") {
		case "fast":
			close(fixture.fastStarted)
			<-fixture.fastRelease
			w.Header().Set("Content-Length", "7")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("invalid"))
		case "slow":
			close(fixture.slowStarted)
			<-r.Context().Done()
			close(fixture.slowCanceled)
		}
	}))
	t.Cleanup(fixture.server.Close)
	return fixture
}

func (f *manifestFailureReadFixture) lookup(ctx context.Context, fileID string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return []string{f.server.URL + "/" + fileID}, nil
}

func waitForManifestFailureSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manifest request")
	}
}

func TestResolveChunkManifestFastFailureCancelsSlowSibling(t *testing.T) {
	testCases := []struct {
		name       string
		inputIDs   []string
		expectedID string
	}{
		{name: "fast then slow returns quickly", inputIDs: []string{"fast", "slow"}, expectedID: "fast"},
		{name: "slow then fast keeps real error", inputIDs: []string{"slow", "fast"}, expectedID: "fast"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newManifestFailureReadFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			chunks := make([]*filer_pb.FileChunk, 0, len(testCase.inputIDs))
			for i, id := range testCase.inputIDs {
				chunks = append(chunks, resolveTestManifest(id, int64(i*100)))
			}
			result := make(chan error, 1)
			go func() {
				_, _, err := ResolveChunkManifest(ctx, fixture.lookup, chunks, 0, 200, nil)
				result <- err
			}()

			waitForManifestFailureSignal(t, fixture.fastStarted)
			waitForManifestFailureSignal(t, fixture.slowStarted)
			close(fixture.fastRelease)

			var err error
			select {
			case err = <-result:
			case <-time.After(time.Second):
				cancel()
				select {
				case <-result:
				case <-time.After(time.Second):
					t.Fatal("ResolveChunkManifest did not finish after caller cancellation")
				}
				t.Fatal("fast manifest failure waited for the slow sibling")
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), "fail to unmarshal manifest "+testCase.expectedID)
			require.NotErrorIs(t, err, context.Canceled)
			waitForManifestFailureSignal(t, fixture.slowCanceled)
		})
	}
}

func TestResolveChunkManifestFastFailureCancelsEncryptedSibling(t *testing.T) {
	fixture := newManifestFailureReadFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slowChunk := resolveTestManifest("slow", 0)
	slowChunk.CipherKey = []byte("0123456789abcdef")
	chunks := []*filer_pb.FileChunk{
		slowChunk,
		resolveTestManifest("fast", 100),
	}
	result := make(chan error, 1)
	go func() {
		_, _, err := ResolveChunkManifest(ctx, fixture.lookup, chunks, 0, 200, nil)
		result <- err
	}()

	waitForManifestFailureSignal(t, fixture.fastStarted)
	waitForManifestFailureSignal(t, fixture.slowStarted)
	close(fixture.fastRelease)

	var err error
	select {
	case err = <-result:
	case <-time.After(time.Second):
		cancel()
		select {
		case <-result:
		case <-time.After(time.Second):
			t.Fatal("ResolveChunkManifest did not finish after caller cancellation")
		}
		t.Fatal("fast manifest failure waited for the encrypted slow sibling")
	}
	require.Error(t, err)
	require.Contains(t, err.Error(), "fail to unmarshal manifest fast")
	require.NotErrorIs(t, err, context.Canceled)
	waitForManifestFailureSignal(t, fixture.slowCanceled)
}

func TestResolveChunkManifestKeepsRealErrorAfterInternalCancellation(t *testing.T) {
	earlyStarted := make(chan struct{})
	lateStarted := make(chan struct{})
	earlyErr := errors.New("early lookup failed")
	lateErr := errors.New("late lookup failed")
	lookup := func(ctx context.Context, fileID string) ([]string, error) {
		switch fileID {
		case "early":
			close(earlyStarted)
			<-ctx.Done()
			return nil, earlyErr
		case "late":
			close(lateStarted)
			return nil, lateErr
		default:
			return nil, fmt.Errorf("unexpected manifest %s", fileID)
		}
	}

	result := make(chan error, 1)
	go func() {
		_, _, err := ResolveChunkManifest(context.Background(), lookup, []*filer_pb.FileChunk{
			resolveTestManifest("early", 0),
			resolveTestManifest("late", 100),
		}, 0, 200, nil)
		result <- err
	}()
	waitForManifestFailureSignal(t, earlyStarted)
	waitForManifestFailureSignal(t, lateStarted)

	select {
	case err := <-result:
		require.ErrorIs(t, err, earlyErr)
		require.NotErrorIs(t, err, lateErr)
	case <-time.After(time.Second):
		t.Fatal("ResolveChunkManifest did not return the input-order error")
	}
}

func TestResolveChunkManifestReturnsLaterFailureWithoutRecursingEarlierChildren(t *testing.T) {
	fixture := newManifestReadFixture(t,
		map[string][]*filer_pb.FileChunk{
			"a":       {resolveTestManifest("a-child", 0)},
			"a-child": {resolveTestData("a-child-data", 0)},
			"b":       nil,
		},
		map[string]time.Duration{"a": 5 * time.Millisecond, "b": 50 * time.Millisecond, "a-child": 2 * time.Second},
	)
	fixture.manifests["b"] = []byte("not a protobuf manifest")

	start := time.Now()
	data, meta, err := ResolveChunkManifest(context.Background(), fixture.lookup, []*filer_pb.FileChunk{
		resolveTestManifest("a", 0),
		resolveTestManifest("b", 100),
	}, 0, 200, nil)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Contains(t, err.Error(), "fail to unmarshal manifest b")
	require.Empty(t, data, "earlier manifest's children must not be recursed when a later manifest already failed")
	require.Nil(t, meta)
	require.Equal(t, int32(2), fixture.loads.Load(), "only the two top-level manifests must be read, not a-child")
	require.Less(t, elapsed, time.Second, "must return promptly without waiting for a-child's slow read")
}

func fileIDs(chunks []*filer_pb.FileChunk) []string {
	ids := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		ids = append(ids, chunk.GetFileIdString())
	}
	return ids
}

func resolveChunkManifestSerialForBenchmark(ctx context.Context, lookupFileIdFn func(context.Context, string) ([]string, error), chunks []*filer_pb.FileChunk, startOffset, stopOffset int64) (dataChunks, manifestChunks []*filer_pb.FileChunk, err error) {
	for _, chunk := range chunks {
		if max(chunk.Offset, startOffset) >= min(chunk.Offset+int64(chunk.Size), stopOffset) {
			continue
		}
		if !chunk.IsChunkManifest {
			dataChunks = append(dataChunks, chunk)
			continue
		}

		resolvedChunks, resolveErr := ResolveOneChunkManifest(ctx, lookupFileIdFn, chunk, nil)
		if resolveErr != nil {
			return dataChunks, nil, resolveErr
		}
		manifestChunks = append(manifestChunks, chunk)
		subDataChunks, subManifestChunks, subErr := resolveChunkManifestSerialForBenchmark(ctx, lookupFileIdFn, resolvedChunks, startOffset, stopOffset)
		if subErr != nil {
			return dataChunks, nil, subErr
		}
		dataChunks = append(dataChunks, subDataChunks...)
		manifestChunks = append(manifestChunks, subManifestChunks...)
	}
	return dataChunks, manifestChunks, nil
}

func BenchmarkResolveChunkManifestSerialVsParallel(b *testing.B) {
	manifests := make(map[string][]*filer_pb.FileChunk)
	inputs := make([]*filer_pb.FileChunk, 0, 8)
	delays := make(map[string]time.Duration)
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("m%d", i)
		manifests[id] = []*filer_pb.FileChunk{resolveTestData(fmt.Sprintf("d%d", i), int64(i*10))}
		inputs = append(inputs, resolveTestManifest(id, int64(i*10)))
		delays[id] = time.Millisecond
	}
	fixture := newManifestReadFixture(b, manifests, delays)
	b.ReportAllocs()

	b.Run("serial", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, err := resolveChunkManifestSerialForBenchmark(context.Background(), fixture.lookup, inputs, 0, 1000)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("parallel", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, err := ResolveChunkManifest(context.Background(), fixture.lookup, inputs, 0, 1000, nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
