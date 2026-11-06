package s3api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpc "google.golang.org/grpc"
)

// recordingFilerClient records the directories a listing asks the filer to list.
type recordingFilerClient struct {
	*testFilerClient
	mu     sync.Mutex
	listed []string
}

func (c *recordingFilerClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	c.mu.Lock()
	c.listed = append(c.listed, in.Directory)
	c.mu.Unlock()
	return c.testFilerClient.ListEntries(ctx, in, opts...)
}

func plainFile(name string) *filer_pb.Entry {
	return &filer_pb.Entry{Name: name, Attributes: &filer_pb.FuseAttributes{}}
}

// A trailing-slash prefix names one directory. Listing its parent by name prefix
// makes a SQL store scan every sibling after the name, which in a wide bucket root
// costs seconds for a prefix holding a handful of keys.
func TestTrailingSlashPrefixLooksUpItsDirectory(t *testing.T) {
	newClient := func() *recordingFilerClient {
		return &recordingFilerClient{testFilerClient: &testFilerClient{entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test": {
				newDir("account-0001"), newDir("account-0001-old"), plainFile("account-0001.txt"),
				newDir("account-00010"), plainFile("solo"),
			},
			"/buckets/test/account-0001":          {newDir("Contract"), plainFile("note")},
			"/buckets/test/account-0001/Contract": {plainFile("document.pdf")},
			"/buckets/test/account-0001-old":      {plainFile("stale")},
			"/buckets/test/account-00010":         {plainFile("other")},
		}}}
	}

	client := newClient()
	seen := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", prefix: "account-0001", bucket: "test"}, &ListingCursor{maxKeys: 1000, prefixEndsOnDelimiter: true})
	assert.Equal(t, []string{"document.pdf", "note"}, seen)
	assert.NotContains(t, client.listed, "/buckets/test", "the named directory is looked up, not found by listing its parent")

	client = newClient()
	seen = listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", prefix: "account-0001", bucket: "test"}, &ListingCursor{maxKeys: 1, prefixEndsOnDelimiter: true})
	assert.Equal(t, []string{"document.pdf"}, seen)

	for _, prefix := range []string{"missing", "solo", "account-000"} {
		seen = listedNames(t, newClient(), listDirectoryRequest{dir: "/buckets/test", prefix: prefix, bucket: "test"}, &ListingCursor{maxKeys: 1000, prefixEndsOnDelimiter: true})
		assert.Empty(t, seen, "prefix %q/ names no directory", prefix)
	}

	client = newClient()
	seen = listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", prefix: "solo", bucket: "test"}, &ListingCursor{maxKeys: 1000, prefixEndsOnDelimiter: true})
	assert.Empty(t, seen)
	assert.NotContains(t, client.listed, "/buckets/test", "a file found by the lookup ends the listing")
}

// branchedTreeKeys builds keys shaped like a bucket of many accounts, each holding a
// few documents several directories down, with siblings that sort around the
// "<name>/" block of a directory, and some directories longer than a prefetch window.
func branchedTreeKeys(accounts int) (keys, emptyDirs []string) {
	for i := 0; i < accounts; i++ {
		keys = append(keys, fmt.Sprintf("acct-%02d/Contract/rec-%02d/2026/10/doc.pdf", i, i))
		if i%3 == 0 {
			keys = append(keys, fmt.Sprintf("acct-%02d/Contract/rec-%02d/2026/11/doc.pdf", i, i))
		}
		if i%4 == 0 {
			keys = append(keys, fmt.Sprintf("acct-%02d/note.txt", i))
		}
		if i%5 == 0 {
			keys = append(keys, fmt.Sprintf("acct-%02d-old/x", i))
		}
		if i%6 == 0 {
			keys = append(keys, fmt.Sprintf("acct-%02d.txt", i))
		}
		if i%7 == 0 {
			emptyDirs = append(emptyDirs, fmt.Sprintf("acct-%02d/empty", i))
		}
		if i%10 == 0 {
			for j := 0; j < listPrefetchWindow+8; j++ {
				keys = append(keys, fmt.Sprintf("acct-%02d/bulk/f-%02d", i, j))
			}
		}
	}
	sort.Strings(keys)
	return keys, emptyDirs
}

func filerTreeOf(bucketDir string, keys, emptyDirs []string) map[string][]*filer_pb.Entry {
	tree := map[string][]*filer_pb.Entry{bucketDir: {}}
	seen := map[string]bool{}
	add := func(path string, isDir bool) {
		for {
			dir, name := toDirAndName(path)
			parent := bucketDir
			if dir != "" {
				parent += "/" + dir
			}
			if !seen[parent+"/"+name] {
				seen[parent+"/"+name] = true
				if isDir {
					tree[parent] = append(tree[parent], newDir(name))
					if _, ok := tree[parent+"/"+name]; !ok {
						tree[parent+"/"+name] = []*filer_pb.Entry{}
					}
				} else {
					tree[parent] = append(tree[parent], plainFile(name))
				}
			}
			if dir == "" {
				return
			}
			path, isDir = dir, true
		}
	}
	for _, key := range keys {
		add(key, false)
	}
	for _, dir := range emptyDirs {
		add(dir, true)
	}
	for _, entries := range tree {
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	}
	return tree
}

// slowFilerClient answers each ListEntries after a delay, like a store that has to
// read a page from disk, and records how many listings were in flight at once.
type slowFilerClient struct {
	*testFilerClient
	delay       time.Duration
	mu          sync.Mutex
	calls       int
	inFlight    int
	maxInFlight int
}

func (c *slowFilerClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	c.mu.Lock()
	c.calls++
	c.inFlight++
	c.maxInFlight = max(c.maxInFlight, c.inFlight)
	c.mu.Unlock()
	time.Sleep(c.delay)
	c.mu.Lock()
	c.inFlight--
	c.mu.Unlock()
	return c.testFilerClient.ListEntries(ctx, in, opts...)
}

// A recursive walk over many small directories pays one listing round trip per
// directory. Reading ahead keeps several of them in flight and must leave the page,
// and where the next page resumes, exactly as the serial walk has them.
func TestRecursiveListingReadsAhead(t *testing.T) {
	keys, emptyDirs := branchedTreeKeys(40)
	t.Run("small entries", func(t *testing.T) {
		testReadAheadMatchesSerialWalk(t, filerTreeOf("/buckets/test", keys, emptyDirs), keys)
	})
	// Entries this large stop prefetched windows and the read-ahead of the walk's own
	// listing at their byte limits, short of their entry limits.
	t.Run("large entries", func(t *testing.T) {
		tree := filerTreeOf("/buckets/test", keys, emptyDirs)
		for dir, entries := range tree {
			size := listPrefetchBytes / 3
			if dir == "/buckets/test" {
				size = listReadAheadBytes / 4
			}
			for _, entry := range entries {
				if !entry.IsDirectory {
					entry.Content = make([]byte, size)
				}
			}
		}
		testReadAheadMatchesSerialWalk(t, tree, keys)
	})
}

func testReadAheadMatchesSerialWalk(t *testing.T, tree map[string][]*filer_pb.Entry, keys []string) {
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}

	walk := func(maxKeys uint16, readAhead bool) (seen []string, nextMarker string, cursor *ListingCursor, client *slowFilerClient) {
		client = &slowFilerClient{testFilerClient: &testFilerClient{entriesByDir: tree}, delay: time.Millisecond}
		cursor = &ListingCursor{maxKeys: maxKeys}
		if readAhead {
			cursor.prefetch = newListPrefetcher(context.Background(), client, maxKeys)
			defer cursor.prefetch.close()
		}
		nextMarker, err := s3a.doListFilerEntries(context.Background(), client, listDirectoryRequest{dir: "/buckets/test", bucket: "test"}, cursor, func(dir string, entry *filer_pb.Entry) {
			seen = append(seen, strings.TrimPrefix(dir+"/"+entry.Name, "/buckets/test/"))
			cursor.maxKeys--
		})
		require.NoError(t, err)
		return
	}

	for _, maxKeys := range []uint16{1, 2, 7, 30, 1000} {
		t.Run(fmt.Sprintf("max=%d", maxKeys), func(t *testing.T) {
			wantSeen, wantMarker, wantCursor, _ := walk(maxKeys, false)
			gotSeen, gotMarker, gotCursor, client := walk(maxKeys, true)
			assert.Equal(t, wantSeen, gotSeen)
			assert.Equal(t, wantMarker, gotMarker)
			assert.Equal(t, wantCursor.isTruncated, gotCursor.isTruncated)
			if maxKeys == 1000 {
				assert.Equal(t, keys, gotSeen)
				assert.Greater(t, client.maxInFlight, 1, "directories are listed concurrently")
			}
		})
	}
}

// Paging a recursive listing through the S3 response builder, with read-ahead on,
// returns every key once and in order.
func TestRecursiveListingPagesWithReadAhead(t *testing.T) {
	keys, emptyDirs := branchedTreeKeys(40)
	filer := &fakeBucketDeleteFiler{entriesByDir: filerTreeOf("/buckets/test", keys, emptyDirs)}
	s3a := newFailoverTestServer(t, startFakeFiler(t, filer))
	s3a.option.BucketsPath = "/buckets"
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("test", &BucketConfig{Name: "test"})

	for _, prefix := range []string{"", "acct-0", "acct-03/", "acct-03/Contract/"} {
		var want []string
		for _, key := range keys {
			if strings.HasPrefix(key, prefix) {
				want = append(want, key)
			}
		}
		for _, pageSize := range []uint16{1, 3, 10, 1000} {
			t.Run(fmt.Sprintf("prefix=%s/max=%d", prefix, pageSize), func(t *testing.T) {
				var got []string
				marker := ""
				for page := 0; ; page++ {
					require.Less(t, page, len(keys)+1, "listing did not terminate")
					response, err := s3a.listFilerEntries(context.Background(), listObjectsRequest{
						bucket: "test", prefix: prefix, marker: marker, maxKeys: pageSize,
					})
					require.NoError(t, err)
					for _, entry := range response.Contents {
						got = append(got, entry.Key)
					}
					if !response.IsTruncated {
						break
					}
					require.Greater(t, response.NextMarker, marker, "pagination must advance")
					marker = response.NextMarker
				}
				assert.Equal(t, want, got)
			})
		}
	}
}

// stallingFilerClient holds a listing open after its last entry until the caller
// gives up, like a SQL store still scanning a directory past the last match.
type stallingFilerClient struct {
	*testFilerClient
	stall func(*filer_pb.ListEntriesRequest) bool
}

type stallingStream struct {
	grpc.ServerStreamingClient[filer_pb.ListEntriesResponse]
	ctx context.Context
}

func (s stallingStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	resp, err := s.ServerStreamingClient.Recv()
	if err == io.EOF {
		<-s.ctx.Done()
		return nil, s.ctx.Err()
	}
	return resp, err
}

func (c *stallingFilerClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	stream, err := c.testFilerClient.ListEntries(ctx, in, opts...)
	if err != nil || !c.stall(in) {
		return stream, err
	}
	return stallingStream{ServerStreamingClient: stream, ctx: ctx}, nil
}

// A page that fills before a listing ends must not wait for the listing's end, and
// the reads still open when the page is done are cancelled and joined.
func TestRecursiveListingDoesNotWaitForStalledListings(t *testing.T) {
	keys, emptyDirs := branchedTreeKeys(40)
	tree := filerTreeOf("/buckets/test", keys, emptyDirs)

	for _, maxKeys := range []uint16{2, 5} {
		want := listedNames(t, &testFilerClient{entriesByDir: tree}, listDirectoryRequest{dir: "/buckets/test", bucket: "test"}, &ListingCursor{maxKeys: maxKeys})

		ctx, cancel := context.WithCancel(context.Background())
		client := &stallingFilerClient{testFilerClient: &testFilerClient{entriesByDir: tree}, stall: func(in *filer_pb.ListEntriesRequest) bool {
			return in.Directory == "/buckets/test" || !strings.HasPrefix(in.Directory, "/buckets/test/acct-00")
		}}
		done := make(chan []string)
		go func() {
			cursor := &ListingCursor{maxKeys: maxKeys}
			cursor.prefetch = newListPrefetcher(ctx, client, maxKeys)
			got := listedNames(t, client, listDirectoryRequest{dir: "/buckets/test", bucket: "test"}, cursor)
			cursor.prefetch.close()
			done <- got
		}()
		select {
		case got := <-done:
			assert.Equal(t, want, got)
		case <-time.After(10 * time.Second):
			cancel()
			t.Fatalf("max=%d: listing waited for a stalled stream", maxKeys)
		}
		cancel()
	}
}

// failingFilerClient breaks the listings it picks after their first entry.
type failingFilerClient struct {
	*testFilerClient
	fail   func(*filer_pb.ListEntriesRequest) bool
	failed atomic.Int32
}

type failingStream struct {
	grpc.ServerStreamingClient[filer_pb.ListEntriesResponse]
	client   *failingFilerClient
	received bool
}

func (s *failingStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	if s.received {
		s.client.failed.Add(1)
		return nil, errors.New("connection reset")
	}
	resp, err := s.ServerStreamingClient.Recv()
	s.received = err == nil
	return resp, err
}

func (c *failingFilerClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	stream, err := c.testFilerClient.ListEntries(ctx, in, opts...)
	if err != nil || !c.fail(in) {
		return stream, err
	}
	return &failingStream{ServerStreamingClient: stream, client: c}, nil
}

// A listing that breaks before the page fills fails the walk, and a prefetched
// window that breaks is read again by the walk itself.
func TestRecursiveListingStreamErrors(t *testing.T) {
	keys, emptyDirs := branchedTreeKeys(40)
	tree := filerTreeOf("/buckets/test", keys, emptyDirs)
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	walk := func(client *failingFilerClient, readAhead bool) (seen []string, err error) {
		cursor := &ListingCursor{maxKeys: 1000}
		if readAhead {
			cursor.prefetch = newListPrefetcher(context.Background(), client, cursor.maxKeys)
			defer cursor.prefetch.close()
		}
		_, err = s3a.doListFilerEntries(context.Background(), client, listDirectoryRequest{dir: "/buckets/test", bucket: "test"}, cursor, func(dir string, entry *filer_pb.Entry) {
			seen = append(seen, strings.TrimPrefix(dir+"/"+entry.Name, "/buckets/test/"))
			cursor.maxKeys--
		})
		return
	}

	for _, readAhead := range []bool{false, true} {
		for _, dir := range []string{"/buckets/test", "/buckets/test/acct-03/Contract"} {
			client := &failingFilerClient{testFilerClient: &testFilerClient{entriesByDir: tree}, fail: func(in *filer_pb.ListEntriesRequest) bool {
				return in.Directory == dir
			}}
			_, err := walk(client, readAhead)
			assert.ErrorContains(t, err, "connection reset", "readAhead=%v dir=%s", readAhead, dir)
		}
	}

	client := &failingFilerClient{testFilerClient: &testFilerClient{entriesByDir: tree}, fail: func(in *filer_pb.ListEntriesRequest) bool {
		return in.Limit == listPrefetchWindow
	}}
	seen, err := walk(client, true)
	require.NoError(t, err)
	assert.Equal(t, keys, seen)
	assert.Positive(t, client.failed.Load(), "prefetched windows broke")
}
