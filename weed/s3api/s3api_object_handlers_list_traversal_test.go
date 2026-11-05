package s3api

import (
	"context"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
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
}
