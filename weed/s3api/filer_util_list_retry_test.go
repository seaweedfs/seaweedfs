package s3api

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// listRetryStream replays a scripted sequence of entries and then, optionally,
// fails. failAfter < 0 means the stream runs to EOF.
type listRetryStream struct {
	entries   []*filer_pb.Entry
	index     int
	failAfter int
	failErr   error
}

func (s *listRetryStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	if s.failErr != nil && s.failAfter >= 0 && s.index >= s.failAfter {
		return nil, s.failErr
	}
	if s.index >= len(s.entries) {
		return nil, io.EOF
	}
	entry := s.entries[s.index]
	s.index++
	return &filer_pb.ListEntriesResponse{Entry: entry}, nil
}

func (s *listRetryStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *listRetryStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *listRetryStream) CloseSend() error             { return nil }
func (s *listRetryStream) Context() context.Context     { return context.Background() }
func (s *listRetryStream) SendMsg(any) error            { return nil }
func (s *listRetryStream) RecvMsg(any) error            { return nil }

// listRetryClient is a filer whose ListEntries fails in a scripted way for the
// first attempts and then behaves. callErrs is consumed one entry per attempt:
// a non-nil entry fails the ListEntries call itself (issue #7221), a nil entry
// lets the call through so recvErrs decides whether the stream breaks partway
// (issue #7235).
type listRetryClient struct {
	filer_pb.SeaweedFilerClient

	entries   []*filer_pb.Entry
	callErrs  []error
	recvErrs  []error
	recvAfter []int

	attempts int
}

func (c *listRetryClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	attempt := c.attempts
	c.attempts++

	if attempt < len(c.callErrs) && c.callErrs[attempt] != nil {
		return nil, c.callErrs[attempt]
	}

	stream := &listRetryStream{entries: c.entries, failAfter: -1}
	if attempt < len(c.recvErrs) && c.recvErrs[attempt] != nil {
		stream.failErr = c.recvErrs[attempt]
		stream.failAfter = c.recvAfter[attempt]
	}
	return stream, nil
}

// listRetryAccessor hands the scripted client to filer_pb.List without any
// wrapping of its own, so the test sees the error exactly as it leaves
// DoSeaweedListWithSnapshot.
type listRetryAccessor struct {
	client filer_pb.SeaweedFilerClient
}

func (a *listRetryAccessor) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(a.client)
}

func (a *listRetryAccessor) AdjustedUrl(*filer_pb.Location) string { return "" }
func (a *listRetryAccessor) GetDataCenter() string                 { return "" }

// listOnce mirrors (*S3ApiServer).list so the test drives the real
// filer_pb.List and DoSeaweedListWithSnapshot code path, including the fresh
// accumulator that makes a replay safe.
func listOnce(client filer_pb.FilerClient, parentDirectoryPath string) (entries []*filer_pb.Entry, isLast bool, err error) {
	err = filer_pb.List(context.Background(), client, parentDirectoryPath, "", func(entry *filer_pb.Entry, isLastEntry bool) error {
		entries = append(entries, entry)
		if isLastEntry {
			isLast = true
		}
		return nil
	}, "", false, 100)

	if len(entries) == 0 {
		isLast = true
	}

	return
}

func testUploadEntries(names ...string) []*filer_pb.Entry {
	entries := make([]*filer_pb.Entry, 0, len(names))
	for _, name := range names {
		entries = append(entries, &filer_pb.Entry{Name: name, Attributes: &filer_pb.FuseAttributes{}})
	}
	return entries
}

func entryNames(entries []*filer_pb.Entry) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name)
	}
	return names
}

// Issue #7221: a transient failure of the ListEntries call itself.
func TestListWithRetryRecoversFromTransientListEntriesCall(t *testing.T) {
	client := &listRetryClient{
		entries: testUploadEntries("upload-1", "upload-2", "upload-3"),
		callErrs: []error{
			status.Error(codes.Unavailable, "filer is restarting"),
			status.Error(codes.Unavailable, "filer is restarting"),
			nil,
		},
	}
	accessor := &listRetryAccessor{client: client}

	entries, _, err := listWithRetry("/buckets/b/.uploads", func() ([]*filer_pb.Entry, bool, error) {
		return listOnce(accessor, "/buckets/b/.uploads")
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"upload-1", "upload-2", "upload-3"}, entryNames(entries))
	assert.Equal(t, listRetryAttempts, client.attempts, "should have used every allowed attempt")
}

// Issue #7235: a transient failure on stream.Recv part way through the listing.
// The replay must return the full listing exactly once, with no entry from the
// aborted attempt carried over.
func TestListWithRetryRecoversFromTransientStreamRecv(t *testing.T) {
	client := &listRetryClient{
		entries: testUploadEntries("upload-1", "upload-2", "upload-3"),
		// break after two successful receives, so the first attempt has already
		// handed "upload-1" to the callback when the stream dies
		recvErrs:  []error{status.Error(codes.Unavailable, "transport is closing"), nil},
		recvAfter: []int{2, 0},
	}
	accessor := &listRetryAccessor{client: client}

	entries, _, err := listWithRetry("/buckets/b/.uploads", func() ([]*filer_pb.Entry, bool, error) {
		return listOnce(accessor, "/buckets/b/.uploads")
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"upload-1", "upload-2", "upload-3"}, entryNames(entries), "replay must not duplicate the entries of the aborted attempt")
	assert.Equal(t, 2, client.attempts)
}

// The retry is bounded: a filer that stays down still fails the request, and
// the transient error reaches the caller.
func TestListWithRetryGivesUpAfterBoundedAttempts(t *testing.T) {
	client := &listRetryClient{
		entries: testUploadEntries("upload-1"),
		callErrs: []error{
			status.Error(codes.Unavailable, "no filer available"),
			status.Error(codes.Unavailable, "no filer available"),
			status.Error(codes.Unavailable, "no filer available"),
			nil, // would succeed on a fourth attempt, which must never happen
		},
	}
	accessor := &listRetryAccessor{client: client}

	_, _, err := listWithRetry("/buckets/b/.uploads", func() ([]*filer_pb.Entry, bool, error) {
		return listOnce(accessor, "/buckets/b/.uploads")
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "Unavailable")
	assert.Equal(t, listRetryAttempts, client.attempts)
}

// A non-transient error is a real failure and must propagate on the first
// attempt rather than being retried away.
func TestListWithRetryPropagatesNonTransientError(t *testing.T) {
	for name, listErr := range map[string]error{
		"permission denied": status.Error(codes.PermissionDenied, "not allowed"),
		"invalid argument":  status.Error(codes.InvalidArgument, "bad prefix"),
	} {
		t.Run(name, func(t *testing.T) {
			client := &listRetryClient{
				entries:  testUploadEntries("upload-1"),
				callErrs: []error{listErr, nil},
			}
			accessor := &listRetryAccessor{client: client}

			_, _, err := listWithRetry("/buckets/b/.uploads", func() ([]*filer_pb.Entry, bool, error) {
				return listOnce(accessor, "/buckets/b/.uploads")
			})

			require.Error(t, err)
			assert.Equal(t, 1, client.attempts, "a non-transient error must not be retried")
		})
	}
}

// Not-found is authoritative: it must not be retried, and it must still read as
// not-found afterwards so listMultipartUploads keeps answering with an empty
// list instead of a 500.
func TestListWithRetryPropagatesNotFound(t *testing.T) {
	client := &listRetryClient{
		entries:  testUploadEntries("upload-1"),
		callErrs: []error{status.Error(codes.NotFound, "filer: no entry is found in filer store"), nil},
	}
	accessor := &listRetryAccessor{client: client}

	_, _, err := listWithRetry("/buckets/b/.uploads", func() ([]*filer_pb.Entry, bool, error) {
		return listOnce(accessor, "/buckets/b/.uploads")
	})

	require.Error(t, err)
	assert.Equal(t, 1, client.attempts, "not-found is authoritative and must not be retried")
	assert.True(t, isFilerNotFound(err), "not-found must survive the retry wrapper")
}

func TestIsRetryableListError(t *testing.T) {
	assert.False(t, isRetryableListError(nil))
	assert.True(t, isRetryableListError(status.Error(codes.Unavailable, "connection refused")))
	assert.True(t, isRetryableListError(status.Error(codes.ResourceExhausted, "too many requests")))
	// the ListEntries path loses the gRPC status to a %v wrap, so classification
	// has to survive on the message alone
	assert.True(t, isRetryableListError(fmt.Errorf("list /buckets/b/.uploads: %v", status.Error(codes.Unavailable, "filer is restarting"))))
	// and this is the shape the S3 gateway actually sees once the failover in
	// (*S3ApiServer).WithFilerClient has exhausted every filer
	assert.True(t, isRetryableListError(fmt.Errorf("all filers failed, last error: %w",
		fmt.Errorf("list /buckets/b/.uploads: %v", status.Error(codes.Unavailable, "filer is restarting")))))
	assert.False(t, isRetryableListError(status.Error(codes.NotFound, "filer: no entry is found in filer store")))
	assert.False(t, isRetryableListError(status.Error(codes.PermissionDenied, "not allowed")))
	assert.False(t, isRetryableListError(context.Canceled))
}
