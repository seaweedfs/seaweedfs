package s3tables

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/require"
)

// ListTableBuckets checks every listed bucket against its own policy. The
// policy is stored on the bucket entry the listing already returns, so the
// check needs no further filer round trip per bucket.

const (
	listPerfReader = "reader"
	listPerfOther  = "stranger"
)

var listPerfAllowReader = fmt.Sprintf(`{"Statement":[{"Effect":"Allow","Principal":%q,"Action":"s3tables:*","Resource":"*"}]}`, listPerfReader)

// seedListPerfBuckets stores n table buckets owned by DefaultAccountID, each
// with a policy that grants listPerfReader access.
func seedListPerfBuckets(tb testing.TB, fs *s3tablestest.MemFiler, n int) {
	tb.Helper()
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("perf-%05d", i)
		meta, err := json.Marshal(tableBucketMetadata{Name: name, OwnerAccountID: DefaultAccountID, CreatedAt: time.Unix(1700000000, 0)})
		require.NoError(tb, err)
		fs.Put(TablesPath, name, map[string][]byte{
			ExtendedKeyTableBucket: []byte("{}"),
			ExtendedKeyMetadata:    meta,
			ExtendedKeyPolicy:      []byte(listPerfAllowReader),
		})
	}
}

func listTableBucketsAs(tb testing.TB, m *Manager, fs *s3tablestest.MemFiler, caller string, maxBuckets int) ListTableBucketsResponse {
	tb.Helper()
	// The handler stops reading the filer's listing once the page is full;
	// cancelling releases that unfinished stream after each call.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var resp ListTableBucketsResponse
	require.NoError(tb, m.Execute(ctx, NewManagerClient(fs.Client), "ListTableBuckets",
		&ListTableBucketsRequest{MaxBuckets: maxBuckets}, &resp, caller))
	return resp
}

// TestListTableBucketsLooksUpNoBucket: a page of 100 buckets is served by the
// listing alone, not by one more filer lookup per bucket.
func TestListTableBucketsLooksUpNoBucket(t *testing.T) {
	fs := s3tablestest.Start(t)
	seedListPerfBuckets(t, fs, 100)
	m := NewManager()

	var lookups atomic.Int64
	fs.OnLookup = func(string, string) { lookups.Add(1) }
	resp := listTableBucketsAs(t, m, fs, listPerfReader, 100)
	fs.OnLookup = nil

	require.Len(t, resp.TableBuckets, 100)
	require.Zero(t, lookups.Load(), "filer lookups while listing one page of 100 buckets")
}

// BenchmarkListTableBuckets lists one page of 100 buckets out of n through
// the in-memory filer over gRPC. lookupDelay is added to every
// LookupDirectoryEntry to stand for a filer whose store is a network hop away
// (a SQL or KV backend). lookups/op is the number of filer lookups one
// listing makes.
func BenchmarkListTableBuckets(b *testing.B) {
	for _, n := range []int{100, 1000} {
		for _, lookupDelay := range []time.Duration{0, 500 * time.Microsecond} {
			b.Run(fmt.Sprintf("buckets=%d/lookupDelay=%s", n, lookupDelay), func(b *testing.B) {
				fs := s3tablestest.Start(b)
				seedListPerfBuckets(b, fs, n)
				m := NewManager()
				var lookups atomic.Int64
				fs.OnLookup = func(string, string) {
					lookups.Add(1)
					if lookupDelay > 0 {
						time.Sleep(lookupDelay)
					}
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if resp := listTableBucketsAs(b, m, fs, listPerfReader, 100); len(resp.TableBuckets) != 100 {
						b.Fatalf("listed %d buckets, want 100", len(resp.TableBuckets))
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(lookups.Load())/float64(b.N), "lookups/op")
			})
		}
	}
}
