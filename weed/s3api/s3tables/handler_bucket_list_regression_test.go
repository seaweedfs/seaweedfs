package s3tables

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/require"
)

func listedBucketNames(resp ListTableBucketsResponse) []string {
	names := make([]string, 0, len(resp.TableBuckets))
	for _, b := range resp.TableBuckets {
		names = append(names, b.Name)
	}
	return names
}

// TestListTableBucketsStillAppliesEachBucketPolicy: the owner sees every
// bucket but one its policy denies, a grantee sees the buckets whose policy
// grants it, and an unrelated account sees none.
func TestListTableBucketsStillAppliesEachBucketPolicy(t *testing.T) {
	fs := s3tablestest.Start(t)
	seedListPerfBuckets(t, fs, 3)
	m := NewManager()
	m.SetDefaultAllow(false)

	deny := `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:GetTableBucket","Resource":"*"}]}`
	fs.Get(TablesPath, "perf-00001").Extended[ExtendedKeyPolicy] = []byte(deny)
	delete(fs.Get(TablesPath, "perf-00002").Extended, ExtendedKeyPolicy)

	names := func(caller string) []string {
		return listedBucketNames(listTableBucketsAs(t, m, fs, caller, 100))
	}
	require.Equal(t, []string{"perf-00000", "perf-00002"}, names(DefaultAccountID))
	require.Equal(t, []string{"perf-00000"}, names(listPerfReader))
	require.Empty(t, names(listPerfOther))
}

// TestListTableBucketsPagesThroughAllBuckets: continuation tokens walk every
// bucket once, in name order, a full page at a time.
func TestListTableBucketsPagesThroughAllBuckets(t *testing.T) {
	fs := s3tablestest.Start(t)
	seedListPerfBuckets(t, fs, 250)
	m := NewManager()

	var all []string
	var pages []int
	token := ""
	for {
		var resp ListTableBucketsResponse
		require.NoError(t, m.Execute(context.Background(), NewManagerClient(fs.Client), "ListTableBuckets",
			&ListTableBucketsRequest{MaxBuckets: 100, ContinuationToken: token}, &resp, listPerfReader))
		all = append(all, listedBucketNames(resp)...)
		pages = append(pages, len(resp.TableBuckets))
		token = resp.ContinuationToken
		if token == "" {
			break
		}
	}
	require.Equal(t, []int{100, 100, 50}, pages)
	require.Len(t, all, 250)
	for i, name := range all {
		require.Equal(t, fmt.Sprintf("perf-%05d", i), name)
	}
}

// TestListTableBucketsAppliesPrefix: only names starting with the prefix are
// listed.
func TestListTableBucketsAppliesPrefix(t *testing.T) {
	fs := s3tablestest.Start(t)
	seedListPerfBuckets(t, fs, 25)
	m := NewManager()

	var resp ListTableBucketsResponse
	require.NoError(t, m.Execute(context.Background(), NewManagerClient(fs.Client), "ListTableBuckets",
		&ListTableBucketsRequest{Prefix: "perf-0001"}, &resp, DefaultAccountID))
	var want []string
	for i := 10; i < 20; i++ {
		want = append(want, fmt.Sprintf("perf-%05d", i))
	}
	require.Equal(t, want, listedBucketNames(resp))
}

// TestListTableBucketsSkipsWhatIsNotABucket: hidden directories, directories
// without the table bucket marker, files and buckets whose metadata does not
// decode are left out.
func TestListTableBucketsSkipsWhatIsNotABucket(t *testing.T) {
	fs := s3tablestest.Start(t)
	seedListPerfBuckets(t, fs, 1)
	valid := fs.Get(TablesPath, "perf-00000").Extended
	fs.Put(TablesPath, ".hidden", valid)
	fs.Put(TablesPath, "plain-dir", map[string][]byte{ExtendedKeyMetadata: valid[ExtendedKeyMetadata]})
	fs.PutFile(TablesPath, "a-file", time.Now())
	fs.Put(TablesPath, "bad-metadata", map[string][]byte{
		ExtendedKeyTableBucket: []byte("{}"),
		ExtendedKeyMetadata:    []byte("{not json"),
		ExtendedKeyPolicy:      []byte(listPerfAllowReader),
	})
	m := NewManager()

	require.Equal(t, []string{"perf-00000"}, listedBucketNames(listTableBucketsAs(t, m, fs, DefaultAccountID, 100)))
}

// TestListTableBucketsRejectsTooManyBuckets: MaxBuckets above 1000 is a
// client error.
func TestListTableBucketsRejectsTooManyBuckets(t *testing.T) {
	fs := s3tablestest.Start(t)
	m := NewManager()
	err := m.Execute(context.Background(), NewManagerClient(fs.Client), "ListTableBuckets",
		&ListTableBucketsRequest{MaxBuckets: 1001}, nil, DefaultAccountID)
	requireS3TablesError(t, err, ErrCodeInvalidRequest)
}
