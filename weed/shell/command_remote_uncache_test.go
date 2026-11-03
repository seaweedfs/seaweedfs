package shell

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestFileFilter_matches_minCacheAge(t *testing.T) {
	now := time.Now().Unix()

	tests := []struct {
		name        string
		minCacheAge int64
		entry       *filer_pb.Entry
		want        bool
	}{
		{
			name:        "no minCacheAge",
			minCacheAge: -1,
			entry: &filer_pb.Entry{
				Attributes: &filer_pb.FuseAttributes{Crtime: now - 100},
			},
			want: true,
		},
		{
			name:        "recent cache, should not match",
			minCacheAge: 3600,
			entry: &filer_pb.Entry{
				Attributes: &filer_pb.FuseAttributes{Crtime: now - 7200},
				RemoteEntry: &filer_pb.RemoteEntry{
					LastLocalSyncTsNs: now * 1e9,
				},
			},
			want: false,
		},
		{
			name:        "old cache, should match",
			minCacheAge: 3600,
			entry: &filer_pb.Entry{
				Attributes: &filer_pb.FuseAttributes{Crtime: now - 7200},
				RemoteEntry: &filer_pb.RemoteEntry{
					LastLocalSyncTsNs: (now - 4000) * 1e9,
				},
			},
			want: true,
		},
		{
			name:        "no remote entry, uses crtime - recent, should not match",
			minCacheAge: 3600,
			entry: &filer_pb.Entry{
				Attributes: &filer_pb.FuseAttributes{Crtime: now - 100},
			},
			want: false,
		},
		{
			name:        "no remote entry, uses crtime - old, should match",
			minCacheAge: 3600,
			entry: &filer_pb.Entry{
				Attributes: &filer_pb.FuseAttributes{Crtime: now - 4000},
			},
			want: true,
		},
		{
			name:        "remote entry with 0 sync ts, uses crtime - recent, should not match",
			minCacheAge: 3600,
			entry: &filer_pb.Entry{
				Attributes:  &filer_pb.FuseAttributes{Crtime: now - 100},
				RemoteEntry: &filer_pb.RemoteEntry{LastLocalSyncTsNs: 0},
			},
			want: false,
		},
		{
			name:        "nil attributes, should not match",
			minCacheAge: 3600,
			entry: &filer_pb.Entry{
				Attributes: nil,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defaultString := ""
			defaultInt64 := int64(-1)
			ff := &FileFilter{
				include:     &defaultString,
				exclude:     &defaultString,
				minSize:     &defaultInt64,
				maxSize:     &defaultInt64,
				minAge:      &defaultInt64,
				maxAge:      &defaultInt64,
				minCacheAge: &tt.minCacheAge,
				now:         now,
			}

			if got := ff.matches(tt.entry); got != tt.want {
				t.Errorf("FileFilter.matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

// The uncache clears the chunks of the entry its walk read. Conditioning the
// write on that copy keeps a file overwritten since the walk from being reset
// to the remote pointer with its new chunks dropped.
func TestUncacheEntryIsConditionedOnTheReadEntry(t *testing.T) {
	read := &filer_pb.Entry{
		Name:        "f",
		Chunks:      []*filer_pb.FileChunk{{Fid: &filer_pb.FileId{VolumeId: 3, FileKey: 1, Cookie: 7}}},
		RemoteEntry: &filer_pb.RemoteEntry{StorageName: "r", LastLocalSyncTsNs: 5},
	}
	for _, tc := range []struct {
		name  string
		err   error
		stale bool
	}{
		{"unchanged", nil, false},
		{"overwritten since the walk", status.Error(codes.FailedPrecondition, "precondition failed"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &conditionalUpdateFiler{err: tc.err}
			entry := proto.Clone(read).(*filer_pb.Entry)
			stale, err := uncacheEntry(fake, "/buckets/b", entry)
			if stale != tc.stale || (err != nil) != (tc.err != nil) {
				t.Fatalf("stale=%v err=%v, want stale=%v err=%v", stale, err, tc.stale, tc.err)
			}
			clauses := fake.req.GetCondition().GetClauses()
			if len(clauses) != 1 || clauses[0].Kind != filer_pb.WriteCondition_IF_ENTRY_EQUAL || !proto.Equal(clauses[0].ExpectedEntry, read) {
				t.Fatalf("condition %v must expect the entry as read", fake.req.GetCondition())
			}
			written := fake.req.Entry
			if len(written.Chunks) != 0 || written.RemoteEntry.LastLocalSyncTsNs != 0 {
				t.Fatalf("written entry %v must drop the local copy", written)
			}
		})
	}
}
