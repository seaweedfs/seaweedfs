package shell

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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
