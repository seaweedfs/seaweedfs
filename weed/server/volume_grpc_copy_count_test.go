package weed_server

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func TestCheckCopyCounts(t *testing.T) {
	tests := []struct {
		name              string
		sourceFileCount   uint64
		sourceDeleteCount uint64
		targetFileCount   uint64
		targetDeleteCount uint64
		wantErr           string
	}{
		{
			name: "empty volume counts match",
		},
		{
			name:              "file and deleted counts match",
			sourceFileCount:   10,
			sourceDeleteCount: 3,
			targetFileCount:   10,
			targetDeleteCount: 3,
		},
		{
			name:              "file count differs",
			sourceFileCount:   10,
			sourceDeleteCount: 3,
			targetFileCount:   9,
			targetDeleteCount: 3,
			wantErr:           "file count",
		},
		{
			name:              "deleted count differs",
			sourceFileCount:   10,
			sourceDeleteCount: 3,
			targetFileCount:   10,
			targetDeleteCount: 2,
			wantErr:           "deleted count",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkCopyCounts(
				&volume_server_pb.VolumeStatusResponse{
					FileCount:        tt.sourceFileCount,
					FileDeletedCount: tt.sourceDeleteCount,
				},
				tt.targetFileCount,
				tt.targetDeleteCount,
			)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("checkCopyCounts() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("checkCopyCounts() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestCopyCountsStable(t *testing.T) {
	tests := []struct {
		name   string
		before *volume_server_pb.VolumeStatusResponse
		after  *volume_server_pb.VolumeStatusResponse
		want   bool
	}{
		{
			name:   "empty volume is stable",
			before: &volume_server_pb.VolumeStatusResponse{},
			after:  &volume_server_pb.VolumeStatusResponse{},
			want:   true,
		},
		{
			name: "matching file and deleted counts are stable",
			before: &volume_server_pb.VolumeStatusResponse{
				FileCount:        10,
				FileDeletedCount: 3,
			},
			after: &volume_server_pb.VolumeStatusResponse{
				FileCount:        10,
				FileDeletedCount: 3,
			},
			want: true,
		},
		{
			name: "read-only state changes without count changes are stable",
			before: &volume_server_pb.VolumeStatusResponse{
				FileCount:        10,
				FileDeletedCount: 3,
				IsReadOnly:       true,
			},
			after: &volume_server_pb.VolumeStatusResponse{
				FileCount:        10,
				FileDeletedCount: 3,
				IsReadOnly:       false,
			},
			want: true,
		},
		{
			name: "changed file count is not stable",
			before: &volume_server_pb.VolumeStatusResponse{
				FileCount:        10,
				FileDeletedCount: 3,
			},
			after: &volume_server_pb.VolumeStatusResponse{
				FileCount:        9,
				FileDeletedCount: 3,
			},
		},
		{
			name: "changed deleted count is not stable",
			before: &volume_server_pb.VolumeStatusResponse{
				FileCount:        10,
				FileDeletedCount: 3,
			},
			after: &volume_server_pb.VolumeStatusResponse{
				FileCount:        10,
				FileDeletedCount: 2,
			},
		},
		{
			name:  "missing before status is not stable",
			after: &volume_server_pb.VolumeStatusResponse{},
		},
		{
			name:   "missing after status is not stable",
			before: &volume_server_pb.VolumeStatusResponse{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := copyCountsStable(tt.before, tt.after); got != tt.want {
				t.Fatalf("copyCountsStable() = %v, want %v", got, tt.want)
			}
		})
	}
}
