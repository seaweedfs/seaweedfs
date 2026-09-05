package remote_storage

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"google.golang.org/protobuf/proto"
)

func TestCacheWaitTimeout(t *testing.T) {
	tests := []struct {
		name            string
		remoteSize      int64
		mountedLocation *remote_pb.RemoteStorageLocation
		want            time.Duration
	}{
		{name: "large file fails fast", remoteSize: 600 * 1024 * 1024, want: 2 * time.Second},
		{name: "small file waits longer", remoteSize: 1024, want: 10 * time.Second},
		{name: "medium file", remoteSize: 100 * 1024 * 1024, want: 5 * time.Second},
		{name: "unknown size", want: 5 * time.Second},
		{
			name:            "mount replaces the size tier",
			remoteSize:      600 * 1024 * 1024,
			mountedLocation: &remote_pb.RemoteStorageLocation{CacheWaitMs: proto.Int32(30000)},
			want:            30 * time.Second,
		},
		{
			name:            "mount opts out of caching",
			remoteSize:      1024,
			mountedLocation: &remote_pb.RemoteStorageLocation{CacheWaitMs: proto.Int32(0)},
			want:            0,
		},
		{
			name:            "negative wait never waits",
			remoteSize:      1024,
			mountedLocation: &remote_pb.RemoteStorageLocation{CacheWaitMs: proto.Int32(-1)},
			want:            0,
		},
		{
			name:            "unset wait keeps the size tier",
			remoteSize:      600 * 1024 * 1024,
			mountedLocation: &remote_pb.RemoteStorageLocation{},
			want:            2 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CacheWaitTimeout(tt.remoteSize, tt.mountedLocation); got != tt.want {
				t.Errorf("CacheWaitTimeout(%d) = %v, want %v", tt.remoteSize, got, tt.want)
			}
		})
	}
}
