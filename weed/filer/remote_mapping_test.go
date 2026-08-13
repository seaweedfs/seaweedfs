package filer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindMountedRemoteMapping(t *testing.T) {
	mappings := &remote_pb.RemoteStorageMapping{
		Mappings: map[string]*remote_pb.RemoteStorageLocation{
			"/buckets/b":     {Name: "outer"},
			"/buckets/b/sub": {Name: "inner"},
			"/buckets/foo":   {Name: "foo"},
		},
	}

	tests := []struct {
		dir       string
		wantMount string
		wantName  string
		wantErr   bool
	}{
		{dir: "/buckets/b", wantMount: "/buckets/b", wantName: "outer"},
		{dir: "/buckets/b/other", wantMount: "/buckets/b", wantName: "outer"},
		{dir: "/buckets/b/sub", wantMount: "/buckets/b/sub", wantName: "inner"},
		{dir: "/buckets/b/sub/deep", wantMount: "/buckets/b/sub", wantName: "inner"},
		{dir: "/buckets/foo/x", wantMount: "/buckets/foo", wantName: "foo"},
		// a sibling sharing a name prefix is not under the mount
		{dir: "/buckets/foobar", wantErr: true},
		{dir: "/buckets/other", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.dir, func(t *testing.T) {
			mount, loc, err := FindMountedRemoteMapping(mappings, tt.dir)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantMount, mount)
			assert.Equal(t, tt.wantName, loc.Name)
		})
	}
}
