package weed_server

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ecjStreamBytes(ids ...types.NeedleId) []byte {
	b := make([]byte, len(ids)*types.NeedleIdSize)
	for i, id := range ids {
		types.NeedleIdToBytes(b[i*types.NeedleIdSize:], id)
	}
	return b
}

func TestReceiveEcjIds(t *testing.T) {
	journal := ecjStreamBytes(1, 2, 3, 2)
	tests := []struct {
		name      string
		responses []*volume_server_pb.CopyFileResponse
		wantIds   []types.NeedleId
		wantFound bool
	}{
		{
			name:      "source has no journal",
			responses: nil,
		},
		{
			name:      "empty journal still carries its modified time",
			responses: []*volume_server_pb.CopyFileResponse{{ModifiedTsNs: 42}},
			wantFound: true,
		},
		{
			// A source that cannot read its journal's mtime reports 0; its
			// bytes still count (the Rust server sends unwrap_or(0)).
			name:      "bytes without a modified time",
			responses: []*volume_server_pb.CopyFileResponse{{FileContent: journal}},
			wantIds:   []types.NeedleId{1, 2, 3},
			wantFound: true,
		},
		{
			name: "records split across chunks",
			responses: []*volume_server_pb.CopyFileResponse{
				{FileContent: journal[:5], ModifiedTsNs: 7},
				{FileContent: journal[5:19]},
				{FileContent: journal[19:]},
			},
			wantIds:   []types.NeedleId{1, 2, 3},
			wantFound: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := &fakeCopyFileStream{responses: tt.responses}
			ids, found, err := receiveEcjIds(stream, util.NewWriteThrottler(0))
			require.NoError(t, err)
			assert.Equal(t, tt.wantFound, found)
			got := make([]types.NeedleId, 0, len(ids))
			for id := range ids {
				got = append(got, id)
			}
			assert.ElementsMatch(t, tt.wantIds, got)
		})
	}
}

func TestReceiveEcjIds_StreamError(t *testing.T) {
	stream := &fakeCopyFileStream{
		responses: []*volume_server_pb.CopyFileResponse{{FileContent: ecjStreamBytes(1), ModifiedTsNs: 1}},
		finalErr:  errors.New("peer went away"),
	}
	_, _, err := receiveEcjIds(stream, util.NewWriteThrottler(0))
	assert.Error(t, err)
}
