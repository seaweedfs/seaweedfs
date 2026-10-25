package erasure_coding

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestMaxShardTransfersPolicyRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		in   *int
	}{
		{name: "unset", in: nil},
		{name: "sequential", in: intPtr(0)},
		{name: "cap", in: intPtr(4)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewDefaultConfig()
			cfg.MaxShardTransfers = cloneMaxShardTransfers(tc.in)

			raw, err := proto.Marshal(cfg.ToTaskPolicy())
			require.NoError(t, err)
			var decoded worker_pb.TaskPolicy
			require.NoError(t, proto.Unmarshal(raw, &decoded))

			restored := NewDefaultConfig()
			require.NoError(t, restored.FromTaskPolicy(&decoded))
			ec := decoded.GetErasureCodingConfig()
			require.NotNil(t, ec)
			if tc.in == nil {
				require.Nil(t, ec.MaxShardTransfers)
				require.Nil(t, restored.MaxShardTransfers)
				return
			}
			require.NotNil(t, ec.MaxShardTransfers)
			require.Equal(t, int32(*tc.in), *ec.MaxShardTransfers)
			require.NotNil(t, restored.MaxShardTransfers)
			require.Equal(t, *tc.in, *restored.MaxShardTransfers)
		})
	}
}

func TestMaxShardTransfersNegativeClampsToSequential(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.MaxShardTransfers = intPtr(-3)
	restored := NewDefaultConfig()
	require.NoError(t, restored.FromTaskPolicy(cfg.ToTaskPolicy()))
	require.NotNil(t, restored.MaxShardTransfers)
	require.Equal(t, 0, *restored.MaxShardTransfers)
}

func intPtr(n int) *int { return &n }
