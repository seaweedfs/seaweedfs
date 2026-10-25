package s3lifecycle

import (
	"strconv"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Direct coverage for the parser. The router and the bootstrap walker
// both go through this single helper to read the demotion stamp, so
// pinning every parsing branch here keeps that contract enforced even
// as the call sites evolve.

func TestSuccessorFromEntryStamp_NilOrMissingReturnsZero(t *testing.T) {
	assert.True(t, SuccessorFromEntryStamp(nil).IsZero())
	assert.True(t, SuccessorFromEntryStamp(&filer_pb.Entry{}).IsZero())
	assert.True(t, SuccessorFromEntryStamp(&filer_pb.Entry{
		Extended: map[string][]byte{},
	}).IsZero())
}

func TestSuccessorFromEntryStamp_EmptyOrInvalidReturnsZero(t *testing.T) {
	for _, raw := range [][]byte{nil, []byte(""), []byte("nope")} {
		got := SuccessorFromEntryStamp(&filer_pb.Entry{
			Extended: map[string][]byte{s3_constants.ExtNoncurrentSinceNsKey: raw},
		})
		assert.True(t, got.IsZero(), "value %q must produce zero time", string(raw))
	}
}

func TestSuccessorFromEntryStamp_NonPositiveReturnsZero(t *testing.T) {
	// Stamps are wall-clock UnixNano captured at demotion time; <=0
	// signals "not set" — caller falls back to derived sibling mtime.
	for _, raw := range []string{"0", "-1"} {
		got := SuccessorFromEntryStamp(&filer_pb.Entry{
			Extended: map[string][]byte{s3_constants.ExtNoncurrentSinceNsKey: []byte(raw)},
		})
		assert.True(t, got.IsZero(), "value %q must produce zero time", raw)
	}
}

func TestSuccessorFromEntryStamp_PositiveNanosRoundTrip(t *testing.T) {
	const ns int64 = 1700000000_123456789
	got := SuccessorFromEntryStamp(&filer_pb.Entry{
		Extended: map[string][]byte{s3_constants.ExtNoncurrentSinceNsKey: []byte("1700000000123456789")},
	})
	assert.Equal(t, time.Unix(0, ns).UTC(), got.UTC())
}

func TestSuccessorFromEntryStamp_OrderedNanosStayOrdered(t *testing.T) {
	// Pins the read contract: if two stamps were written in increasing
	// nanosecond order, the parser must return Times in the same order.
	// Uses fixed nanosecond values rather than time.Now().UnixNano() —
	// UnixNano() drops the monotonic component, so back-to-back calls
	// can decrease if the wall clock steps backward and the property
	// being verified would then exercise OS clock behavior, not the
	// parser.
	require := require.New(t)
	const earlier int64 = 1700000000_000000001
	const later int64 = 1700000000_000000002

	earlierEntry := &filer_pb.Entry{
		Extended: map[string][]byte{s3_constants.ExtNoncurrentSinceNsKey: []byte(strconv.FormatInt(earlier, 10))},
	}
	laterEntry := &filer_pb.Entry{
		Extended: map[string][]byte{s3_constants.ExtNoncurrentSinceNsKey: []byte(strconv.FormatInt(later, 10))},
	}
	require.False(SuccessorFromEntryStamp(earlierEntry).After(SuccessorFromEntryStamp(laterEntry)),
		"parser must preserve write-order between two stamps")
}
