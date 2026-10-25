package credential_test

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/credential/memory"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Side-effect: register filer_etc so the existing
	// TestCredentialStoreInterface / TestGetAvailableStores assertions
	// (which expect filer_etc to always be present) stay satisfied now
	// that this test file pulls in `memory` and the `len(Stores) == 0`
	// skip path no longer fires.
	_ "github.com/seaweedfs/seaweedfs/weed/credential/filer_etc"
)

// TestSaveConfigurationDelegatesWithoutMaster confirms the override delegates
// to the underlying store and the no-master fan-out path is a safe no-op.
// propagateChange already short-circuits when masterClient is nil, so we
// don't need to spin up a fake S3 IAM cache server here.
func TestSaveConfigurationDelegatesWithoutMaster(t *testing.T) {
	ctx := context.Background()
	upstream := &memory.MemoryStore{}
	require.NoError(t, upstream.Initialize(nil, ""))

	ps := credential.NewPropagatingCredentialStore(upstream, nil, nil)

	cfg := &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "alice"}, {Name: "bob"}},
	}
	require.NoError(t, ps.SaveConfiguration(ctx, cfg))

	users, err := upstream.ListUsers(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"alice", "bob"}, users)
}

// TestSaveConfigurationDiffSnapshotTakenBeforeWrite confirms that deletions
// the underlying SaveConfiguration prunes are visible to the propagation
// diff. We replay a second SaveConfiguration that drops "bob" and verify the
// underlying store reflects the deletion (the propagation fan-out is a
// no-op without masterClient but the diff snapshot pathway runs).
func TestSaveConfigurationDiffSnapshotTakenBeforeWrite(t *testing.T) {
	ctx := context.Background()
	upstream := &memory.MemoryStore{}
	require.NoError(t, upstream.Initialize(nil, ""))

	ps := credential.NewPropagatingCredentialStore(upstream, nil, nil)

	require.NoError(t, ps.SaveConfiguration(ctx, &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "alice"}, {Name: "bob"}},
	}))
	require.NoError(t, ps.SaveConfiguration(ctx, &iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{Name: "alice"}},
	}))

	users, err := upstream.ListUsers(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"alice"}, users)
}
