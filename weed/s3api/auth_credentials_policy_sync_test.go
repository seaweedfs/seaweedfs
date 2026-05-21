package s3api

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/stretchr/testify/require"
)

func newTestIAMManager(t *testing.T) *integration.IAMManager {
	t.Helper()
	mgr := integration.NewIAMManager()
	require.NoError(t, mgr.Initialize(&integration.IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: 12 * time.Hour},
			Issuer:           "test",
			SigningKey:       []byte("test-signing-key-32-characters!!"),
			AccountId:        "111122223333",
		},
		Policy: &policy.PolicyEngineConfig{DefaultEffect: "Allow", StoreType: "memory"},
		Roles:  &integration.RoleStoreConfig{StoreType: "memory"},
	}, func() string { return "localhost:8888" }))
	return mgr
}

func TestPutPolicy_SyncsToIAMManager(t *testing.T) {
	mgr := newTestIAMManager(t)
	iam := &IdentityAccessManagement{}
	iam.SetIAMIntegration(NewS3IAMIntegration(mgr, ""))

	policyDoc, _ := json.Marshal(map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{"Effect": "Allow", "Action": "s3:*", "Resource": "arn:aws:s3:::backup/*"},
		},
	})

	require.NoError(t, iam.PutPolicy("BackupAll", string(policyDoc)))

	allowed, err := mgr.IsActionAllowed(context.Background(), &integration.ActionRequest{
		Principal:   "arn:aws:iam::111122223333:user/test",
		Action:      "s3:PutObject",
		Resource:    "arn:aws:s3:::backup/file.txt",
		PolicyNames: []string{"BackupAll"},
	})
	require.NoError(t, err)
	require.True(t, allowed, "PutPolicy should sync to IAM Manager policy engine")
}

func TestDeletePolicy_SyncsToIAMManager(t *testing.T) {
	mgr := newTestIAMManager(t)
	iam := &IdentityAccessManagement{}
	iam.SetIAMIntegration(NewS3IAMIntegration(mgr, ""))

	policyDoc, _ := json.Marshal(map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{"Effect": "Allow", "Action": "s3:*", "Resource": "arn:aws:s3:::backup/*"},
		},
	})

	require.NoError(t, iam.PutPolicy("BackupAll", string(policyDoc)))
	require.NoError(t, iam.DeletePolicy("BackupAll"))

	allowed, err := mgr.IsActionAllowed(context.Background(), &integration.ActionRequest{
		Principal:   "arn:aws:iam::111122223333:user/test",
		Action:      "s3:PutObject",
		Resource:    "arn:aws:s3:::backup/file.txt",
		PolicyNames: []string{"BackupAll"},
	})
	require.NoError(t, err)
	require.False(t, allowed, "DeletePolicy should remove from IAM Manager policy engine")
}

// TestSetIAMIntegration_FlushesLoadedPolicies reproduces the startup race: a
// policy is loaded into the IAM cache before the integration is attached (so the
// sync was skipped), then SetIAMIntegration must flush it into the manager's
// engine. Without the flush, policy_names identities get AccessDenied until an
// external IAM change triggers a reload.
func TestSetIAMIntegration_FlushesLoadedPolicies(t *testing.T) {
	iam := &IdentityAccessManagement{}

	policyDoc, _ := json.Marshal(map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{"Effect": "Allow", "Action": "s3:*", "Resource": "arn:aws:s3:::backup/*"},
		},
	})

	// Integration not attached yet, so this only lands in the legacy engine.
	require.NoError(t, iam.PutPolicy("BackupAll", string(policyDoc)))

	mgr := newTestIAMManager(t)
	iam.SetIAMIntegration(NewS3IAMIntegration(mgr, ""))

	allowed, err := mgr.IsActionAllowed(context.Background(), &integration.ActionRequest{
		Principal:   "arn:aws:iam::111122223333:user/test",
		Action:      "s3:PutObject",
		Resource:    "arn:aws:s3:::backup/file.txt",
		PolicyNames: []string{"BackupAll"},
	})
	require.NoError(t, err)
	require.True(t, allowed, "SetIAMIntegration should flush already-loaded policies into the IAM Manager")
}

// TestResyncIAMManager_ReflectsCurrentPolicies pins the property that prevents
// the stale-snapshot race: SyncRuntimePolicies replaces the full desired set, so
// the resync must derive that set from the live iam.policies map at apply time,
// not from a view captured earlier. Here the map is mutated out from under an
// already-synced policy, and the next resync must converge the manager onto the
// new map rather than reinstating the old state.
func TestResyncIAMManager_ReflectsCurrentPolicies(t *testing.T) {
	mgr := newTestIAMManager(t)
	iam := &IdentityAccessManagement{}
	iam.SetIAMIntegration(NewS3IAMIntegration(mgr, ""))

	doc := func(name string) string {
		b, _ := json.Marshal(map[string]interface{}{
			"Version": "2012-10-17",
			"Statement": []map[string]interface{}{
				{"Effect": "Allow", "Action": "s3:*", "Resource": "arn:aws:s3:::" + name + "/*"},
			},
		})
		return string(b)
	}
	allows := func(name string) bool {
		allowed, err := mgr.IsActionAllowed(context.Background(), &integration.ActionRequest{
			Principal:   "arn:aws:iam::111122223333:user/test",
			Action:      "s3:PutObject",
			Resource:    "arn:aws:s3:::" + name + "/file.txt",
			PolicyNames: []string{name},
		})
		require.NoError(t, err)
		return allowed
	}

	require.NoError(t, iam.PutPolicy("alpha", doc("alpha")))
	require.True(t, allows("alpha"))

	// Swap the map contents without going through PutPolicy/DeletePolicy, then
	// resync. A snapshot-based sync would push a pre-swap view; the fresh-read
	// resync must mirror the current map: alpha gone, beta present.
	iam.m.Lock()
	delete(iam.policies, "alpha")
	iam.policies["beta"] = &iam_pb.Policy{Name: "beta", Content: doc("beta")}
	iam.m.Unlock()
	iam.resyncIAMManagerPolicies()

	require.False(t, allows("alpha"), "resync should drop a policy no longer in the map")
	require.True(t, allows("beta"), "resync should add a policy newly in the map")
}
