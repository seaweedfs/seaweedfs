package s3api

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
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
