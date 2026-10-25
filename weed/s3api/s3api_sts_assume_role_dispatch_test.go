package s3api

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
)

// TestAssumeRoleWithWebIdentity_DispatchesThroughIAMManager confirms the
// public STS HTTP path goes through IAMManager.AssumeRoleWithWebIdentity (and
// thereby its enforceProviderAccountScope check and MaxSessionDuration
// clamp) instead of bypassing to the bare STS service. The two paths surface
// different errors when their underlying service is uninitialized, which is
// the cheapest behavioural signal that doesn't require a full OIDC stack.
func TestAssumeRoleWithWebIdentity_DispatchesThroughIAMManager(t *testing.T) {
	req := &sts.AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:aws:iam::111111111111:role/Test",
		WebIdentityToken: "ignored",
		RoleSessionName:  "session",
	}

	t.Run("with IAMManager wired", func(t *testing.T) {
		mgr := integration.NewIAMManager() // not initialized on purpose
		h := &STSHandlers{
			stsService: nil, // intentionally nil; the wrapper must run first
			iam: &IdentityAccessManagement{
				iamIntegration: NewS3IAMIntegration(mgr, ""),
			},
		}
		_, err := h.assumeRoleWithWebIdentity(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), "IAM manager not initialized") {
			t.Fatalf("expected IAMManager wrapper to handle the call; got err=%v", err)
		}
	})

	t.Run("without IAM integration", func(t *testing.T) {
		h := &STSHandlers{
			stsService: sts.NewSTSService(), // not initialized
			iam:        &IdentityAccessManagement{},
		}
		_, err := h.assumeRoleWithWebIdentity(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), "STS service not initialized") {
			t.Fatalf("expected fallback to bare STS service; got err=%v", err)
		}
	})
}
