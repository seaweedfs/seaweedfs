package policy

import (
	"fmt"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/stretchr/testify/require"
)

// TestShellPolicyAttachDetach exercises s3.policy.attach and s3.policy.detach.
func TestShellPolicyAttachDetach(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	cluster, err := startMiniCluster(t)
	require.NoError(t, err)
	defer cluster.Stop()

	const weedCmd = "weed"
	master := string(pb.NewServerAddress("127.0.0.1", cluster.masterPort, cluster.masterGrpcPort))
	filer := string(pb.NewServerAddress("127.0.0.1", cluster.filerPort, cluster.filerGrpcPort))

	// Create a policy via file.
	policyJSON := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`
	tmpFile, err := os.CreateTemp("", "test_policy_*.json")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	_, err = tmpFile.WriteString(policyJSON)
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	policyName := uniqueName("attach-pol")
	userName := uniqueName("attach-user")

	execShell(t, weedCmd, master, filer,
		fmt.Sprintf("s3.policy -put -name=%s -file=%s", policyName, tmpFile.Name()))
	defer execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.policy -delete -name=%s", policyName))

	execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.create -name %s", userName))
	defer execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.delete -name %s", userName))

	t.Run("AttachAndVerify", func(t *testing.T) {
		out := execShell(t, weedCmd, master, filer,
			fmt.Sprintf("s3.policy.attach -policy %s -user %s", policyName, userName))
		requireContains(t, out, policyName, "policy.attach output")

		out = execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.show -name %s", userName))
		requireContains(t, out, policyName, "user.show after attach")
	})

	t.Run("AttachIdempotent", func(t *testing.T) {
		// Should succeed without error per the command's idempotent design.
		execShell(t, weedCmd, master, filer,
			fmt.Sprintf("s3.policy.attach -policy %s -user %s", policyName, userName))
	})

	t.Run("DetachAndVerify", func(t *testing.T) {
		out := execShell(t, weedCmd, master, filer,
			fmt.Sprintf("s3.policy.detach -policy %s -user %s", policyName, userName))
		requireContains(t, out, policyName, "policy.detach output")

		out = execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.show -name %s", userName))
		requireNotContains(t, out, fmt.Sprintf("\"%s\"", policyName), "user.show after detach")
	})
}
