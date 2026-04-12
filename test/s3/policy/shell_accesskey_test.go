package policy

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/stretchr/testify/require"
)

// TestShellAccessKeyLifecycle exercises s3.accesskey.* commands end-to-end.
func TestShellAccessKeyLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	cluster, err := startMiniCluster(t)
	require.NoError(t, err)
	defer cluster.Stop()

	const weedCmd = "weed"
	master := string(pb.NewServerAddress("127.0.0.1", cluster.masterPort, cluster.masterGrpcPort))
	filer := string(pb.NewServerAddress("127.0.0.1", cluster.filerPort, cluster.filerGrpcPort))

	userName := uniqueName("akuser")
	// Create user with explicit key so we know the initial value.
	initialAK := "INITIALAK1234567890X"
	initialSK := "initialsecret1234567890abcdefghijklmnop"
	execShell(t, weedCmd, master, filer,
		fmt.Sprintf("s3.user.create -name %s -access_key %s -secret_key %s", userName, initialAK, initialSK))
	defer execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.delete -name %s", userName))

	t.Run("ListInitialKey", func(t *testing.T) {
		out := execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.accesskey.list -user %s", userName))
		requireContains(t, out, initialAK, "accesskey.list initial")
	})

	var createdAK string
	t.Run("CreateAdditionalKey", func(t *testing.T) {
		out := execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.accesskey.create -user %s", userName))
		requireContains(t, out, "Access Key:", "accesskey.create output")
		requireContains(t, out, "Secret Key:", "accesskey.create output")
		createdAK = extractFieldAfter(out, "Access Key:")
		if createdAK == "" {
			t.Fatalf("failed to extract access key from create output:\n%s", out)
		}

		out = execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.accesskey.list -user %s", userName))
		requireContains(t, out, initialAK, "list contains original")
		requireContains(t, out, createdAK, "list contains new key")
	})

	t.Run("RotateKey", func(t *testing.T) {
		if createdAK == "" {
			t.Fatal("createdAK is empty; CreateAdditionalKey must run successfully first")
		}
		out := execShell(t, weedCmd, master, filer,
			fmt.Sprintf("s3.accesskey.rotate -user %s -access_key %s", userName, initialAK))
		requireContains(t, out, initialAK, "rotate shows old key")
		requireContains(t, out, "deleted", "rotate marks old key deleted")

		out = execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.accesskey.list -user %s", userName))
		requireNotContains(t, out, initialAK, "old key removed")
		requireContains(t, out, createdAK, "other key still present")
	})

	t.Run("DeleteKey", func(t *testing.T) {
		if createdAK == "" {
			t.Fatal("createdAK is empty; CreateAdditionalKey must run successfully first")
		}
		execShell(t, weedCmd, master, filer,
			fmt.Sprintf("s3.accesskey.delete -user %s -access_key %s", userName, createdAK))
		out := execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.accesskey.list -user %s", userName))
		requireNotContains(t, out, createdAK, "deleted key removed from list")
	})
}
