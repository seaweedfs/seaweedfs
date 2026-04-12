package policy

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/stretchr/testify/require"
)

// TestShellUserLifecycle exercises the s3.user.* commands end-to-end:
// create, show, list, enable, disable, delete, and provision.
func TestShellUserLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	cluster, err := startMiniCluster(t)
	require.NoError(t, err)
	defer cluster.Stop()

	const weedCmd = "weed"
	master := string(pb.NewServerAddress("127.0.0.1", cluster.masterPort, cluster.masterGrpcPort))
	filer := string(pb.NewServerAddress("127.0.0.1", cluster.filerPort, cluster.filerGrpcPort))

	t.Run("CreateShowListDelete", func(t *testing.T) {
		userName := uniqueName("user")

		out := execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.create -name %s", userName))
		requireContains(t, out, userName, "user.create output")
		requireContains(t, out, "access_key", "user.create JSON")
		requireContains(t, out, "Secret Key:", "user.create stderr")

		out = execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.show -name %s", userName))
		requireContains(t, out, userName, "user.show")

		out = execShell(t, weedCmd, master, filer, "s3.user.list")
		requireContains(t, out, userName, "user.list")

		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.delete -name %s", userName))

		out = execShell(t, weedCmd, master, filer, "s3.user.list")
		requireNotContains(t, out, userName, "user.list after delete")
	})

	t.Run("CreateWithExplicitKeys", func(t *testing.T) {
		userName := uniqueName("user-expl")
		ak := "TESTAK1234567890ABCD"
		sk := "testsecretkey1234567890abcdefghijklmnopq"

		out := execShell(t, weedCmd, master, filer,
			fmt.Sprintf("s3.user.create -name %s -access_key %s -secret_key %s", userName, ak, sk))
		requireContains(t, out, ak, "user.create with explicit keys")

		out = execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.show -name %s", userName))
		requireContains(t, out, ak, "user.show reveals access key")

		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.delete -name %s", userName))
	})

	t.Run("EnableDisable", func(t *testing.T) {
		userName := uniqueName("user-toggle")

		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.create -name %s", userName))

		out := execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.disable -name %s", userName))
		requireContains(t, out, "disabled", "user.disable")

		out = execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.show -name %s", userName))
		requireContains(t, out, "disabled", "user.show after disable")

		out = execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.enable -name %s", userName))
		requireContains(t, out, "enabled", "user.enable")

		out = execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.show -name %s", userName))
		requireContains(t, out, "enabled", "user.show after enable")

		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.delete -name %s", userName))
	})

	t.Run("Provision", func(t *testing.T) {
		userName := uniqueName("prov-user")
		bucketName := uniqueName("prov-bkt")

		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.create -name %s", bucketName))

		out := execShell(t, weedCmd, master, filer,
			fmt.Sprintf("s3.user.provision -name %s -bucket %s -role readwrite", userName, bucketName))
		requireContains(t, out, "Created policy", "provision output")
		requireContains(t, out, "Created user", "provision output")
		requireContains(t, out, "Access Key:", "provision credentials")
		requireContains(t, out, "Secret Key:", "provision credentials")

		out = execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.show -name %s", userName))
		requireContains(t, out, userName, "user.show after provision")

		// Second call with same user but different bucket/role should succeed without creating duplicate user.
		bucket2 := uniqueName("prov-bkt2")
		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.create -name %s", bucket2))
		out = execShell(t, weedCmd, master, filer,
			fmt.Sprintf("s3.user.provision -name %s -bucket %s -role readonly", userName, bucket2))
		requireContains(t, out, "already exists", "provision on existing user")
		requireContains(t, out, "Created policy", "second policy created")
		requireNotContains(t, out, "Access Key:", "no new credentials for existing user")

		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.delete -name %s", userName))
		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.delete -name %s", bucketName))
		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.delete -name %s", bucket2))
	})
}
