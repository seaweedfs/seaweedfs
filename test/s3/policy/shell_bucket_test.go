package policy

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/stretchr/testify/require"
)

// TestShellBucketLifecycle exercises s3.bucket.* commands end-to-end:
// create/list/delete, owner, quota, versioning, lock, quota.enforce.
func TestShellBucketLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	cluster, err := startMiniCluster(t)
	require.NoError(t, err)
	defer cluster.Stop()

	const weedCmd = "weed"
	master := string(pb.NewServerAddress("127.0.0.1", cluster.masterPort, cluster.masterGrpcPort))
	filer := string(pb.NewServerAddress("127.0.0.1", cluster.filerPort, cluster.filerGrpcPort))

	t.Run("CreateListDelete", func(t *testing.T) {
		bucketName := uniqueName("bkt")
		out := execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.create -name %s", bucketName))
		requireContains(t, out, bucketName, "bucket.create output")

		out = execShell(t, weedCmd, master, filer, "s3.bucket.list")
		requireContains(t, out, bucketName, "bucket.list contains created")

		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.delete -name %s", bucketName))

		out = execShell(t, weedCmd, master, filer, "s3.bucket.list")
		requireNotContains(t, out, bucketName, "bucket.list after delete")
	})

	t.Run("Owner", func(t *testing.T) {
		bucketName := uniqueName("bkt-own")
		ownerName := uniqueName("owner")
		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.create -name %s", ownerName))
		defer execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.delete -name %s", ownerName))

		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.create -name %s", bucketName))
		defer execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.delete -name %s", bucketName))

		// Initially no owner.
		out := execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.owner -name %s", bucketName))
		requireContains(t, out, "none", "initial owner none")

		// Set owner.
		execShell(t, weedCmd, master, filer,
			fmt.Sprintf("s3.bucket.owner -name %s -owner %s", bucketName, ownerName))
		out = execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.owner -name %s", bucketName))
		requireContains(t, out, ownerName, "owner set")

		// Remove owner.
		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.owner -name %s -delete", bucketName))
		out = execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.owner -name %s", bucketName))
		requireContains(t, out, "none", "owner removed")
	})

	t.Run("Quota", func(t *testing.T) {
		bucketName := uniqueName("bkt-quota")
		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.create -name %s", bucketName))
		defer execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.delete -name %s", bucketName))

		execShell(t, weedCmd, master, filer,
			fmt.Sprintf("s3.bucket.quota -name %s -op=set -sizeMB=1024", bucketName))

		out := execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.quota -name %s -op=get", bucketName))
		requireContains(t, out, "1024", "quota.get shows size")

		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.quota -name %s -op=disable", bucketName))
		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.quota -name %s -op=enable", bucketName))

		// Enforce should run on an empty bucket without error.
		execShell(t, weedCmd, master, filer, "s3.bucket.quota.enforce -apply")

		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.quota -name %s -op=remove", bucketName))
	})

	t.Run("Versioning", func(t *testing.T) {
		bucketName := uniqueName("bkt-ver")
		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.create -name %s", bucketName))
		defer execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.delete -name %s", bucketName))

		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.versioning -name %s -enable", bucketName))
		out := execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.versioning -name %s", bucketName))
		requireContains(t, out, "Enabled", "versioning enabled")

		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.versioning -name %s -suspend", bucketName))
		out = execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.versioning -name %s", bucketName))
		requireContains(t, out, "Suspended", "versioning suspended")
	})

	t.Run("Lock", func(t *testing.T) {
		bucketName := uniqueName("bkt-lock")
		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.create -name %s", bucketName))
		defer execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.delete -name %s", bucketName))

		out := execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.lock -name %s", bucketName))
		requireContains(t, out, "Disabled", "lock initially disabled")

		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.lock -name %s -enable", bucketName))

		out = execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.lock -name %s", bucketName))
		requireContains(t, out, "Enabled", "lock enabled")

		// Versioning should have been auto-enabled.
		out = execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.versioning -name %s", bucketName))
		requireContains(t, out, "Enabled", "versioning auto-enabled by lock")
	})

	t.Run("CreateWithLock", func(t *testing.T) {
		bucketName := uniqueName("bkt-wlock")
		out := execShell(t, weedCmd, master, filer,
			fmt.Sprintf("s3.bucket.create -name %s -withLock", bucketName))
		// Cleanup may fail if the bucket contains locked objects; we created none.
		defer execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.delete -name %s", bucketName))

		requireContains(t, out, "Object Lock", "create -withLock output")

		out = execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.bucket.lock -name %s", bucketName))
		requireContains(t, out, "Enabled", "lock enabled after create -withLock")
	})
}
