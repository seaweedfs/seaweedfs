package policy

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/stretchr/testify/require"
)

// TestShellServiceAccountLifecycle exercises s3.serviceaccount.* commands end-to-end.
func TestShellServiceAccountLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	cluster, err := startMiniCluster(t)
	require.NoError(t, err)
	defer cluster.Stop()

	const weedCmd = "weed"
	master := string(pb.NewServerAddress("127.0.0.1", cluster.masterPort, cluster.masterGrpcPort))
	filer := string(pb.NewServerAddress("127.0.0.1", cluster.filerPort, cluster.filerGrpcPort))

	userName := uniqueName("sauser")
	execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.create -name %s", userName))
	defer execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.user.delete -name %s", userName))

	var saID string

	t.Run("CreateAndList", func(t *testing.T) {
		description := "integration-test-sa"
		out := execShell(t, weedCmd, master, filer,
			fmt.Sprintf("s3.serviceaccount.create -user %s -description %s", userName, description))
		requireContains(t, out, "Created service account", "serviceaccount.create")
		requireContains(t, out, "Access Key:", "serviceaccount.create credentials")
		requireContains(t, out, "Secret Key:", "serviceaccount.create credentials")

		out = execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.serviceaccount.list -user %s", userName))
		requireContains(t, out, userName, "serviceaccount.list parent column")
		saID = extractServiceAccountID(t, out, userName)
	})

	t.Run("Show", func(t *testing.T) {
		if saID == "" {
			t.Skip("no saID extracted")
		}
		out := execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.serviceaccount.show -id %s", saID))
		requireContains(t, out, saID, "show contains id")
		requireContains(t, out, userName, "show contains parent")
		requireContains(t, out, "enabled", "show contains status")
	})

	t.Run("CreateWithActions", func(t *testing.T) {
		out := execShell(t, weedCmd, master, filer,
			fmt.Sprintf("s3.serviceaccount.create -user %s -actions Read,List -expiry 24h", userName))
		requireContains(t, out, "Access Key:", "serviceaccount.create with options")

		out = execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.serviceaccount.list -user %s", userName))
		// Should now have at least 2 service accounts — count lines that start with parent in field[1]
		count := 0
		for _, line := range splitLines(out) {
			fields := fieldsOf(line)
			if len(fields) >= 2 && fields[1] == userName {
				count++
			}
		}
		if count < 2 {
			t.Errorf("expected at least 2 service accounts for %s, got %d\n%s", userName, count, out)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		if saID == "" {
			t.Skip("no saID extracted")
		}
		execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.serviceaccount.delete -id %s", saID))
		out := execShell(t, weedCmd, master, filer, fmt.Sprintf("s3.serviceaccount.list -user %s", userName))
		requireNotContains(t, out, saID, "deleted sa removed from list")
	})
}
