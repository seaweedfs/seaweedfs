package sftp

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestScopedUserCannotAccessSiblingPrefix verifies that a permission entry
// for /tenants/alice does not leak onto sibling paths such as
// /tenants/alice-archive that merely share the string prefix.
func TestScopedUserCannotAccessSiblingPrefix(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	config := DefaultTestConfig()
	config.EnableDebug = testing.Verbose()

	fw := NewSftpTestFramework(t, config)
	require.NoError(t, fw.Setup(config))
	defer fw.Cleanup()

	// admin seeds both tenants
	admin, adminConn, err := fw.ConnectSFTP("admin", "adminpassword")
	require.NoError(t, err)
	defer adminConn.Close()
	defer admin.Close()

	require.NoError(t, admin.MkdirAll("/tenants/alice"))
	require.NoError(t, admin.MkdirAll("/tenants/alice-archive"))
	secret, err := admin.Create("/tenants/alice-archive/secret.txt")
	require.NoError(t, err)
	_, err = secret.Write([]byte("SIBLING-SECRET"))
	require.NoError(t, err)
	require.NoError(t, secret.Close())

	scoped, scopedConn, err := fw.ConnectSFTP("scoped", "scopedpassword")
	require.NoError(t, err)
	defer scopedConn.Close()
	defer scoped.Close()

	// positive control: the scoped user can write within its own prefix
	own, err := scoped.Create("/tenants/alice/own.txt")
	require.NoError(t, err)
	_, err = own.Write([]byte("ok"))
	require.NoError(t, err)
	require.NoError(t, own.Close())

	// reading the sibling tenant's file must fail
	if f, err := scoped.Open("/tenants/alice-archive/secret.txt"); err == nil {
		data, _ := io.ReadAll(f)
		f.Close()
		t.Fatalf("ACL for /tenants/alice read sibling /tenants/alice-archive: %q", data)
	}

	// overwriting the sibling tenant's file must fail
	if f, err := scoped.Create("/tenants/alice-archive/secret.txt"); err == nil {
		f.Write([]byte("OVERWRITTEN-BY-SCOPED-USER"))
		f.Close()
		t.Fatal("ACL for /tenants/alice overwrote sibling /tenants/alice-archive")
	}

	// the sibling file is intact
	f, err := admin.Open("/tenants/alice-archive/secret.txt")
	require.NoError(t, err)
	data, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Equal(t, "SIBLING-SECRET", string(data))
}
