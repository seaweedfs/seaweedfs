package security

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestDecodeJwtAcceptsPreviousKey(t *testing.T) {
	current, previous, other := SigningKey("current-key"), SigningKey("previous-key"), SigningKey("other-key")
	fid := "3,01637037d6"
	for _, tc := range []struct {
		name     string
		signedBy SigningKey
		previous []SigningKey
		wantOk   bool
	}{
		{name: "current", signedBy: current, wantOk: true},
		{name: "current_with_previous", signedBy: current, previous: []SigningKey{previous}, wantOk: true},
		{name: "previous", signedBy: previous, previous: []SigningKey{previous}, wantOk: true},
		{name: "previous_not_configured", signedBy: previous},
		{name: "empty_previous", signedBy: previous, previous: []SigningKey{nil}},
		{name: "other", signedBy: other, previous: []SigningKey{previous}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token := GenJwtForVolumeServer(tc.signedBy, 60, fid)
			claims := &SeaweedFileIdClaims{}
			parsed, err := DecodeJwt(current, token, claims, tc.previous...)
			if !tc.wantOk {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.True(t, parsed.Valid)
			require.Equal(t, fid, claims.Fid)
		})
	}

	expired, err := jwt.NewWithClaims(jwt.SigningMethodHS256, SeaweedFileIdClaims{
		Fid:              fid,
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Minute))},
	}).SignedString([]byte(previous))
	require.NoError(t, err)
	_, err = DecodeJwt(current, EncodedJwt(expired), &SeaweedFileIdClaims{}, previous)
	require.True(t, errors.Is(err, jwt.ErrTokenExpired), "expired token signed by the previous key: %v", err)
	require.True(t, IsExpiredVolumeJwt(current, EncodedJwt(expired), fid, previous))
	require.False(t, IsExpiredVolumeJwt(current, EncodedJwt(expired), fid))
}

func TestGuardVerificationKeys(t *testing.T) {
	g := NewGuard(nil, "write-a", 10, "read-a", 60, "write-b", "read-b")
	key, previous := g.VerificationKeys(true)
	require.Equal(t, "write-a", string(key))
	require.Equal(t, "write-b", string(previous))
	key, previous = g.VerificationKeys(false)
	require.Equal(t, "read-a", string(key))
	require.Equal(t, "read-b", string(previous))

	g.UpdateSigningKeys("write-b", 10, "read-b", 60, "write-a", "read-a")
	require.Equal(t, "write-b", string(g.SigningKey()))
	key, previous = g.VerificationKeys(true)
	require.Equal(t, "write-b", string(key))
	require.Equal(t, "write-a", string(previous))

	g.UpdateSigningKeys("write-b", 10, "read-b", 60, "", "")
	_, previous = g.VerificationKeys(true)
	require.Empty(t, previous)
	_, previous = g.VerificationKeys(false)
	require.Empty(t, previous)
}

func TestCheckGrpcAdminTokenAcceptsPreviousKey(t *testing.T) {
	current, previous := SigningKey("current-admin-key"), SigningKey("previous-admin-key")
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer "+string(GenJwtForFilerAdmin(previous, 60))))
	require.Error(t, CheckGrpcAdminToken(ctx, current))
	require.NoError(t, CheckGrpcAdminToken(ctx, current, previous))
	require.NoError(t, CheckGrpcAdminPeer(ctx, current, previous))

	_, err := CheckGrpcMetadataSubscription(ctx, SigningKey("subscribe-key"), current, nil, MethodSubscribeMetadata)
	require.Error(t, err)
	scope, err := CheckGrpcMetadataSubscription(ctx, SigningKey("subscribe-key"), current, previous, MethodSubscribeMetadata)
	require.NoError(t, err)
	require.Nil(t, scope)
}

func TestReloadSigningKeysPreviousKeys(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("HOME", dir)
	g := NewGuard(nil, "", 10, "", 60, "", "")
	writeSecurity := func(content string) {
		t.Helper()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "security.toml"), []byte(content), 0o600))
	}
	requirePrevious := func(want, wantRead string) {
		t.Helper()
		_, previous := g.VerificationKeys(true)
		_, previousRead := g.VerificationKeys(false)
		require.Equal(t, want, string(previous))
		require.Equal(t, wantRead, string(previousRead))
	}

	writeSecurity("[jwt.signing]\nprevious_key = \"old\"\n[jwt.signing.read]\nprevious_key = \"old-read\"\n")
	g.ReloadSigningKeys("jwt.signing")
	requirePrevious("old", "old-read")

	writeSecurity("[jwt.signing]\n")
	g.ReloadSigningKeys("jwt.signing")
	requirePrevious("", "")

	writeSecurity("[jwt.signing]\nprevious_key = \"old\"\n[jwt.signing.read]\nprevious_key = \"old-read\"\n")
	g.ReloadSigningKeys("jwt.signing")
	writeSecurity("[jwt.signing\nprevious_key = \n")
	g.ReloadSigningKeys("jwt.signing")
	requirePrevious("old", "old-read")
	require.NoError(t, os.Remove(filepath.Join(dir, "security.toml")))
	g.ReloadSigningKeys("jwt.signing")
	requirePrevious("old", "old-read")
}

func TestReloadSigningKeysTakesSwappedPairFromOneRead(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("HOME", dir)
	v := util.GetViper()
	v.Set("jwt.signing.key", "new")
	t.Cleanup(func() { v.Set("jwt.signing.key", "") })
	require.NoError(t, os.WriteFile(filepath.Join(dir, "security.toml"),
		[]byte("[jwt.signing]\nkey = \"old\"\nprevious_key = \"new\"\n"), 0o600))

	g := NewGuard(nil, "new", 10, "", 60, "old", "")
	g.ReloadSigningKeys("jwt.signing")
	key, previous := g.VerificationKeys(true)
	require.Equal(t, "old", string(key))
	require.Equal(t, "new", string(previous))

	require.NoError(t, os.WriteFile(filepath.Join(dir, "security.toml"), []byte("[jwt.signing]\n"), 0o600))
	g.ReloadSigningKeys("jwt.signing")
	key, previous = g.VerificationKeys(true)
	require.Equal(t, "new", string(key), "a key the file no longer sets keeps its merged value")
	require.Empty(t, previous)
}

func TestFilerAdminKeysFollowSecurityFile(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("HOME", dir)
	file := filepath.Join(dir, "security.toml")

	require.NoError(t, os.WriteFile(file, []byte("[jwt.filer_signing]\nkey = \"new\"\nprevious_key = \"old\"\n"), 0o600))
	key, previous := FilerAdminKeys()
	require.Equal(t, "new", string(key))
	require.Equal(t, "old", string(previous))

	require.NoError(t, os.WriteFile(file, []byte("[jwt.filer_signing]\nkey = \"new\"\n"), 0o600))
	key, previous = FilerAdminKeys()
	require.Equal(t, "new", string(key))
	require.Empty(t, previous, "a previous_key removed from the file must stop verifying")
}

func TestReloadSigningKeysKeepsKeysInEffectOnReadFailure(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("HOME", dir)
	v := util.GetViper()
	v.Set("jwt.signing.key", "old")
	t.Cleanup(func() { v.Set("jwt.signing.key", "") })
	file := filepath.Join(dir, "security.toml")
	require.NoError(t, os.WriteFile(file, []byte("[jwt.signing]\nkey = \"new\"\nprevious_key = \"old\"\n"), 0o600))

	g := NewGuard(nil, "old", 10, "", 60, "", "")
	g.ReloadSigningKeys("jwt.signing")
	key, previous := g.VerificationKeys(true)
	require.Equal(t, "new", string(key))
	require.Equal(t, "old", string(previous))

	require.NoError(t, os.WriteFile(file, []byte("[jwt.signing\nkey = \n"), 0o600))
	g.ReloadSigningKeys("jwt.signing")
	key, previous = g.VerificationKeys(true)
	require.Equal(t, "new", string(key))
	require.Equal(t, "old", string(previous))
}

func TestFilerAdminKeysKeepLastReadPairOnReadFailure(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("HOME", dir)
	v := util.GetViper()
	v.Set("jwt.filer_signing.previous_key", "old")
	t.Cleanup(func() { v.Set("jwt.filer_signing.previous_key", "") })
	file := filepath.Join(dir, "security.toml")

	require.NoError(t, os.WriteFile(file, []byte("[jwt.filer_signing]\nkey = \"new\"\n"), 0o600))
	key, previous := FilerAdminKeys()
	require.Equal(t, "new", string(key))
	require.Empty(t, previous)

	require.NoError(t, os.WriteFile(file, []byte("[jwt.filer_signing\nkey = \n"), 0o600))
	key, previous = FilerAdminKeys()
	require.Equal(t, "new", string(key))
	require.Empty(t, previous, "a retired previous_key must not come back when the file cannot be read")

	require.NoError(t, os.Remove(file))
	key, previous = FilerAdminKeys()
	require.Equal(t, "new", string(key))
	require.Empty(t, previous)
}

func TestFilerAdminKeysReadAndCacheUnderOneLock(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("HOME", dir)
	file := filepath.Join(dir, "security.toml")
	require.NoError(t, os.WriteFile(file, []byte("[jwt.filer_signing]\nkey = \"new\"\nprevious_key = \"old\"\n"), 0o600))

	done := make(chan [2]SigningKey, 1)
	func() {
		filerAdminKeysMu.Lock()
		defer filerAdminKeysMu.Unlock()
		go func() {
			key, previous := FilerAdminKeys()
			done <- [2]SigningKey{key, previous}
		}()
		waitForLockWaiter(t, "security.FilerAdminKeys(")
		require.NoError(t, os.WriteFile(file, []byte("[jwt.filer_signing]\nkey = \"new\"\n"), 0o600))
	}()
	keys := <-done
	require.Equal(t, "new", string(keys[0]))
	require.Empty(t, keys[1], "a caller waiting for the lock must read the file after taking it")

	require.NoError(t, os.WriteFile(file, []byte("[jwt.filer_signing\nkey = \n"), 0o600))
	key, previous := FilerAdminKeys()
	require.Equal(t, "new", string(key))
	require.Empty(t, previous)
}

func waitForLockWaiter(t *testing.T, fn string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	buf := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		n := runtime.Stack(buf, true)
		if n == len(buf) {
			buf = make([]byte, 2*len(buf))
			continue
		}
		for _, g := range strings.Split(string(buf[:n]), "\n\n") {
			if strings.Contains(g, "sync.(*Mutex).lockSlow") && strings.Contains(g, fn) {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no goroutine in %s waited for the lock", fn)
}
