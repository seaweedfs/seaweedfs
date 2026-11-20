package security

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
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
