package security

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func testSigningKey(t *testing.T) SigningKey {
	t.Helper()
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		t.Fatal(err)
	}
	return SigningKey(hex.EncodeToString(bytes))
}

func TestWithFilerTokenPreservesContextAndMetadata(t *testing.T) {
	key := testSigningKey(t)
	token := GenJwtForFilerAdmin(key, 60)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "trace-id", "trace-value")

	var sourceContext context.Context
	withToken, err := WithFilerToken(ctx, func(got context.Context) (EncodedJwt, error) {
		sourceContext = got
		return token, nil
	})
	if err != nil {
		t.Fatalf("WithFilerToken returned error: %v", err)
	}
	if sourceContext != ctx {
		t.Fatal("token source did not receive the caller context")
	}
	gotDeadline, gotDeadlineOK := withToken.Deadline()
	wantDeadline, wantDeadlineOK := ctx.Deadline()
	if !gotDeadlineOK || !wantDeadlineOK || !gotDeadline.Equal(wantDeadline) {
		t.Fatalf("deadline changed: got (%v, %v), want (%v, %v)", gotDeadline, gotDeadlineOK, wantDeadline, wantDeadlineOK)
	}

	md, ok := metadata.FromOutgoingContext(withToken)
	if !ok || len(md.Get("authorization")) != 1 || md.Get("authorization")[0] != BearerPrefix+string(token) {
		t.Fatal("authorization metadata did not contain exactly one expected bearer token")
	}
	if len(md.Get("trace-id")) != 1 || md.Get("trace-id")[0] != "trace-value" {
		t.Fatal("unrelated outgoing metadata was not preserved")
	}
	original, _ := metadata.FromOutgoingContext(ctx)
	if len(original.Get("authorization")) != 0 {
		t.Fatal("WithFilerToken mutated the caller metadata")
	}

	cancel()
	if withToken.Err() != context.Canceled {
		t.Fatalf("wrapped context error = %v, want cancellation", withToken.Err())
	}
}

func TestWithFilerTokenSkipsExplicitIdentityAndEmptySource(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want string
	}{
		{name: "nil source", ctx: context.Background(), want: ""},
		{
			name: "empty token",
			ctx:  context.Background(),
			want: "",
		},
		{
			name: "caller identity",
			ctx:  metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer caller-token"),
			want: "Bearer caller-token",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			source := FilerTokenSource(nil)
			if tt.name == "empty token" || tt.name == "caller identity" {
				source = func(context.Context) (EncodedJwt, error) {
					calls++
					return "", nil
				}
			}
			got, err := WithFilerToken(tt.ctx, source)
			if err != nil {
				t.Fatalf("WithFilerToken returned error: %v", err)
			}
			md, _ := metadata.FromOutgoingContext(got)
			values := md.Get("authorization")
			if tt.want == "" && len(values) != 0 {
				t.Fatal("unexpected authorization metadata for an anonymous call")
			}
			if tt.want != "" && (len(values) != 1 || values[0] != tt.want) {
				t.Fatal("explicit caller authorization metadata was not preserved")
			}
			if tt.name == "caller identity" && calls != 0 {
				t.Fatalf("source called %d times for explicit caller identity", calls)
			}
		})
	}
}

func TestWithFilerTokenReturnsSourceError(t *testing.T) {
	want := errors.New("token source failed")
	got, err := WithFilerToken(context.Background(), func(context.Context) (EncodedJwt, error) {
		return "", want
	})
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if got != nil {
		t.Fatal("failed token resolution returned a context")
	}
}

type recordingClientConn struct {
	invokes   int
	newStream int
	contexts  []context.Context
}

func (c *recordingClientConn) Invoke(ctx context.Context, _ string, _, _ interface{}, _ ...grpc.CallOption) error {
	c.invokes++
	c.contexts = append(c.contexts, ctx)
	return nil
}

func (c *recordingClientConn) NewStream(ctx context.Context, _ *grpc.StreamDesc, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
	c.newStream++
	c.contexts = append(c.contexts, ctx)
	return nil, nil
}

func TestWithFilerClientAuthProtectsServicePrefix(t *testing.T) {
	key := testSigningKey(t)
	token := GenJwtForFilerAdmin(key, 60)
	calls := 0
	conn := &recordingClientConn{}
	wrapped := WithFilerClientAuth(conn, func(context.Context) (EncodedJwt, error) {
		calls++
		return token, nil
	})

	for _, method := range []string{
		"/other.Service/Call",
		"/filer_pb.SeaweedFilerEvil/Call",
	} {
		if err := wrapped.Invoke(context.Background(), method, nil, nil); err != nil {
			t.Fatalf("Invoke(%q): %v", method, err)
		}
	}
	if calls != 0 {
		t.Fatalf("token source called %d times for non-filer methods", calls)
	}

	if err := wrapped.Invoke(context.Background(), "/filer_pb.SeaweedFiler/LookupDirectoryEntry", nil, nil); err != nil {
		t.Fatalf("filer Invoke: %v", err)
	}
	if calls != 1 {
		t.Fatalf("token source called %d times, want one filer call", calls)
	}
	md, _ := metadata.FromOutgoingContext(conn.contexts[len(conn.contexts)-1])
	if len(md.Get("authorization")) != 1 || !IsFilerAdminToken(key, EncodedJwt(md.Get("authorization")[0][len(BearerPrefix):])) {
		t.Fatal("filer call did not receive a valid admin token")
	}

	if _, err := wrapped.NewStream(context.Background(), &grpc.StreamDesc{}, "/other.Service/Stream"); err != nil {
		t.Fatalf("non-filer NewStream: %v", err)
	}
	if _, err := wrapped.NewStream(context.Background(), &grpc.StreamDesc{}, "/filer_pb.SeaweedFiler/ListEntries"); err != nil {
		t.Fatalf("filer NewStream: %v", err)
	}
	if calls != 2 {
		t.Fatalf("token source called %d times after stream calls, want two", calls)
	}
}

func TestWithFilerClientAuthSourceErrorDoesNotReachConnection(t *testing.T) {
	want := errors.New("token source failed")
	conn := &recordingClientConn{}
	wrapped := WithFilerClientAuth(conn, func(context.Context) (EncodedJwt, error) {
		return "", want
	})
	if err := wrapped.Invoke(context.Background(), "/filer_pb.SeaweedFiler/LookupDirectoryEntry", nil, nil); !errors.Is(err, want) {
		t.Fatal("Invoke did not return the token source error")
	}
	if _, err := wrapped.NewStream(context.Background(), &grpc.StreamDesc{}, "/filer_pb.SeaweedFiler/ListEntries"); !errors.Is(err, want) {
		t.Fatal("NewStream did not return the token source error")
	}
	if conn.invokes != 0 || conn.newStream != 0 {
		t.Fatalf("connection received calls after token source failure: invokes=%d streams=%d", conn.invokes, conn.newStream)
	}
}

func TestFilerAdminTokenKeyPrecedenceAndNoKey(t *testing.T) {
	v := viper.New()
	grpcKey := testSigningKey(t)
	httpKey := testSigningKey(t)
	v.Set("jwt.filer_signing.grpc_key", string(grpcKey))
	v.Set("jwt.filer_signing.key", string(httpKey))
	t.Cleanup(func() {
		v.Set("jwt.filer_signing.grpc_key", "")
		v.Set("jwt.filer_signing.key", "")
	})

	token := filerAdminToken(v)
	if !IsFilerAdminToken(grpcKey, token) || IsFilerAdminToken(httpKey, token) {
		t.Fatal("grpc_key did not take precedence over the HTTP signing key")
	}

	v.Set("jwt.filer_signing.grpc_key", "")
	token = filerAdminToken(v)
	if !IsFilerAdminToken(httpKey, token) {
		t.Fatal("HTTP signing-key fallback did not produce a valid token")
	}
	v.Set("jwt.filer_signing.key", "")
	token = filerAdminToken(v)
	if token != "" {
		t.Fatal("no configured key produced a token")
	}
}

func TestFilerAdminTokenHasTenSecondExpiry(t *testing.T) {
	v := viper.New()
	key := testSigningKey(t)
	v.Set("jwt.filer_signing.grpc_key", string(key))
	token := filerAdminToken(v)
	claims := &SeaweedFilerAdminClaims{}
	parsed, err := DecodeJwt(key, token, claims)
	if err != nil || parsed == nil || !parsed.Valid || claims.ExpiresAt == nil {
		t.Fatal("admin token did not decode with an expiry")
	}
	remaining := time.Until(claims.ExpiresAt.Time)
	if remaining < 9*time.Second || remaining > 10*time.Second {
		t.Fatal("admin token expiry was outside the ten-second window")
	}
}

func writeFilerSecurityToml(t *testing.T, path string, grpcKey, httpKey SigningKey) {
	t.Helper()
	contents := "[jwt.filer_signing]\n"
	if len(grpcKey) > 0 {
		contents += "grpc_key = \"" + string(grpcKey) + "\"\n"
	}
	if len(httpKey) > 0 {
		contents += "key = \"" + string(httpKey) + "\"\n"
	}
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
}

// readFilerClientKeysEveryCall makes token sources re-read their
// configuration on every call, so rotation tests observe each file change.
func readFilerClientKeysEveryCall(t *testing.T) {
	t.Helper()
	saved := filerClientKeyTTL
	filerClientKeyTTL = 0
	resetGlobalFilerClientKeys()
	t.Cleanup(func() {
		filerClientKeyTTL = saved
		resetGlobalFilerClientKeys()
	})
}

func resetGlobalFilerClientKeys() {
	globalFilerClientKeys.mu.Lock()
	globalFilerClientKeys.read = false
	globalFilerClientKeys.known = false
	globalFilerClientKeys.mu.Unlock()
}

func TestFilerAdminTokenReloadsConfigurationAndKeepsLastKeysOnReadError(t *testing.T) {
	readFilerClientKeysEveryCall(t)
	dir := t.TempDir()
	savedDirectory := util.ConfigurationFileDirectory
	util.ConfigurationFileDirectory = util.DirectoryValueType(dir)
	t.Cleanup(func() { util.ConfigurationFileDirectory = savedDirectory })
	path := filepath.Join(dir, "security.toml")
	keyA := testSigningKey(t)
	keyB := testSigningKey(t)
	writeFilerSecurityToml(t, path, keyA, SigningKey(""))

	token, err := FilerAdminToken(context.Background())
	if err != nil || !IsFilerAdminToken(keyA, token) {
		t.Fatal("global source did not use the configured current key")
	}
	writeFilerSecurityToml(t, path, keyB, SigningKey(""))
	token, err = FilerAdminToken(context.Background())
	if err != nil || !IsFilerAdminToken(keyB, token) || IsFilerAdminToken(keyA, token) {
		t.Fatal("global source did not reload the rotated key")
	}
	writeFilerSecurityToml(t, path, SigningKey(""), SigningKey(""))
	token, err = FilerAdminToken(context.Background())
	if err != nil || token != "" {
		t.Fatalf("keyless configuration returned token=%t err=%v", token != "", err)
	}
	writeFilerSecurityToml(t, path, keyA, SigningKey(""))
	if token, err = FilerAdminToken(context.Background()); err != nil || !IsFilerAdminToken(keyA, token) {
		t.Fatal("global source did not read the restored key")
	}
	if err := os.WriteFile(path, []byte("[jwt.filer_signing\n"), 0600); err != nil {
		t.Fatal(err)
	}
	token, err = FilerAdminToken(context.Background())
	if err != nil || !IsFilerAdminToken(keyA, token) {
		t.Fatalf("unreadable configuration did not keep the last keys: token=%t err=%v", token != "", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	token, err = FilerAdminToken(context.Background())
	if err != nil || token != "" {
		t.Fatalf("deleted configuration returned token=%t err=%v", token != "", err)
	}
}

func writeFilerSecurityConfig(t *testing.T, path string, grpcKey, httpKey SigningKey) {
	t.Helper()
	contents := "jwt:\n  filer_signing:\n"
	if len(grpcKey) > 0 {
		contents += "    grpc_key: \"" + string(grpcKey) + "\"\n"
	}
	if len(httpKey) > 0 {
		contents += "    key: \"" + string(httpKey) + "\"\n"
	}
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestFilerAdminTokenFromFileUsesIndependentRotatingSources(t *testing.T) {
	readFilerClientKeysEveryCall(t)
	dir := t.TempDir()
	pathA := filepath.Join(dir, "endpoint-a.yaml")
	pathB := filepath.Join(dir, "endpoint-b.yaml")
	keyA1 := testSigningKey(t)
	keyA2 := testSigningKey(t)
	keyB := testSigningKey(t)
	writeFilerSecurityConfig(t, pathA, keyA1, SigningKey(""))
	writeFilerSecurityConfig(t, pathB, SigningKey(""), keyB)

	sourceA := FilerAdminTokenFromFile(pathA)
	sourceB := FilerAdminTokenFromFile(pathB)
	token, err := sourceA(context.Background())
	if err != nil || !IsFilerAdminToken(keyA1, token) || IsFilerAdminToken(keyA2, token) {
		t.Fatalf("endpoint A initial token valid for expected key=%v wrong key=%v err=%v", IsFilerAdminToken(keyA1, token), IsFilerAdminToken(keyA2, token), err)
	}
	token, err = sourceB(context.Background())
	if err != nil || !IsFilerAdminToken(keyB, token) {
		t.Fatalf("endpoint B token valid=%v err=%v", IsFilerAdminToken(keyB, token), err)
	}

	writeFilerSecurityConfig(t, pathA, keyA2, SigningKey(""))
	token, err = sourceA(context.Background())
	if err != nil || !IsFilerAdminToken(keyA2, token) || IsFilerAdminToken(keyA1, token) {
		t.Fatalf("endpoint A rotated token valid for new key=%v old key=%v err=%v", IsFilerAdminToken(keyA2, token), IsFilerAdminToken(keyA1, token), err)
	}
	writeFilerSecurityConfig(t, pathA, SigningKey(""), SigningKey(""))
	token, err = sourceA(context.Background())
	if err != nil || token != "" {
		t.Fatalf("readable keyless endpoint A returned token=%t err=%v", token != "", err)
	}
	if err := os.Remove(pathA); err != nil {
		t.Fatal(err)
	}
	token, err = sourceA(context.Background())
	if err == nil || token != "" {
		t.Fatalf("removed endpoint A config returned token=%t err=%v", token != "", err)
	}
	// The independent endpoint remains readable after endpoint A is removed.
	token, err = sourceB(context.Background())
	if err != nil || !IsFilerAdminToken(keyB, token) {
		t.Fatalf("endpoint B after endpoint A removal valid=%v err=%v", IsFilerAdminToken(keyB, token), err)
	}
}

// TestFilerAdminTokenFromFileIgnoresEnvironmentOverrides covers filer.sync
// between two clusters: a process-wide WEED_ key must not replace either
// endpoint's key, or one cluster would receive a token valid on the other.
func TestFilerAdminTokenFromFileIgnoresEnvironmentOverrides(t *testing.T) {
	readFilerClientKeysEveryCall(t)
	dir := t.TempDir()
	pathA := filepath.Join(dir, "endpoint-a.yaml")
	pathB := filepath.Join(dir, "endpoint-b.yaml")
	keyA := testSigningKey(t)
	keyB := testSigningKey(t)
	envKey := testSigningKey(t)
	writeFilerSecurityConfig(t, pathA, keyA, SigningKey(""))
	writeFilerSecurityConfig(t, pathB, SigningKey(""), keyB)
	t.Setenv("WEED_JWT_FILER_SIGNING_GRPC_KEY", string(envKey))
	t.Setenv("WEED_JWT_FILER_SIGNING_KEY", string(envKey))

	for _, endpoint := range []struct {
		path string
		key  SigningKey
	}{{pathA, keyA}, {pathB, keyB}} {
		token, err := FilerAdminTokenFromFile(endpoint.path)(context.Background())
		if err != nil || !IsFilerAdminToken(endpoint.key, token) || IsFilerAdminToken(envKey, token) {
			t.Fatalf("%s: token valid for its own key=%v, for the environment key=%v, err=%v",
				filepath.Base(endpoint.path), IsFilerAdminToken(endpoint.key, token), IsFilerAdminToken(envKey, token), err)
		}
	}
	// A file without keys stays keyless rather than adopting the environment's.
	keyless := filepath.Join(dir, "keyless.yaml")
	writeFilerSecurityConfig(t, keyless, SigningKey(""), SigningKey(""))
	if token, err := FilerAdminTokenFromFile(keyless)(context.Background()); err != nil || token != "" {
		t.Fatalf("keyless endpoint returned token=%t err=%v", token != "", err)
	}
}

func TestFilerAdminTokenFromFileFailsClosedForMissingOrMalformedConfig(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.yaml")
	source := FilerAdminTokenFromFile(missing)
	token, err := source(context.Background())
	if err == nil || token != "" {
		t.Fatalf("missing config returned token=%t err=%v", token != "", err)
	}

	malformed := filepath.Join(dir, "malformed.yaml")
	if err := os.WriteFile(malformed, []byte("jwt:\n  filer_signing: [\n"), 0600); err != nil {
		t.Fatal(err)
	}
	token, err = FilerAdminTokenFromFile(malformed)(context.Background())
	if err == nil || token != "" {
		t.Fatalf("malformed config returned token=%t err=%v", token != "", err)
	}
}

func TestFilerAdminTokenCachesKeysWithinRefreshInterval(t *testing.T) {
	saved := filerClientKeyTTL
	filerClientKeyTTL = time.Hour
	resetGlobalFilerClientKeys()
	t.Cleanup(func() {
		filerClientKeyTTL = saved
		resetGlobalFilerClientKeys()
	})
	dir := t.TempDir()
	savedDirectory := util.ConfigurationFileDirectory
	util.ConfigurationFileDirectory = util.DirectoryValueType(dir)
	t.Cleanup(func() { util.ConfigurationFileDirectory = savedDirectory })
	path := filepath.Join(dir, "security.toml")
	keyA := testSigningKey(t)
	keyB := testSigningKey(t)
	writeFilerSecurityToml(t, path, keyA, SigningKey(""))

	token, err := FilerAdminToken(context.Background())
	if err != nil || !IsFilerAdminToken(keyA, token) {
		t.Fatal("global source did not use the configured key")
	}
	writeFilerSecurityToml(t, path, keyB, SigningKey(""))
	token, err = FilerAdminToken(context.Background())
	if err != nil || !IsFilerAdminToken(keyA, token) {
		t.Fatal("global source re-read its configuration within the refresh interval")
	}

	globalFilerClientKeys.mu.Lock()
	globalFilerClientKeys.readAt = time.Now().Add(-2 * time.Hour)
	globalFilerClientKeys.mu.Unlock()
	token, err = FilerAdminToken(context.Background())
	if err != nil || !IsFilerAdminToken(keyB, token) || IsFilerAdminToken(keyA, token) {
		t.Fatal("global source did not pick up the rotated key after the refresh interval")
	}

	// Changing an environment override takes effect at once.
	envKey := testSigningKey(t)
	t.Setenv("WEED_JWT_FILER_SIGNING_GRPC_KEY", string(envKey))
	if token, err = FilerAdminToken(context.Background()); err != nil || !IsFilerAdminToken(envKey, token) {
		t.Fatal("global source did not follow a changed environment override")
	}
	os.Unsetenv("WEED_JWT_FILER_SIGNING_GRPC_KEY")
	if token, err = FilerAdminToken(context.Background()); err != nil || !IsFilerAdminToken(keyB, token) {
		t.Fatal("global source did not follow a removed environment override")
	}

	// A failed read keeps the last keys and is not cached: the next call
	// reads the repaired file without waiting for the interval.
	if err := os.WriteFile(path, []byte("[jwt.filer_signing\n"), 0600); err != nil {
		t.Fatal(err)
	}
	globalFilerClientKeys.mu.Lock()
	globalFilerClientKeys.readAt = time.Now().Add(-2 * time.Hour)
	globalFilerClientKeys.mu.Unlock()
	if token, err = FilerAdminToken(context.Background()); err != nil || !IsFilerAdminToken(keyB, token) {
		t.Fatalf("unreadable configuration did not keep the last keys: token=%t err=%v", token != "", err)
	}
	writeFilerSecurityToml(t, path, keyA, SigningKey(""))
	if token, err = FilerAdminToken(context.Background()); err != nil || !IsFilerAdminToken(keyA, token) {
		t.Fatal("global source did not recover after the configuration was repaired")
	}
}

func TestFilerAdminTokenUnreadableConfigurationKeepsStartupKeys(t *testing.T) {
	readFilerClientKeysEveryCall(t)
	dir := t.TempDir()
	savedDirectory := util.ConfigurationFileDirectory
	util.ConfigurationFileDirectory = util.DirectoryValueType(dir)
	t.Cleanup(func() { util.ConfigurationFileDirectory = savedDirectory })
	if err := os.WriteFile(filepath.Join(dir, "security.toml"), []byte("[jwt.filer_signing\n"), 0600); err != nil {
		t.Fatal(err)
	}
	startup := util.GetViper()
	savedGrpc, savedShared := startup.GetString("jwt.filer_signing.grpc_key"), startup.GetString("jwt.filer_signing.key")
	t.Cleanup(func() {
		startup.Set("jwt.filer_signing.grpc_key", savedGrpc)
		startup.Set("jwt.filer_signing.key", savedShared)
	})

	// A process that never read a key keeps its tokenless behavior.
	startup.Set("jwt.filer_signing.grpc_key", "")
	startup.Set("jwt.filer_signing.key", "")
	if token, err := FilerAdminToken(context.Background()); err != nil || token != "" {
		t.Fatalf("unreadable configuration without startup keys returned token=%t err=%v", token != "", err)
	}
	key := testSigningKey(t)
	startup.Set("jwt.filer_signing.key", string(key))
	if token, err := FilerAdminToken(context.Background()); err != nil || !IsFilerAdminToken(key, token) {
		t.Fatalf("unreadable configuration did not use the startup key: err=%v", err)
	}
}

func TestFilerSubscriptionTokensPreferSharedKey(t *testing.T) {
	grpcKey := testSigningKey(t)
	sharedKey := testSigningKey(t)
	bearer := func(ctx context.Context) EncodedJwt {
		t.Helper()
		md, _ := metadata.FromOutgoingContext(ctx)
		values := md.Get("authorization")
		if len(values) != 1 {
			t.Fatalf("got %d authorization values, want 1", len(values))
		}
		return EncodedJwt(values[0][len(BearerPrefix):])
	}

	for _, tc := range []struct {
		name                 string
		grpcKey, sharedKey   SigningKey
		wantCall, wantStream SigningKey
	}{
		{name: "both keys", grpcKey: grpcKey, sharedKey: sharedKey, wantCall: grpcKey, wantStream: sharedKey},
		{name: "grpc key only", grpcKey: grpcKey, wantCall: grpcKey, wantStream: grpcKey},
		{name: "shared key only", sharedKey: sharedKey, wantCall: sharedKey, wantStream: sharedKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := viper.New()
			v.Set("jwt.filer_signing.grpc_key", string(tc.grpcKey))
			v.Set("jwt.filer_signing.key", string(tc.sharedKey))
			source := func(ctx context.Context) (EncodedJwt, error) {
				return filerAdminTokenForKeys(ctx, SigningKey(v.GetString("jwt.filer_signing.grpc_key")), SigningKey(v.GetString("jwt.filer_signing.key"))), nil
			}
			conn := &recordingClientConn{}
			wrapped := WithFilerClientAuth(conn, source)

			if err := wrapped.Invoke(context.Background(), "/filer_pb.SeaweedFiler/LookupDirectoryEntry", nil, nil); err != nil {
				t.Fatal(err)
			}
			if !IsFilerAdminToken(tc.wantCall, bearer(conn.contexts[len(conn.contexts)-1])) {
				t.Fatal("unary filer call was not signed with the filesystem gRPC key")
			}
			if _, err := wrapped.NewStream(context.Background(), &grpc.StreamDesc{}, "/filer_pb.SeaweedFiler/ListEntries"); err != nil {
				t.Fatal(err)
			}
			if !IsFilerAdminToken(tc.wantCall, bearer(conn.contexts[len(conn.contexts)-1])) {
				t.Fatal("non-subscription stream was not signed with the filesystem gRPC key")
			}
			for _, method := range []string{"/filer_pb.SeaweedFiler/SubscribeMetadata", "/filer_pb.SeaweedFiler/SubscribeLocalMetadata"} {
				if _, err := wrapped.NewStream(context.Background(), &grpc.StreamDesc{}, method); err != nil {
					t.Fatal(err)
				}
				if !IsFilerAdminToken(tc.wantStream, bearer(conn.contexts[len(conn.contexts)-1])) {
					t.Fatalf("%s was not signed with the expected subscription key", method)
				}
			}
			ctx, err := WithFilerSubscriptionToken(context.Background(), source)
			if err != nil || !IsFilerAdminToken(tc.wantStream, bearer(ctx)) {
				t.Fatal("WithFilerSubscriptionToken did not prefer the shared key")
			}
		})
	}
}

// TestFilerClientKeysRefuseKeysTooShortToSign covers GODEBUG=fips140=only,
// where signing with a short key panics: a key read after startup is refused
// the way startup refuses it, so a source never signs with it.
func TestFilerClientKeysRefuseKeysTooShortToSign(t *testing.T) {
	readFilerClientKeysEveryCall(t)
	saved := filerClientKeyCheck
	filerClientKeyCheck = func(settings, keys []string) error {
		for i, key := range keys {
			if key != "" && len(key) < util.Fips140MinHmacKeyBytes {
				return errors.New(settings[i] + " is too short")
			}
		}
		return nil
	}
	t.Cleanup(func() { filerClientKeyCheck = saved })
	short := SigningKey("0123456789")

	dir := t.TempDir()
	endpoint := filepath.Join(dir, "endpoint.yaml")
	writeFilerSecurityConfig(t, endpoint, short, testSigningKey(t))
	if token, err := FilerAdminTokenFromFile(endpoint)(context.Background()); err == nil || token != "" || !strings.Contains(err.Error(), "jwt.filer_signing.grpc_key") {
		t.Fatalf("endpoint file with a short grpc_key returned token=%t err=%v", token != "", err)
	}

	savedDirectory := util.ConfigurationFileDirectory
	util.ConfigurationFileDirectory = util.DirectoryValueType(dir)
	t.Cleanup(func() { util.ConfigurationFileDirectory = savedDirectory })
	path := filepath.Join(dir, "security.toml")
	key := testSigningKey(t)
	writeFilerSecurityToml(t, path, key, SigningKey(""))
	if token, err := FilerAdminToken(context.Background()); err != nil || !IsFilerAdminToken(key, token) {
		t.Fatalf("global source did not use the configured key: err=%v", err)
	}
	writeFilerSecurityToml(t, path, short, SigningKey(""))
	if token, err := FilerAdminToken(context.Background()); err != nil || !IsFilerAdminToken(key, token) {
		t.Fatalf("global source did not keep the last usable key over a short one: token=%t err=%v", token != "", err)
	}

	// With no usable key read yet, a short startup key is refused too.
	resetGlobalFilerClientKeys()
	startup := util.GetViper()
	savedShared := startup.GetString("jwt.filer_signing.key")
	t.Cleanup(func() { startup.Set("jwt.filer_signing.key", savedShared) })
	startup.Set("jwt.filer_signing.key", string(short))
	if token, err := FilerAdminToken(context.Background()); err == nil || token != "" {
		t.Fatalf("short startup key returned token=%t err=%v", token != "", err)
	}
}
