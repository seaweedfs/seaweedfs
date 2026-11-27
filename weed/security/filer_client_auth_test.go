package security

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
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

func TestFilerAdminTokenReloadsConfigurationAndFailsClosed(t *testing.T) {
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
	if err := os.WriteFile(path, []byte("[jwt.filer_signing\n"), 0600); err != nil {
		t.Fatal(err)
	}
	token, err = FilerAdminToken(context.Background())
	if err == nil || token != "" {
		t.Fatalf("malformed configuration returned token=%t err=%v", token != "", err)
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
