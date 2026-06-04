package s3api

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/seaweedfs/seaweedfs/weed/credential/memory"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

// An advanced -iam.config file (STS/OIDC/roles) carries no inline identities, so the
// server must not enter static-config mode. Otherwise it freezes live reloads and
// filer-backed identities created at runtime (e.g. by the operator's IAM CRDs) never
// take effect.
func TestIamConfigWithoutIdentitiesIsNotStatic(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	path := writeTempIamConfig(t, `{"sts":{"signingKey":"dGVzdC1zaWduaW5nLWtleQ=="}}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(path); err != nil {
		t.Fatalf("failed to load advanced iam config: %v", err)
	}
	s3a.iam.markStaticIdentities()

	if s3a.iam.IsStaticConfig() {
		t.Fatalf("advanced iam config without identities must not be treated as static")
	}

	// A filer change (operator creating a user) must still reload at runtime.
	if err := s3a.iam.credentialManager.CreateUser(context.Background(), &iam_pb.Identity{Name: "alice"}); err != nil {
		t.Fatalf("failed to create alice: %v", err)
	}
	if err := s3a.onIamConfigChange(filer.IamConfigDirectory+"/identities", nil, &filer_pb.Entry{Name: "alice.json"}); err != nil {
		t.Fatalf("onIamConfigChange returned error: %v", err)
	}
	if !hasIdentity(s3a.iam, "alice") {
		t.Fatalf("expected alice to load after filer change with -iam.config-only setup")
	}
}

// A -config identity file marks its identities static, protecting them and keeping the
// established behavior of not live-reloading those from the filer.
func TestConfigWithIdentitiesIsStatic(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	path := writeTempIamConfig(t, `{"identities":[{"name":"static-admin","credentials":[{"accessKey":"AKIAITEST","secretKey":"c2VjcmV0"}],"actions":["Admin"]}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(path); err != nil {
		t.Fatalf("failed to load identity config: %v", err)
	}
	s3a.iam.markStaticIdentities()

	if !s3a.iam.IsStaticConfig() {
		t.Fatalf("config file with inline identities must be treated as static")
	}

	s3a.iam.m.RLock()
	id := s3a.iam.nameToIdentity["static-admin"]
	s3a.iam.m.RUnlock()
	if id == nil || !id.IsStatic {
		t.Fatalf("expected static-admin to be marked static")
	}

	// A static identity file does not live-reload dynamic identities from the filer.
	if err := s3a.iam.credentialManager.CreateUser(context.Background(), &iam_pb.Identity{Name: "alice"}); err != nil {
		t.Fatalf("failed to create alice: %v", err)
	}
	if err := s3a.onIamConfigChange(filer.IamConfigDirectory+"/identities", nil, &filer_pb.Entry{Name: "alice.json"}); err != nil {
		t.Fatalf("onIamConfigChange returned error: %v", err)
	}
	if hasIdentity(s3a.iam, "alice") {
		t.Fatalf("did not expect alice to load while running off a static identity file")
	}
}

func writeTempIamConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "iam.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}
	return path
}
