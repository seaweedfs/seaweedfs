package s3api

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
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
	waitForIdentity(t, s3a.iam, "alice")
}

// A -config identity file protects its identities but must not block live
// delivery of filer-managed identities to a running gateway.
func TestConfigWithIdentitiesStillLiveReloadsDynamic(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	path := writeTempIamConfig(t, `{"identities":[{"name":"static-admin","credentials":[{"accessKey":"AKIAITEST","secretKey":"c2VjcmV0"}],"actions":["Admin"]}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(path); err != nil {
		t.Fatalf("failed to load identity config: %v", err)
	}

	if !s3a.iam.IsStaticConfig() {
		t.Fatalf("config file with inline identities must be treated as static")
	}

	s3a.iam.m.RLock()
	id := s3a.iam.nameToIdentity["static-admin"]
	s3a.iam.m.RUnlock()
	if id == nil || !id.IsStatic {
		t.Fatalf("expected static-admin to be marked static")
	}

	if err := s3a.iam.credentialManager.CreateUser(context.Background(), &iam_pb.Identity{Name: "alice"}); err != nil {
		t.Fatalf("failed to create alice: %v", err)
	}
	if err := s3a.onIamConfigChange(filer.IamConfigDirectory+"/identities", nil, &filer_pb.Entry{Name: "alice.json"}); err != nil {
		t.Fatalf("onIamConfigChange returned error: %v", err)
	}
	waitForIdentity(t, s3a.iam, "alice")
	if !hasIdentity(s3a.iam, "static-admin") {
		t.Fatalf("static-admin must survive the dynamic reload")
	}

	// deletion on the filer must revoke on the running gateway too
	if err := s3a.iam.credentialManager.DeleteUser(context.Background(), "alice"); err != nil {
		t.Fatalf("failed to delete alice: %v", err)
	}
	if err := s3a.onIamConfigChange(filer.IamConfigDirectory+"/identities", &filer_pb.Entry{Name: "alice.json"}, nil); err != nil {
		t.Fatalf("onIamConfigChange returned error: %v", err)
	}
	waitForIdentityGone(t, s3a.iam, "alice")
	if !hasIdentity(s3a.iam, "static-admin") {
		t.Fatalf("static-admin must survive the deletion reload")
	}
}

// A single pushed identity (PutIdentity) is a partial merge and must not wipe
// other dynamic identities or a dynamic anonymous identity.
func TestUpsertIdentityKeepsOtherDynamicIdentities(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	path := writeTempIamConfig(t, `{"identities":[{"name":"static-admin","credentials":[{"accessKey":"AKIAITEST","secretKey":"c2VjcmV0"}],"actions":["Admin"]}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(path); err != nil {
		t.Fatalf("failed to load identity config: %v", err)
	}

	for _, name := range []string{"alice", "anonymous"} {
		if err := s3a.iam.credentialManager.CreateUser(context.Background(), &iam_pb.Identity{Name: name}); err != nil {
			t.Fatalf("failed to create %s: %v", name, err)
		}
	}
	if err := s3a.iam.LoadS3ApiConfigurationFromCredentialManager(); err != nil {
		t.Fatalf("failed to load from credential manager: %v", err)
	}

	if err := s3a.iam.UpsertIdentity(&iam_pb.Identity{Name: "bob", Actions: []string{"Read"}}); err != nil {
		t.Fatalf("failed to upsert bob: %v", err)
	}
	for _, name := range []string{"static-admin", "alice", "anonymous", "bob"} {
		if !hasIdentity(s3a.iam, name) {
			t.Fatalf("expected %s to survive a partial upsert", name)
		}
	}
}

// Reloading the static config file (grace.OnReload) must mark newly added
// identities as static so dynamic filer updates can't overwrite them, while
// leaving already-loaded dynamic (filer-managed) identities untouched.
func TestReloadStaticConfigMarksNewIdentitiesWithoutFreezingDynamic(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	p1 := writeTempIamConfig(t, `{"identities":[{"name":"static-admin","credentials":[{"accessKey":"AKADMIN0","secretKey":"c2VjcmV0"}],"actions":["Admin"]}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(p1); err != nil {
		t.Fatalf("failed to load initial config: %v", err)
	}

	// A dynamic identity arrives from the filer; merge mode keeps it dynamic.
	if err := s3a.iam.credentialManager.CreateUser(context.Background(), &iam_pb.Identity{Name: "alice"}); err != nil {
		t.Fatalf("failed to create alice: %v", err)
	}
	if err := s3a.iam.LoadS3ApiConfigurationFromCredentialManager(); err != nil {
		t.Fatalf("failed to load from credential manager: %v", err)
	}
	if !hasIdentity(s3a.iam, "alice") {
		t.Fatalf("expected alice to load dynamically")
	}

	// Reload the static file with a new identity bob.
	p2 := writeTempIamConfig(t, `{"identities":[{"name":"static-admin","credentials":[{"accessKey":"AKADMIN0","secretKey":"c2VjcmV0"}],"actions":["Admin"]},{"name":"bob","credentials":[{"accessKey":"AKBOB000","secretKey":"c2VjcmV0"}],"actions":["Read"]}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(p2); err != nil {
		t.Fatalf("failed to reload config: %v", err)
	}

	if !isStaticName(s3a.iam, "bob") {
		t.Fatalf("expected reloaded identity bob to be marked static")
	}
	if isStaticName(s3a.iam, "alice") {
		t.Fatalf("dynamic identity alice must not be frozen as static by a config reload")
	}
}

// A config-file reload must apply an edited secretKey to its static identity.
func TestReloadStaticConfigUpdatesExistingSecretKey(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	p1 := writeTempIamConfig(t, `{"identities":[{"name":"static-admin","credentials":[{"accessKey":"AKADMIN0","secretKey":"b2xkc2VjcmV0"}],"actions":["Admin"]}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(p1); err != nil {
		t.Fatalf("failed to load initial config: %v", err)
	}

	_, cred, found := s3a.iam.lookupByAccessKey("AKADMIN0")
	if !found || cred.SecretKey != "b2xkc2VjcmV0" {
		t.Fatalf("expected initial secretKey to load, got found=%v cred=%+v", found, cred)
	}

	// Rotate the secretKey in the file and reload.
	p2 := writeTempIamConfig(t, `{"identities":[{"name":"static-admin","credentials":[{"accessKey":"AKADMIN0","secretKey":"bmV3c2VjcmV0"}],"actions":["Admin"]}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(p2); err != nil {
		t.Fatalf("failed to reload config: %v", err)
	}

	_, cred, found = s3a.iam.lookupByAccessKey("AKADMIN0")
	if !found {
		t.Fatalf("static-admin access key disappeared after reload")
	}
	if cred.SecretKey != "bmV3c2VjcmV0" {
		t.Fatalf("expected reloaded secretKey bmV3c2VjcmV0, got %q", cred.SecretKey)
	}
	if !isStaticName(s3a.iam, "static-admin") {
		t.Fatalf("static-admin must stay marked static after reload")
	}
}

// A reload must also reapply a service-account credential under a static parent.
func TestReloadStaticConfigUpdatesServiceAccountSecret(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	p1 := writeTempIamConfig(t, `{"identities":[{"name":"static-admin","credentials":[{"accessKey":"AKADMIN0","secretKey":"YWRtaW4="}],"actions":["Admin"]}],"serviceAccounts":[{"id":"sa-1","parentUser":"static-admin","credential":{"accessKey":"AKSA0001","secretKey":"b2xkc2E="}}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(p1); err != nil {
		t.Fatalf("failed to load initial config: %v", err)
	}
	if _, cred, found := s3a.iam.lookupByAccessKey("AKSA0001"); !found || cred.SecretKey != "b2xkc2E=" {
		t.Fatalf("expected service account secret to load, got found=%v cred=%+v", found, cred)
	}

	// Rotate the service account secret in the file and reload.
	p2 := writeTempIamConfig(t, `{"identities":[{"name":"static-admin","credentials":[{"accessKey":"AKADMIN0","secretKey":"YWRtaW4="}],"actions":["Admin"]}],"serviceAccounts":[{"id":"sa-1","parentUser":"static-admin","credential":{"accessKey":"AKSA0001","secretKey":"bmV3c2E="}}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(p2); err != nil {
		t.Fatalf("failed to reload config: %v", err)
	}
	_, cred, found := s3a.iam.lookupByAccessKey("AKSA0001")
	if !found {
		t.Fatalf("service account access key disappeared after reload")
	}
	if cred.SecretKey != "bmV3c2E=" {
		t.Fatalf("expected reloaded service account secret bmV3c2E=, got %q", cred.SecretKey)
	}
}

// A full snapshot reconciles policy and group deletions, static-file policies
// survive, and groups in a static config file are ignored: the dynamic store
// is the only source of groups.
func TestFullStateMergeReconcilesPoliciesAndGroups(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	path := writeTempIamConfig(t, `{"identities":[{"name":"static-admin","credentials":[{"accessKey":"AKIAITEST","secretKey":"c2VjcmV0"}],"actions":["Admin"]}],"policies":[{"name":"file-policy","content":"{}"}],"groups":[{"name":"file-group","members":["static-admin"]}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(path); err != nil {
		t.Fatalf("failed to load identity config: %v", err)
	}
	if hasGroup(s3a.iam, "file-group") {
		t.Fatalf("groups in a static config file must be ignored")
	}

	full := &iam_pb.S3ApiConfiguration{
		Policies: []*iam_pb.Policy{{Name: "dynamic-policy", Content: "{}"}},
		Groups:   []*iam_pb.Group{{Name: "g1"}},
	}
	if err := s3a.iam.MergeS3ApiConfiguration(full, false, true); err != nil {
		t.Fatalf("full merge failed: %v", err)
	}
	if !hasPolicy(s3a.iam, "file-policy") || !hasPolicy(s3a.iam, "dynamic-policy") || !hasGroup(s3a.iam, "g1") {
		t.Fatalf("expected file-policy, dynamic-policy and g1 after full merge")
	}

	// partial merge preserves groups and policies
	if err := s3a.iam.UpsertIdentity(&iam_pb.Identity{Name: "bob"}); err != nil {
		t.Fatalf("upsert failed: %v", err)
	}
	if !hasPolicy(s3a.iam, "dynamic-policy") || !hasGroup(s3a.iam, "g1") {
		t.Fatalf("partial merge must not drop dynamic-policy or g1")
	}

	// a static-file reload leaves dynamic groups alone
	if err := s3a.iam.loadS3ApiConfigurationFromFile(path); err != nil {
		t.Fatalf("failed to reload config: %v", err)
	}
	if !hasGroup(s3a.iam, "g1") {
		t.Fatalf("dynamic g1 must survive a static-file reload")
	}

	// empty full snapshot: dynamic policy and last group deleted, file policy stays
	if err := s3a.iam.MergeS3ApiConfiguration(&iam_pb.S3ApiConfiguration{}, false, true); err != nil {
		t.Fatalf("empty full merge failed: %v", err)
	}
	if hasPolicy(s3a.iam, "dynamic-policy") {
		t.Fatalf("expected dynamic-policy to be removed by empty full snapshot")
	}
	if hasGroup(s3a.iam, "g1") {
		t.Fatalf("expected g1 to be removed by empty full snapshot")
	}
	if !hasPolicy(s3a.iam, "file-policy") {
		t.Fatalf("file-policy must survive full-state reconciliation")
	}
}

// flakyStore fails LoadConfiguration a fixed number of times.
type flakyStore struct {
	credential.CredentialStore
	failures int
}

func (f *flakyStore) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	if f.failures > 0 {
		f.failures--
		return nil, fmt.Errorf("transient store failure")
	}
	return f.CredentialStore.LoadConfiguration(ctx)
}

// A failed reload queued from an IAM config change must keep retrying until
// the store recovers.
func TestFailedReloadRetriesUntilSuccess(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	prev := iamReloadRetryInterval
	iamReloadRetryInterval = 10 * time.Millisecond
	t.Cleanup(func() { iamReloadRetryInterval = prev })

	if err := s3a.iam.credentialManager.CreateUser(context.Background(), &iam_pb.Identity{Name: "alice"}); err != nil {
		t.Fatalf("failed to create alice: %v", err)
	}
	s3a.iam.credentialManager.Store = &flakyStore{CredentialStore: s3a.iam.credentialManager.Store, failures: 2}

	// onIamConfigChange only queues a coalesced reload; the retry loop does the
	// work and retries through the transient store failures.
	if err := s3a.onIamConfigChange(filer.IamConfigDirectory+"/identities", nil, &filer_pb.Entry{Name: "alice.json"}); err != nil {
		t.Fatalf("onIamConfigChange returned error: %v", err)
	}
	waitForIdentity(t, s3a.iam, "alice")
}

func hasPolicy(iam *IdentityAccessManagement, name string) bool {
	iam.m.RLock()
	defer iam.m.RUnlock()
	_, ok := iam.policies[name]
	return ok
}

func hasGroup(iam *IdentityAccessManagement, name string) bool {
	iam.m.RLock()
	defer iam.m.RUnlock()
	_, ok := iam.groups[name]
	return ok
}

func isStaticName(iam *IdentityAccessManagement, name string) bool {
	iam.m.RLock()
	defer iam.m.RUnlock()
	return iam.staticIdentityNames[name]
}

func writeTempIamConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "iam.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}
	return path
}

// A static config file may hold ${VAR} in place of a key, so a deployment can
// keep the keys in its own secret store and pass them in as environment
// variables rather than baking them into the file.
func TestStaticConfigExpandsEnvCredentialRefs(t *testing.T) {
	t.Setenv("SEAWEEDFS_S3_ADMIN_ACCESS_KEY_ID", "AKIAFROMENV")
	t.Setenv("SEAWEEDFS_S3_ADMIN_SECRET_ACCESS_KEY", "secretfromenv")

	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	path := writeTempIamConfig(t, `{"identities":[{"name":"anvAdmin","credentials":[{"accessKey":"${SEAWEEDFS_S3_ADMIN_ACCESS_KEY_ID}","secretKey":"${SEAWEEDFS_S3_ADMIN_SECRET_ACCESS_KEY}"}],"actions":["Admin"]}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(path); err != nil {
		t.Fatalf("failed to load identity config: %v", err)
	}

	_, cred, found := s3a.iam.lookupByAccessKey("AKIAFROMENV")
	if !found {
		t.Fatalf("expected the access key from the environment to be loaded")
	}
	if cred.SecretKey != "secretfromenv" {
		t.Fatalf("expected the secret key from the environment, got %q", cred.SecretKey)
	}
	if _, _, found := s3a.iam.lookupByAccessKey("${SEAWEEDFS_S3_ADMIN_ACCESS_KEY_ID}"); found {
		t.Fatalf("the unexpanded reference must not remain usable as an access key")
	}
}

// An unset variable must not leave the reference behind as a literal key.
func TestStaticConfigDropsUnresolvedEnvCredentialRefs(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	path := writeTempIamConfig(t, `{"identities":[{"name":"anvAdmin","credentials":[{"accessKey":"${SEAWEEDFS_S3_MISSING_ACCESS_KEY_ID}","secretKey":"${SEAWEEDFS_S3_MISSING_SECRET_ACCESS_KEY}"}],"actions":["Admin"]}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(path); err != nil {
		t.Fatalf("failed to load identity config: %v", err)
	}

	if _, _, found := s3a.iam.lookupByAccessKey("${SEAWEEDFS_S3_MISSING_ACCESS_KEY_ID}"); found {
		t.Fatalf("a credential referencing an unset variable must be dropped")
	}
	if !hasIdentity(s3a.iam, "anvAdmin") {
		t.Fatalf("expected the identity itself to still load")
	}
}

// Keys that merely contain a dollar sign are literal, and identities coming
// from the filer are never expanded.
func TestEnvCredentialRefsOnlyApplyToStaticConfig(t *testing.T) {
	t.Setenv("SEAWEEDFS_S3_DYNAMIC_SECRET_ACCESS_KEY", "secretfromenv")

	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	if err := s3a.iam.LoadS3ApiConfigurationFromBytes([]byte(`{"identities":[{"name":"dynamic","credentials":[{"accessKey":"AKIALITERAL","secretKey":"${SEAWEEDFS_S3_DYNAMIC_SECRET_ACCESS_KEY}"}],"actions":["Admin"]}]}`)); err != nil {
		t.Fatalf("failed to load dynamic config: %v", err)
	}

	_, cred, found := s3a.iam.lookupByAccessKey("AKIALITERAL")
	if !found {
		t.Fatalf("expected the dynamic identity to load")
	}
	if cred.SecretKey != "${SEAWEEDFS_S3_DYNAMIC_SECRET_ACCESS_KEY}" {
		t.Fatalf("a dynamic identity must keep its secret key verbatim, got %q", cred.SecretKey)
	}
}

// A key holding a dollar sign that is not a reference stays untouched.
func TestStaticConfigKeepsLiteralDollarSigns(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	path := writeTempIamConfig(t, `{"identities":[{"name":"anvAdmin","credentials":[{"accessKey":"AKIALITERAL","secretKey":"pa$$word"}],"actions":["Admin"]}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(path); err != nil {
		t.Fatalf("failed to load identity config: %v", err)
	}

	_, cred, found := s3a.iam.lookupByAccessKey("AKIALITERAL")
	if !found {
		t.Fatalf("expected the identity to load")
	}
	if cred.SecretKey != "pa$$word" {
		t.Fatalf("expected the literal secret key, got %q", cred.SecretKey)
	}
}

// A secret store handing over a blank value must not leave an access key that
// any signature matches.
func TestStaticConfigDropsEmptyEnvCredentialRefs(t *testing.T) {
	t.Setenv("SEAWEEDFS_S3_ADMIN_ACCESS_KEY_ID", "AKIAFROMENV")
	t.Setenv("SEAWEEDFS_S3_ADMIN_SECRET_ACCESS_KEY", "")

	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	path := writeTempIamConfig(t, `{"identities":[{"name":"anvAdmin","credentials":[{"accessKey":"${SEAWEEDFS_S3_ADMIN_ACCESS_KEY_ID}","secretKey":"${SEAWEEDFS_S3_ADMIN_SECRET_ACCESS_KEY}"}],"actions":["Admin"]}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(path); err != nil {
		t.Fatalf("failed to load identity config: %v", err)
	}

	if _, _, found := s3a.iam.lookupByAccessKey("AKIAFROMENV"); found {
		t.Fatalf("a credential whose secret key resolves to empty must be dropped")
	}
}

// A reference the substitution cannot match, such as a typo in the variable
// name, must not survive as a literal key.
func TestStaticConfigDropsMalformedEnvCredentialRefs(t *testing.T) {
	for _, malformed := range []string{"${MY-VAR}", "${1VAR}", "${}", "${UNTERMINATED", "${A}${B"} {
		t.Run(malformed, func(t *testing.T) {
			t.Setenv("SEAWEEDFS_S3_ADMIN_SECRET_ACCESS_KEY", "secretfromenv")
			t.Setenv("A", "a")

			s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

			path := writeTempIamConfig(t, fmt.Sprintf(`{"identities":[{"name":"anvAdmin","credentials":[{"accessKey":%q,"secretKey":"${SEAWEEDFS_S3_ADMIN_SECRET_ACCESS_KEY}"}],"actions":["Admin"]}]}`, malformed))
			if err := s3a.iam.loadS3ApiConfigurationFromFile(path); err != nil {
				t.Fatalf("failed to load identity config: %v", err)
			}

			if _, _, found := s3a.iam.lookupByAccessKey(malformed); found {
				t.Fatalf("%s must not become a usable access key", malformed)
			}
		})
	}
}

// A resolved value that happens to contain ${ is still the key the operator set.
func TestStaticConfigKeepsBracesComingFromTheEnvironment(t *testing.T) {
	t.Setenv("SEAWEEDFS_S3_ADMIN_ACCESS_KEY_ID", "AKIAFROMENV")
	t.Setenv("SEAWEEDFS_S3_ADMIN_SECRET_ACCESS_KEY", "pa${ss}word")

	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	path := writeTempIamConfig(t, `{"identities":[{"name":"anvAdmin","credentials":[{"accessKey":"${SEAWEEDFS_S3_ADMIN_ACCESS_KEY_ID}","secretKey":"${SEAWEEDFS_S3_ADMIN_SECRET_ACCESS_KEY}"}],"actions":["Admin"]}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(path); err != nil {
		t.Fatalf("failed to load identity config: %v", err)
	}

	_, cred, found := s3a.iam.lookupByAccessKey("AKIAFROMENV")
	if !found {
		t.Fatalf("expected the identity to load")
	}
	if cred.SecretKey != "pa${ss}word" {
		t.Fatalf("expected the secret key from the environment verbatim, got %q", cred.SecretKey)
	}
}

// Editing an identity out of the static config file and reloading must revoke it:
// the identity, its access keys and those of its service accounts stop working on
// the running gateway, and its name stops being protected as static. What the file
// still declares, a dynamic filer-managed identity and the AWS environment
// identity are all untouched.
func TestReloadStaticConfigRevokesRemovedIdentity(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAENV1")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "c2VjcmV0")

	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	p1 := writeTempIamConfig(t, `{"identities":[{"name":"static-admin","credentials":[{"accessKey":"AKADMIN0","secretKey":"c2VjcmV0"}],"actions":["Admin"]},{"name":"revoked","credentials":[{"accessKey":"AKREVOKE","secretKey":"c2VjcmV0"}],"actions":["Read","List"]},{"name":"kept","credentials":[{"accessKey":"AKKEPT00","secretKey":"c2VjcmV0"}],"actions":["Read"]}],"serviceAccounts":[{"id":"sa-1","parentUser":"revoked","credential":{"accessKey":"AKSA0001","secretKey":"c2VjcmV0"}}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(p1); err != nil {
		t.Fatalf("failed to load initial config: %v", err)
	}
	if _, _, found := s3a.iam.lookupByAccessKey("AKREVOKE"); !found {
		t.Fatalf("expected the identity to authenticate before the reload")
	}
	if _, _, found := s3a.iam.lookupByAccessKey("AKSA0001"); !found {
		t.Fatalf("expected the service account key to be loaded before the reload")
	}

	// A dynamic identity arrives from the filer and must survive the reload.
	if err := s3a.iam.credentialManager.CreateUser(context.Background(), &iam_pb.Identity{Name: "dynamic", Credentials: []*iam_pb.Credential{{AccessKey: "AKDYN000", SecretKey: "c2VjcmV0"}}}); err != nil {
		t.Fatalf("failed to create dynamic identity: %v", err)
	}
	if err := s3a.iam.LoadS3ApiConfigurationFromCredentialManager(); err != nil {
		t.Fatalf("failed to load from credential manager: %v", err)
	}
	if _, _, found := s3a.iam.lookupByAccessKey("AKDYN000"); !found {
		t.Fatalf("expected the dynamic identity's key to authenticate before the reload")
	}

	// Drop "revoked" and its service account from the file and reload.
	p2 := writeTempIamConfig(t, `{"identities":[{"name":"static-admin","credentials":[{"accessKey":"AKADMIN0","secretKey":"c2VjcmV0"}],"actions":["Admin"]},{"name":"kept","credentials":[{"accessKey":"AKKEPT00","secretKey":"c2VjcmV0"}],"actions":["Read"]}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(p2); err != nil {
		t.Fatalf("failed to reload config: %v", err)
	}

	if hasIdentity(s3a.iam, "revoked") {
		t.Fatalf("an identity removed from the config file must be dropped by the reload")
	}
	for _, key := range []string{"AKREVOKE", "AKSA0001"} {
		if _, _, found := s3a.iam.lookupByAccessKey(key); found {
			t.Fatalf("access key %s of the removed identity must stop authenticating", key)
		}
	}
	if isStaticName(s3a.iam, "revoked") {
		t.Fatalf("a name that left the config file must stop being static")
	}

	// What the file still declares is untouched.
	if !hasIdentity(s3a.iam, "kept") || !isStaticName(s3a.iam, "kept") {
		t.Fatalf("identities still in the file must survive the reload")
	}
	if _, _, found := s3a.iam.lookupByAccessKey("AKKEPT00"); !found {
		t.Fatalf("a kept identity's access key must keep working")
	}

	// The file cannot revoke what it never declared.
	if !hasIdentity(s3a.iam, "dynamic") {
		t.Fatalf("a dynamic identity must survive a static-file reload")
	}
	if _, _, found := s3a.iam.lookupByAccessKey("AKDYN000"); !found {
		t.Fatalf("a dynamic identity's access key must keep working after a static-file reload")
	}
	if !hasIdentity(s3a.iam, "admin-AKIAENV1") {
		t.Fatalf("the AWS environment identity must survive a static-file reload")
	}
	if _, _, found := s3a.iam.lookupByAccessKey("AKIAENV1"); !found {
		t.Fatalf("the AWS environment identity's access key must keep working")
	}
}

// A file reload always merges, so emptying the file must not flip the next reload
// into replacing the store and dropping the filer-managed identities.
func TestReloadStaticConfigWithoutIdentitiesKeepsDynamic(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	p1 := writeTempIamConfig(t, `{"identities":[{"name":"static-admin","credentials":[{"accessKey":"AKADMIN0","secretKey":"c2VjcmV0"}],"actions":["Admin"]}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(p1); err != nil {
		t.Fatalf("failed to load initial config: %v", err)
	}

	if err := s3a.iam.credentialManager.CreateUser(context.Background(), &iam_pb.Identity{Name: "alice", Credentials: []*iam_pb.Credential{{AccessKey: "AKALICE0", SecretKey: "c2VjcmV0"}}}); err != nil {
		t.Fatalf("failed to create alice: %v", err)
	}
	if err := s3a.iam.LoadS3ApiConfigurationFromCredentialManager(); err != nil {
		t.Fatalf("failed to load from credential manager: %v", err)
	}

	empty := writeTempIamConfig(t, `{"identities":[]}`)
	for i := 1; i <= 2; i++ {
		if err := s3a.iam.loadS3ApiConfigurationFromFile(empty); err != nil {
			t.Fatalf("reload %d failed: %v", i, err)
		}
		if hasIdentity(s3a.iam, "static-admin") {
			t.Fatalf("reload %d: an identity removed from the file must stay removed", i)
		}
		if !hasIdentity(s3a.iam, "alice") {
			t.Fatalf("reload %d: a filer-managed identity must survive a reload of an emptied file", i)
		}
		if _, _, found := s3a.iam.lookupByAccessKey("AKALICE0"); !found {
			t.Fatalf("reload %d: the filer-managed identity's access key must keep working", i)
		}
	}
}
