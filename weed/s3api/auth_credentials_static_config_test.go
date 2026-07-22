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
	if !hasIdentity(s3a.iam, "alice") {
		t.Fatalf("expected alice to load after filer change with -iam.config-only setup")
	}
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
	if !hasIdentity(s3a.iam, "alice") {
		t.Fatalf("expected alice to live-reload despite the static identity file")
	}
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
	if hasIdentity(s3a.iam, "alice") {
		t.Fatalf("expected alice to be removed after deletion on the filer")
	}
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

// A full snapshot must also reconcile policy and group deletions, without
// touching policies from the static config file or groups on partial merges.
func TestFullStateMergeReconcilesPoliciesAndGroups(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	path := writeTempIamConfig(t, `{"identities":[{"name":"static-admin","credentials":[{"accessKey":"AKIAITEST","secretKey":"c2VjcmV0"}],"actions":["Admin"]}],"policies":[{"name":"file-policy","content":"{}"}],"groups":[{"name":"file-group","members":["static-admin"]}]}`)
	if err := s3a.iam.loadS3ApiConfigurationFromFile(path); err != nil {
		t.Fatalf("failed to load identity config: %v", err)
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
	if !hasGroup(s3a.iam, "file-group") {
		t.Fatalf("file-group must survive a full snapshot that does not carry it")
	}

	// partial merge preserves groups and policies
	if err := s3a.iam.UpsertIdentity(&iam_pb.Identity{Name: "bob"}); err != nil {
		t.Fatalf("upsert failed: %v", err)
	}
	if !hasPolicy(s3a.iam, "dynamic-policy") || !hasGroup(s3a.iam, "g1") {
		t.Fatalf("partial merge must not drop dynamic-policy or g1")
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
	if !hasPolicy(s3a.iam, "file-policy") || !hasGroup(s3a.iam, "file-group") {
		t.Fatalf("file-policy and file-group must survive full-state reconciliation")
	}
	s3a.iam.m.RLock()
	memberGroups := s3a.iam.userGroups["static-admin"]
	s3a.iam.m.RUnlock()
	if len(memberGroups) != 1 || memberGroups[0] != "file-group" {
		t.Fatalf("expected static-admin membership in file-group to survive, got %v", memberGroups)
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

// A failed event-driven reload must keep retrying until the store recovers.
func TestFailedReloadRetriesUntilSuccess(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{})

	prev := iamReloadRetryInterval
	iamReloadRetryInterval = 10 * time.Millisecond
	t.Cleanup(func() { iamReloadRetryInterval = prev })

	s3a.iam.reloadCh = make(chan struct{}, 1)
	go s3a.iam.reloadRetryLoop()
	t.Cleanup(s3a.iam.Shutdown)

	if err := s3a.iam.credentialManager.CreateUser(context.Background(), &iam_pb.Identity{Name: "alice"}); err != nil {
		t.Fatalf("failed to create alice: %v", err)
	}
	s3a.iam.credentialManager.Store = &flakyStore{CredentialStore: s3a.iam.credentialManager.Store, failures: 2}

	// the event-driven reload fails and hands off to the retry loop
	if err := s3a.onIamConfigChange(filer.IamConfigDirectory+"/identities", nil, &filer_pb.Entry{Name: "alice.json"}); err == nil {
		t.Fatalf("expected the event-driven reload to fail")
	}
	deadline := time.Now().Add(5 * time.Second)
	for !hasIdentity(s3a.iam, "alice") {
		if time.Now().After(deadline) {
			t.Fatalf("expected alice to load once the store recovered")
		}
		time.Sleep(10 * time.Millisecond)
	}
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
