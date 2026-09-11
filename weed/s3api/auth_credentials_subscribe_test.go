package s3api

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/memory"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func TestOnIamConfigChangeLegacyIdentityDeletionReloadsConfiguration(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{
		{
			Name: "anonymous",
			Actions: []string{
				"Read:test",
			},
		},
	})

	err := s3a.onIamConfigChange(
		filer.IamConfigDirectory,
		&filer_pb.Entry{Name: filer.IamIdentityFile},
		nil,
	)
	if err != nil {
		t.Fatalf("onIamConfigChange returned error for legacy identity deletion: %v", err)
	}

	if !hasIdentity(s3a.iam, "anonymous") {
		t.Fatalf("expected anonymous identity to remain loaded after legacy identity deletion event")
	}
}

func TestOnIamConfigChangeReloadsOnIamIdentityDirectoryChanges(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{
		{Name: "anonymous"},
	})

	// Seed initial in-memory IAM state.
	if err := s3a.iam.LoadS3ApiConfigurationFromCredentialManager(); err != nil {
		t.Fatalf("failed to load initial IAM configuration: %v", err)
	}
	if hasIdentity(s3a.iam, "alice") {
		t.Fatalf("did not expect alice identity before creating user")
	}

	if err := s3a.iam.credentialManager.CreateUser(context.Background(), &iam_pb.Identity{Name: "alice"}); err != nil {
		t.Fatalf("failed to create alice in memory credential manager: %v", err)
	}

	if err := s3a.onIamConfigChange(
		filer.IamConfigDirectory+"/identities",
		nil,
		&filer_pb.Entry{Name: "alice.json"},
	); err != nil {
		t.Fatalf("onIamConfigChange returned error for identities directory update: %v", err)
	}

	waitForIdentity(t, s3a.iam, "alice")
}

func newTestS3ApiServerWithMemoryIAM(t *testing.T, identities []*iam_pb.Identity) *S3ApiServer {
	t.Helper()

	// Create S3ApiConfiguration for test with provided identities
	config := &iam_pb.S3ApiConfiguration{
		Identities:      identities,
		Accounts:        []*iam_pb.Account{},
		ServiceAccounts: []*iam_pb.ServiceAccount{},
	}

	// Create memory credential manager
	cm, err := credential.NewCredentialManager(credential.StoreTypeMemory, nil, "")
	if err != nil {
		t.Fatalf("failed to create memory credential manager: %v", err)
	}

	// Save test configuration
	if err := cm.SaveConfiguration(context.Background(), config); err != nil {
		t.Fatalf("failed to save test configuration: %v", err)
	}

	// Create a test IAM instance
	iam := &IdentityAccessManagement{
		m:                 sync.RWMutex{},
		nameToIdentity:    make(map[string]*Identity),
		accessKeyIdent:    make(map[string]*Identity),
		identities:        []*Identity{},
		policies:          make(map[string]*iam_pb.Policy),
		accounts:          make(map[string]*Account),
		emailAccount:      make(map[string]*Account),
		hashes:            make(map[string]*sync.Pool),
		hashCounters:      make(map[string]*int32),
		isAuthEnabled:     false,
		stopChan:          make(chan struct{}),
		reloadCh:          make(chan struct{}, 1),
		useStaticConfig:   false,
		credentialManager: cm,
	}
	go iam.reloadRetryLoop()
	t.Cleanup(iam.Shutdown)

	// Load test configuration
	if err := iam.ReplaceS3ApiConfiguration(config); err != nil {
		t.Fatalf("failed to load test configuration: %v", err)
	}

	return &S3ApiServer{
		iam: iam,
	}
}

func hasIdentity(iam *IdentityAccessManagement, identityName string) bool {
	iam.m.RLock()
	defer iam.m.RUnlock()

	_, ok := iam.nameToIdentity[identityName]
	return ok
}

func waitForIdentity(t *testing.T, iam *IdentityAccessManagement, name string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !hasIdentity(iam, name) {
		if time.Now().After(deadline) {
			t.Fatalf("expected identity %s to be loaded", name)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForIdentityGone(t *testing.T, iam *IdentityAccessManagement, name string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for hasIdentity(iam, name) {
		if time.Now().After(deadline) {
			t.Fatalf("expected identity %s to be gone", name)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// countingStore wraps a store and counts LoadConfiguration calls.
type countingStore struct {
	credential.CredentialStore
	loads int64
}

func (c *countingStore) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	atomic.AddInt64(&c.loads, 1)
	return c.CredentialStore.LoadConfiguration(ctx)
}

// A burst of IAM config change events must coalesce into a handful of reloads,
// not one full reload per event.
func TestOnIamConfigChangeCoalescesBurstReloads(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{{Name: "anonymous"}})

	counter := &countingStore{CredentialStore: s3a.iam.credentialManager.Store}
	s3a.iam.credentialManager.Store = counter

	const burst = 50
	for i := 0; i < burst; i++ {
		if err := s3a.onIamConfigChange(
			filer.IamConfigDirectory+"/identities",
			nil,
			&filer_pb.Entry{Name: fmt.Sprintf("u%d.json", i)},
		); err != nil {
			t.Fatalf("onIamConfigChange returned error: %v", err)
		}
	}

	// Wait for the queue to drain: no pending signal.
	deadline := time.Now().Add(5 * time.Second)
	for {
		s3a.iam.reloadMu.Lock()
		empty := len(s3a.iam.reloadCh) == 0
		s3a.iam.reloadMu.Unlock()
		if empty {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("reload queue did not drain")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Let any final coalesced reload finish.
	time.Sleep(50 * time.Millisecond)

	loads := atomic.LoadInt64(&counter.loads)
	if loads > 3 {
		t.Fatalf("expected a burst of %d events to coalesce into <=3 reloads, got %d", burst, loads)
	}
}
