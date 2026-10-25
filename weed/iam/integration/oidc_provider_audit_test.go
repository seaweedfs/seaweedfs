package integration

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
)

func newAuditableManager(t *testing.T) (*IAMManager, *MemoryAuditSink) {
	t.Helper()
	mgr := NewIAMManager()
	sink := NewMemoryAuditSink()
	cfg := &IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: 12 * time.Hour},
			Issuer:           "test-sts",
			SigningKey:       []byte("test-signing-key-32-characters-long"),
			AccountId:        "111122223333",
		},
		Policy: &policy.PolicyEngineConfig{DefaultEffect: "Deny", StoreType: "memory"},
		Roles:  &RoleStoreConfig{StoreType: "memory"},
	}
	if err := mgr.Initialize(cfg, func() string { return "localhost:8888" }); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	mgr.SetOIDCProviderAuditSink(sink)
	return mgr, sink
}

func TestOIDCAuditLifecycleEventsAreEmitted(t *testing.T) {
	mgr, sink := newAuditableManager(t)
	ctx := context.Background()

	rec := &OIDCProviderRecord{
		AccountID: "111122223333",
		ARN:       "arn:aws:iam::111122223333:oidc-provider/idp.example",
		URL:       "https://idp.example",
		ClientIDs: []string{"x"},
	}
	if err := mgr.CreateOIDCProvider(ctx, rec); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := mgr.AddClientIDToOIDCProvider(ctx, rec.ARN, "y"); err != nil {
		t.Fatalf("AddClientID: %v", err)
	}
	if err := mgr.RemoveClientIDFromOIDCProvider(ctx, rec.ARN, "y"); err != nil {
		t.Fatalf("RemoveClientID: %v", err)
	}
	if err := mgr.UpdateOIDCProviderThumbprints(ctx, rec.ARN, []string{"0000000000000000000000000000000000000000"}); err != nil {
		t.Fatalf("UpdateThumbprints: %v", err)
	}
	if err := mgr.TagOIDCProvider(ctx, rec.ARN, map[string]string{"team": "infra"}); err != nil {
		t.Fatalf("Tag: %v", err)
	}
	if err := mgr.UntagOIDCProvider(ctx, rec.ARN, []string{"team"}); err != nil {
		t.Fatalf("Untag: %v", err)
	}
	if err := mgr.DeleteOIDCProvider(ctx, rec.ARN); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	want := []OIDCProviderAuditEventType{
		OIDCAuditEventCreated,
		OIDCAuditEventClientIDAdded,
		OIDCAuditEventClientIDRemoved,
		OIDCAuditEventThumbprintsSet,
		OIDCAuditEventTagsAdded,
		OIDCAuditEventTagsRemoved,
		OIDCAuditEventDeleted,
	}
	events := sink.Events()
	if len(events) != len(want) {
		t.Fatalf("expected %d events, got %d (%+v)", len(want), len(events), events)
	}
	for i, e := range events {
		if e.Type != want[i] {
			t.Errorf("event %d: type=%s want=%s", i, e.Type, want[i])
		}
		if e.ARN != rec.ARN {
			t.Errorf("event %d: ARN=%s want=%s", i, e.ARN, rec.ARN)
		}
		if e.OccurredAt.IsZero() {
			t.Errorf("event %d has zero OccurredAt", i)
		}
	}
}

func TestOIDCAuditDefaultsToGlogSink(t *testing.T) {
	// No SetOIDCProviderAuditSink call — should default to glog and not panic.
	mgr, _ := newAuditableManager(t)
	mgr.SetOIDCProviderAuditSink(nil)
	rec := &OIDCProviderRecord{
		AccountID: "111122223333",
		ARN:       "arn:aws:iam::111122223333:oidc-provider/idp.example",
		URL:       "https://idp.example",
		ClientIDs: []string{"x"},
	}
	if err := mgr.CreateOIDCProvider(context.Background(), rec); err != nil {
		t.Fatalf("Create with default sink: %v", err)
	}
}
