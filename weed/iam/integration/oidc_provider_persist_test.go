package integration

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/stretchr/testify/assert"
)

// persistentTestStore stands in for a store that outlives the process (the
// filer store): anything but *MemoryOIDCProviderStore is treated as persistent.
type persistentTestStore struct{ *MemoryOIDCProviderStore }

const (
	persistTestCurrentIssuer = "https://current.example"
	persistTestStaleIssuer   = "https://stale.example"
	persistTestAPIIssuer     = "https://api.example"
)

func newPersistTestManager(t *testing.T) *IAMManager {
	t.Helper()
	mgr := NewIAMManager()
	cfg := &IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: 12 * time.Hour},
			Issuer:           "test-sts",
			SigningKey:       []byte("test-signing-key-32-characters-long"),
			Providers: []*sts.ProviderConfig{{
				Name:    "current",
				Type:    sts.ProviderTypeOIDC,
				Enabled: true,
				Config:  map[string]interface{}{"issuer": persistTestCurrentIssuer, "clientId": "aud"},
			}},
		},
		Policy: &policy.PolicyEngineConfig{DefaultEffect: "Deny", StoreType: "memory"},
		Roles:  &RoleStoreConfig{StoreType: "memory"},
	}
	if err := mgr.Initialize(cfg, func() string { return "localhost:8888" }); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	return mgr
}

// seedEarlierBoot fills a store as an earlier boot would have left it: the
// provider the config still lists, one it has since dropped, and one created
// through the IAM API.
func seedEarlierBoot(t *testing.T, store OIDCProviderStore) (current, stale, api string) {
	t.Helper()
	ctx := context.Background()
	for _, r := range []struct{ issuer, source string }{
		{persistTestCurrentIssuer, OIDCProviderSourceStaticConfig},
		{persistTestStaleIssuer, OIDCProviderSourceStaticConfig},
		{persistTestAPIIssuer, ""},
	} {
		arn, err := DeriveOIDCProviderARN("", r.issuer)
		if err != nil {
			t.Fatalf("derive ARN: %v", err)
		}
		rec := &OIDCProviderRecord{ARN: arn, URL: r.issuer, ClientIDs: []string{"aud"}, Source: r.source}
		if err := store.StoreProvider(ctx, "", rec); err != nil {
			t.Fatalf("seed %s: %v", r.issuer, err)
		}
	}
	current, _ = DeriveOIDCProviderARN("", persistTestCurrentIssuer)
	stale, _ = DeriveOIDCProviderARN("", persistTestStaleIssuer)
	api, _ = DeriveOIDCProviderARN("", persistTestAPIIssuer)
	return current, stale, api
}

func storedARNs(t *testing.T, mgr *IAMManager) map[string]bool {
	t.Helper()
	recs, err := mgr.ListOIDCProviders(context.Background())
	if err != nil {
		t.Fatalf("ListOIDCProviders: %v", err)
	}
	out := map[string]bool{}
	for _, r := range recs {
		out[r.ARN] = true
	}
	return out
}

// stsKnowsIssuer reports whether STS resolves a provider for the issuer. The
// token is not validly signed; the only question is which error comes back.
func stsKnowsIssuer(t *testing.T, mgr *IAMManager, issuer string) bool {
	t.Helper()
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": issuer, "sub": "probe", "aud": "aud", "exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte("not-the-providers-key"))
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	_, _, err = mgr.GetSTSService().ValidateWebIdentityToken(context.Background(), tok)
	if err == nil {
		t.Fatalf("an unsigned-by-provider token for %s was accepted", issuer)
	}
	return !strings.Contains(err.Error(), "no identity provider registered")
}

func TestPersistentStorePrunesDroppedStaticProvidersAndHydratesSTS(t *testing.T) {
	mgr := newPersistTestManager(t)
	store := &persistentTestStore{NewMemoryOIDCProviderStore()}
	current, stale, api := seedEarlierBoot(t, store)
	mgr.SetOIDCProviderStore(store)

	if stsKnowsIssuer(t, mgr, persistTestAPIIssuer) {
		t.Fatal("precondition: STS already knows the API-created issuer before hydration")
	}

	mgr.pruneAndHydrateOIDCProviders(context.Background(), map[string]bool{current: true})

	got := storedARNs(t, mgr)
	if got[stale] {
		t.Errorf("provider dropped from static config is still stored: %s", stale)
	}
	if !got[current] {
		t.Errorf("provider still in static config was pruned: %s", current)
	}
	if !got[api] {
		t.Errorf("provider created through the IAM API was pruned: %s", api)
	}
	if !stsKnowsIssuer(t, mgr, persistTestAPIIssuer) {
		t.Error("STS does not trust the API-created provider found in the store at startup")
	}
	if stsKnowsIssuer(t, mgr, persistTestStaleIssuer) {
		t.Error("STS still trusts the provider dropped from static config")
	}
}

func TestInMemoryStoreIsNeitherPrunedNorHydrated(t *testing.T) {
	mgr := newPersistTestManager(t)
	store := NewMemoryOIDCProviderStore()
	current, stale, _ := seedEarlierBoot(t, store)
	mgr.SetOIDCProviderStore(store)

	mgr.pruneAndHydrateOIDCProviders(context.Background(), map[string]bool{current: true})

	if !storedARNs(t, mgr)[stale] {
		t.Error("an in-memory store was pruned; it holds nothing from an earlier boot")
	}
	if stsKnowsIssuer(t, mgr, persistTestAPIIssuer) {
		t.Error("an in-memory store was hydrated into STS at startup")
	}
}

func TestStaticMirrorMarksItsRecords(t *testing.T) {
	mgr := newPersistTestManager(t)
	recs, err := mgr.ListOIDCProviders(context.Background())
	if err != nil {
		t.Fatalf("ListOIDCProviders: %v", err)
	}
	if len(recs) != 1 || recs[0].Source != OIDCProviderSourceStaticConfig {
		t.Fatalf("static mirror did not mark its record: %+v", recs)
	}
}

// unreachableThenReadyStore fails its first reads, as a filer that is not up
// yet when the S3 server starts.
type unreachableThenReadyStore struct {
	*MemoryOIDCProviderStore
	mu        sync.Mutex
	failsLeft int
}

func (s *unreachableThenReadyStore) ListProviders(ctx context.Context, addr string) ([]*OIDCProviderRecord, error) {
	s.mu.Lock()
	if s.failsLeft > 0 {
		s.failsLeft--
		s.mu.Unlock()
		return nil, errors.New("filer unavailable")
	}
	s.mu.Unlock()
	return s.MemoryOIDCProviderStore.ListProviders(ctx, addr)
}

// A store that cannot be read at startup is retried: the metadata
// subscription only reports later changes, so without a retry the providers
// already stored would stay unknown to STS.
func TestStartupLoadRetriesUntilTheStoreIsReadable(t *testing.T) {
	saved := oidcHydrateRetry
	oidcHydrateRetry.initial, oidcHydrateRetry.max = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { oidcHydrateRetry = saved })

	mgr := newPersistTestManager(t)
	store := &unreachableThenReadyStore{MemoryOIDCProviderStore: NewMemoryOIDCProviderStore(), failsLeft: 3}
	current, stale, api := seedEarlierBoot(t, store)
	mgr.SetOIDCProviderStore(store)

	mgr.pruneAndHydrateOIDCProviders(context.Background(), map[string]bool{current: true})

	deadline := time.Now().Add(2 * time.Second)
	for !stsKnowsIssuer(t, mgr, persistTestAPIIssuer) {
		if time.Now().After(deadline) {
			t.Fatal("STS never learned the stored provider after the store became readable")
		}
		time.Sleep(5 * time.Millisecond)
	}
	got := storedARNs(t, mgr)
	assert.False(t, got[stale], "the provider dropped from static config was not pruned on retry")
	assert.True(t, got[api])
}
