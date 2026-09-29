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
	"github.com/stretchr/testify/require"
)

// persistentTestStore stands in for a store that outlives the process and may
// be shared (the filer store): anything but *MemoryOIDCProviderStore is
// treated as persistent.
type persistentTestStore struct{ *MemoryOIDCProviderStore }

const (
	persistTestStaticIssuer = "https://static.example"
	persistTestAPIIssuer    = "https://api.example"
)

// persistTestConfig is an IAM config whose file defines the given OIDC
// issuers.
func persistTestConfig(issuers ...string) *IAMConfig {
	var providers []*sts.ProviderConfig
	for i, issuer := range issuers {
		providers = append(providers, &sts.ProviderConfig{
			Name:    "static-" + string(rune('a'+i)),
			Type:    sts.ProviderTypeOIDC,
			Enabled: true,
			Config:  map[string]interface{}{"issuer": issuer, "clientId": "aud"},
		})
	}
	return &IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: 12 * time.Hour},
			Issuer:           "test-sts",
			SigningKey:       []byte("test-signing-key-32-characters-long"),
			Providers:        providers,
		},
		Policy: &policy.PolicyEngineConfig{DefaultEffect: "Deny", StoreType: "memory"},
		Roles:  &RoleStoreConfig{StoreType: "memory"},
	}
}

// startServer initializes a manager as an S3 server would at boot, with the
// given config file providers and OIDC provider store.
func startServer(t *testing.T, store OIDCProviderStore, issuers ...string) *IAMManager {
	t.Helper()
	cfg := persistTestConfig(issuers...)
	mgr := NewIAMManager()
	require.NoError(t, mgr.Initialize(cfg, func() string { return "localhost:8888" }))
	mgr.installOIDCProviderStore(store, cfg.STS)
	return mgr
}

func arnOf(t *testing.T, issuer string) string {
	t.Helper()
	arn, err := DeriveOIDCProviderARN("", issuer)
	require.NoError(t, err)
	return arn
}

func listedARNs(t *testing.T, mgr *IAMManager) map[string]bool {
	t.Helper()
	recs, err := mgr.ListOIDCProviders(context.Background())
	require.NoError(t, err)
	out := map[string]bool{}
	for _, r := range recs {
		out[r.ARN] = true
	}
	return out
}

func storedARNs(t *testing.T, store OIDCProviderStore) map[string]bool {
	t.Helper()
	recs, err := store.ListProviders(context.Background(), "")
	require.NoError(t, err)
	out := map[string]bool{}
	for _, r := range recs {
		out[r.ARN] = true
	}
	return out
}

func createAPIProvider(t *testing.T, mgr *IAMManager, issuer string) {
	t.Helper()
	require.NoError(t, mgr.CreateOIDCProvider(context.Background(), &OIDCProviderRecord{
		ARN: arnOf(t, issuer), URL: issuer, ClientIDs: []string{"aud"},
	}))
}

// stsKnowsIssuer reports whether STS resolves a provider for the issuer. The
// token is not validly signed; the only question is which error comes back.
func stsKnowsIssuer(t *testing.T, mgr *IAMManager, issuer string) bool {
	t.Helper()
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": issuer, "sub": "probe", "aud": "aud", "exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte("not-the-providers-key"))
	require.NoError(t, err)
	_, _, err = mgr.GetSTSService().ValidateWebIdentityToken(context.Background(), tok)
	require.Error(t, err, "an unsigned-by-provider token for %s was accepted", issuer)
	return !strings.Contains(err.Error(), "no identity provider registered")
}

// A persistent store never receives the config file's providers: it outlives
// the file and may be shared with servers whose files differ.
func TestPersistentStoreNeverHoldsConfigFileProviders(t *testing.T) {
	store := &persistentTestStore{NewMemoryOIDCProviderStore()}
	mgr := startServer(t, store, persistTestStaticIssuer)

	assert.Empty(t, storedARNs(t, store), "a config-file provider was written to the persistent store")
	assert.True(t, listedARNs(t, mgr)[arnOf(t, persistTestStaticIssuer)], "the IAM API no longer lists the config-file provider")
	rec, err := mgr.GetOIDCProvider(context.Background(), arnOf(t, persistTestStaticIssuer))
	require.NoError(t, err)
	assert.Equal(t, persistTestStaticIssuer, rec.URL)
	assert.True(t, stsKnowsIssuer(t, mgr, persistTestStaticIssuer), "STS stopped trusting the config-file provider")
}

// Servers sharing a store, with different config files, must not remove or
// replace each other's providers — including a zero-config server.
func TestServersSharingAStoreKeepEachOthersProviders(t *testing.T) {
	store := &persistentTestStore{NewMemoryOIDCProviderStore()}
	configured := startServer(t, store, persistTestStaticIssuer)
	createAPIProvider(t, configured, persistTestAPIIssuer)

	zeroConfig := startServer(t, store)

	assert.True(t, storedARNs(t, store)[arnOf(t, persistTestAPIIssuer)], "a peer's start removed an API-created provider")
	assert.True(t, listedARNs(t, configured)[arnOf(t, persistTestStaticIssuer)], "the configured server lost its config-file provider")
	assert.False(t, listedARNs(t, zeroConfig)[arnOf(t, persistTestStaticIssuer)], "a server lists a provider only its peer's config file defines")
	assert.True(t, stsKnowsIssuer(t, zeroConfig, persistTestAPIIssuer), "the peer does not trust the shared API-created provider")
	assert.False(t, stsKnowsIssuer(t, zeroConfig, persistTestStaticIssuer), "the peer trusts a provider only another server's config file defines")
}

// Removing a provider from the config file revokes it at the next start.
func TestRemovingAProviderFromTheConfigFileRevokesIt(t *testing.T) {
	store := &persistentTestStore{NewMemoryOIDCProviderStore()}
	startServer(t, store, persistTestStaticIssuer)

	restarted := startServer(t, store) // the file no longer lists it

	assert.False(t, listedARNs(t, restarted)[arnOf(t, persistTestStaticIssuer)])
	assert.False(t, stsKnowsIssuer(t, restarted, persistTestStaticIssuer), "a provider removed from the config file is still trusted")
}

// The API cannot change a provider the config file defines, nor create one
// with its ARN; the file is where it changes.
func TestConfigFileProvidersCannotBeChangedThroughTheAPI(t *testing.T) {
	store := &persistentTestStore{NewMemoryOIDCProviderStore()}
	mgr := startServer(t, store, persistTestStaticIssuer)
	ctx := context.Background()
	arn := arnOf(t, persistTestStaticIssuer)

	for name, err := range map[string]error{
		"add client ID":     mgr.AddClientIDToOIDCProvider(ctx, arn, "other"),
		"remove client ID":  mgr.RemoveClientIDFromOIDCProvider(ctx, arn, "aud"),
		"update thumbprint": mgr.UpdateOIDCProviderThumbprints(ctx, arn, []string{"9e99a48a9960b14926bb7f3b02e22da2b0ab7280"}),
		"tag":               mgr.TagOIDCProvider(ctx, arn, map[string]string{"k": "v"}),
		"untag":             mgr.UntagOIDCProvider(ctx, arn, []string{"k"}),
		"delete":            mgr.DeleteOIDCProvider(ctx, arn),
	} {
		assert.ErrorIs(t, err, ErrOIDCProviderStatic, name)
	}
	err := mgr.CreateOIDCProvider(ctx, &OIDCProviderRecord{ARN: arn, URL: persistTestStaticIssuer, ClientIDs: []string{"aud"}})
	assert.ErrorIs(t, err, ErrOIDCProviderAlreadyExists)
	assert.Empty(t, storedARNs(t, store), "a refused change still wrote to the store")
}

// Providers created through the API on an earlier boot, or on a peer, are
// trusted at startup rather than after the next change.
func TestStartupLoadsStoredProvidersIntoSTS(t *testing.T) {
	store := &persistentTestStore{NewMemoryOIDCProviderStore()}
	createAPIProvider(t, startServer(t, store), persistTestAPIIssuer)

	restarted := startServer(t, store)
	assert.True(t, stsKnowsIssuer(t, restarted, persistTestAPIIssuer))
}

// An in-memory store keeps its behaviour: the config file's providers are
// records in it, as before.
func TestInMemoryStoreStillHoldsConfigFileProviders(t *testing.T) {
	store := NewMemoryOIDCProviderStore()
	startServer(t, store, persistTestStaticIssuer)
	assert.True(t, storedARNs(t, store)[arnOf(t, persistTestStaticIssuer)])
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

	seeded := NewMemoryOIDCProviderStore()
	require.NoError(t, seeded.StoreProvider(context.Background(), "", &OIDCProviderRecord{
		ARN: arnOf(t, persistTestAPIIssuer), URL: persistTestAPIIssuer, ClientIDs: []string{"aud"},
	}))
	mgr := startServer(t, &unreachableThenReadyStore{MemoryOIDCProviderStore: seeded, failsLeft: 3})

	deadline := time.Now().Add(2 * time.Second)
	for !stsKnowsIssuer(t, mgr, persistTestAPIIssuer) {
		if time.Now().After(deadline) {
			t.Fatal("STS never learned the stored provider after the store became readable")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A provider stored under a config-file provider's ARN takes precedence, as it
// does in STS; the API then changes the stored one, and deleting it brings the
// config-file provider back.
func TestStoredProviderTakesPrecedenceOverTheConfigFileOne(t *testing.T) {
	ctx := context.Background()
	store := &persistentTestStore{NewMemoryOIDCProviderStore()}
	configured := startServer(t, store, persistTestStaticIssuer)
	peer := startServer(t, store)
	arn := arnOf(t, persistTestStaticIssuer)
	require.NoError(t, peer.CreateOIDCProvider(ctx, &OIDCProviderRecord{ARN: arn, URL: persistTestStaticIssuer, ClientIDs: []string{"stored"}}))

	rec, err := configured.GetOIDCProvider(ctx, arn)
	require.NoError(t, err)
	assert.Equal(t, []string{"stored"}, rec.ClientIDs, "the config-file provider hides the stored one")
	assert.NoError(t, configured.AddClientIDToOIDCProvider(ctx, arn, "more"), "the stored provider cannot be changed")

	require.NoError(t, configured.DeleteOIDCProvider(ctx, arn))
	rec, err = configured.GetOIDCProvider(ctx, arn)
	require.NoError(t, err)
	assert.Equal(t, []string{"aud"}, rec.ClientIDs, "deleting the stored provider did not bring the config-file one back")
}

// A store installed through SetOIDCProviderStore behaves like one installed
// at startup: stored providers load into STS and config-file providers stay
// visible to the IAM API.
func TestSetOIDCProviderStoreInstallsLikeStartup(t *testing.T) {
	store := &persistentTestStore{NewMemoryOIDCProviderStore()}
	require.NoError(t, store.StoreProvider(context.Background(), "", &OIDCProviderRecord{
		ARN: arnOf(t, persistTestAPIIssuer), URL: persistTestAPIIssuer, ClientIDs: []string{"aud"},
	}))

	cfg := persistTestConfig(persistTestStaticIssuer)
	mgr := NewIAMManager()
	require.NoError(t, mgr.Initialize(cfg, func() string { return "localhost:8888" }))
	mgr.SetOIDCProviderStore(store)

	assert.True(t, stsKnowsIssuer(t, mgr, persistTestAPIIssuer), "a stored provider was not trusted after install")
	assert.True(t, listedARNs(t, mgr)[arnOf(t, persistTestStaticIssuer)], "the IAM API no longer lists the config-file provider")
	assert.ErrorIs(t, mgr.DeleteOIDCProvider(context.Background(), arnOf(t, persistTestStaticIssuer)), ErrOIDCProviderStatic)
}

// A config-file provider reports no creation time: a time taken at startup
// would change with every restart.
func TestConfigFileProvidersReportNoCreationTime(t *testing.T) {
	store := &persistentTestStore{NewMemoryOIDCProviderStore()}
	mgr := startServer(t, store, persistTestStaticIssuer)
	rec, err := mgr.GetOIDCProvider(context.Background(), arnOf(t, persistTestStaticIssuer))
	require.NoError(t, err)
	assert.True(t, rec.CreatedAt.IsZero())
}

// countingUnreadableStore never becomes readable and counts the attempts.
type countingUnreadableStore struct {
	*MemoryOIDCProviderStore
	mu    sync.Mutex
	reads int
}

func (s *countingUnreadableStore) ListProviders(context.Context, string) ([]*OIDCProviderRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	return nil, errors.New("filer unavailable")
}

func (s *countingUnreadableStore) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

// Installing another store stops the previous one's startup retry.
func TestInstallingAnotherStoreStopsThePreviousRetry(t *testing.T) {
	saved := oidcHydrateRetry
	oidcHydrateRetry.initial, oidcHydrateRetry.max = time.Millisecond, time.Millisecond
	t.Cleanup(func() { oidcHydrateRetry = saved })

	first := &countingUnreadableStore{MemoryOIDCProviderStore: NewMemoryOIDCProviderStore()}
	mgr := startServer(t, first)
	deadline := time.Now().Add(2 * time.Second)
	for first.readCount() <= 1 {
		if time.Now().After(deadline) {
			t.Fatal("precondition: the first store is never retried")
		}
		time.Sleep(time.Millisecond)
	}

	mgr.installOIDCProviderStore(&persistentTestStore{NewMemoryOIDCProviderStore()}, persistTestConfig().STS)
	time.Sleep(10 * time.Millisecond) // let an in-flight attempt finish
	settled := first.readCount()
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, settled, first.readCount(), "the superseded store is still being retried")
}

// blockingListStore blocks the first ListProviders call after arm, having
// already read its snapshot, until release is closed.
type blockingListStore struct {
	*MemoryOIDCProviderStore
	mu      sync.Mutex
	armed   bool
	entered chan struct{}
	release chan struct{}
}

func (s *blockingListStore) arm() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armed, s.entered, s.release = true, make(chan struct{}), make(chan struct{})
}

func (s *blockingListStore) ListProviders(ctx context.Context, addr string) ([]*OIDCProviderRecord, error) {
	records, err := s.MemoryOIDCProviderStore.ListProviders(ctx, addr)
	s.mu.Lock()
	block := s.armed
	s.armed = false
	s.mu.Unlock()
	if block {
		close(s.entered)
		<-s.release
	}
	return records, err
}

// A refresh that read the store before a provider was deleted cannot leave the
// deleted provider trusted by finishing after the deletion's own refresh.
func TestAnOlderRefreshCannotRestoreADeletedProvider(t *testing.T) {
	store := &blockingListStore{MemoryOIDCProviderStore: NewMemoryOIDCProviderStore()}
	mgr := startServer(t, store)
	createAPIProvider(t, mgr, persistTestAPIIssuer)
	require.True(t, stsKnowsIssuer(t, mgr, persistTestAPIIssuer), "precondition: the provider is trusted")

	store.arm()
	stale := make(chan struct{})
	go func() {
		defer close(stale)
		_ = mgr.RefreshOIDCProvidersFromStore(context.Background())
	}()
	<-store.entered // the stale refresh holds a snapshot with the provider

	deleted := make(chan error, 1)
	go func() { deleted <- mgr.DeleteOIDCProvider(context.Background(), arnOf(t, persistTestAPIIssuer)) }()
	// Serialized, the deletion's refresh waits for the stale one; otherwise
	// let it finish first, which is the ordering that went wrong.
	var deleteErr error
	select {
	case deleteErr = <-deleted:
		deleted = nil
	case <-time.After(200 * time.Millisecond):
	}
	close(store.release)
	<-stale
	if deleted != nil {
		deleteErr = <-deleted
	}
	require.NoError(t, deleteErr)
	assert.False(t, stsKnowsIssuer(t, mgr, persistTestAPIIssuer), "a refresh older than the deletion left the deleted provider trusted")
}

// A refresh that fails is retried until the store answers. A change event reports each mutation once, so
// a subscription refresh that found the filer unreachable — mid-restart, say — left the provider set stale
// until some unrelated later change; a peer's new provider stayed untrusted and a deleted one trusted.
func TestAFailedRefreshIsRetriedUntilTheStoreAnswers(t *testing.T) {
	saved := oidcHydrateRetry
	oidcHydrateRetry.initial, oidcHydrateRetry.max = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { oidcHydrateRetry = saved })

	store := &unreachableThenReadyStore{MemoryOIDCProviderStore: NewMemoryOIDCProviderStore()}
	mgr := startServer(t, store)
	require.NoError(t, store.StoreProvider(context.Background(), "", &OIDCProviderRecord{
		ARN: arnOf(t, persistTestAPIIssuer), URL: persistTestAPIIssuer, ClientIDs: []string{"aud"},
	}))
	store.mu.Lock()
	store.failsLeft = 3
	store.mu.Unlock()
	require.Error(t, mgr.RefreshOIDCProvidersFromStore(context.Background()), "precondition: the refresh fails")

	deadline := time.Now().Add(2 * time.Second)
	for !stsKnowsIssuer(t, mgr, persistTestAPIIssuer) {
		if time.Now().After(deadline) {
			t.Fatal("a failed refresh was never retried: the stored provider stays untrusted until an unrelated change")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Failures during an outage start ONE retry, not one per event: a filer that is down for a while produces a
// change event per mutation, and each would otherwise add a goroutine polling the same store. Once the store
// answers and the retry ends, a later failure starts a new one.
func TestFailedRefreshesShareOneRetry(t *testing.T) {
	saved := oidcHydrateRetry
	oidcHydrateRetry.initial, oidcHydrateRetry.max = time.Millisecond, time.Millisecond
	t.Cleanup(func() { oidcHydrateRetry = saved })

	store := &unreachableThenReadyStore{MemoryOIDCProviderStore: NewMemoryOIDCProviderStore()}
	mgr := startServer(t, store)
	store.mu.Lock()
	store.failsLeft = 1 << 30
	store.mu.Unlock()
	for range 20 {
		require.Error(t, mgr.RefreshOIDCProvidersFromStore(context.Background()))
	}
	mgr.oidcRetryMu.Lock()
	started := mgr.oidcRetrySeq
	mgr.oidcRetryMu.Unlock()
	assert.Equal(t, uint64(1), started, "twenty failed refreshes started more than one retry")

	store.mu.Lock()
	store.failsLeft = 0
	store.mu.Unlock()
	deadline := time.Now().Add(2 * time.Second)
	for {
		mgr.oidcRetryMu.Lock()
		running := mgr.oidcRetryGen != 0
		mgr.oidcRetryMu.Unlock()
		if !running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the retry never ended after the store answered")
		}
		time.Sleep(time.Millisecond)
	}
	store.mu.Lock()
	store.failsLeft = 1
	store.mu.Unlock()
	require.Error(t, mgr.RefreshOIDCProvidersFromStore(context.Background()))
	mgr.oidcRetryMu.Lock()
	assert.Equal(t, uint64(2), mgr.oidcRetrySeq, "a failure after the retry ended starts a new one")
	mgr.oidcRetryMu.Unlock()
}

// flakyCountingStore fails while fail is set and counts the lists that succeed.
type flakyCountingStore struct {
	*MemoryOIDCProviderStore
	mu   sync.Mutex
	fail bool
	ok   int
}

func (s *flakyCountingStore) ListProviders(ctx context.Context, addr string) ([]*OIDCProviderRecord, error) {
	s.mu.Lock()
	if s.fail {
		s.mu.Unlock()
		return nil, errors.New("filer unavailable")
	}
	s.ok++
	s.mu.Unlock()
	return s.MemoryOIDCProviderStore.ListProviders(ctx, addr)
}

func (s *flakyCountingStore) set(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = fail
}

func (s *flakyCountingStore) successes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ok
}

// A refresh of store A that fails as store B is installed must not start a retry for A: nothing would cancel
// it, and when A answered it would replace B's providers (a removed one trusted again, B's own untrusted).
func TestAFailedRefreshOfASupersededStoreStartsNoRetry(t *testing.T) {
	a := &persistentTestStore{NewMemoryOIDCProviderStore()}
	mgr := startServer(t, a)
	mgr.SetOIDCProviderStore(&persistentTestStore{NewMemoryOIDCProviderStore()})
	mgr.oidcRetryMu.Lock()
	before := mgr.oidcRetrySeq
	mgr.oidcRetryMu.Unlock()

	mgr.startOIDCRetry(a) // the late call of A's failed refresh, after B's install

	mgr.oidcRetryMu.Lock()
	defer mgr.oidcRetryMu.Unlock()
	assert.Equal(t, before, mgr.oidcRetrySeq, "a retry was started for the store that was replaced")
	assert.Zero(t, mgr.oidcRetryGen)
}

// A refresh that listed store A before B was installed must not hand STS A's providers afterwards.
func TestASupersededSnapshotIsNotApplied(t *testing.T) {
	a := &persistentTestStore{NewMemoryOIDCProviderStore()}
	require.NoError(t, a.StoreProvider(context.Background(), "", &OIDCProviderRecord{
		ARN: arnOf(t, persistTestAPIIssuer), URL: persistTestAPIIssuer, ClientIDs: []string{"aud"},
	}))
	b := &persistentTestStore{NewMemoryOIDCProviderStore()}
	require.NoError(t, b.StoreProvider(context.Background(), "", &OIDCProviderRecord{
		ARN: arnOf(t, persistTestStaticIssuer), URL: persistTestStaticIssuer, ClientIDs: []string{"aud"},
	}))
	mgr := startServer(t, a)
	mgr.SetOIDCProviderStore(b)

	assert.Error(t, mgr.refreshOIDCProvidersFrom(context.Background(), a), "a snapshot of the replaced store was applied")
	assert.False(t, stsKnowsIssuer(t, mgr, persistTestAPIIssuer), "the replaced store's provider is trusted")
	assert.True(t, stsKnowsIssuer(t, mgr, persistTestStaticIssuer), "the current store's provider is not")
}

// A refresh that fails while a retry runs is not dropped: the retry may already have listed an older snapshot,
// so after its success it runs once more and picks up whatever the failed refresh would have seen.
func TestAFailureDuringARetryIsNotDropped(t *testing.T) {
	saved := oidcHydrateRetry
	oidcHydrateRetry.initial, oidcHydrateRetry.max = time.Millisecond, 2*time.Millisecond
	t.Cleanup(func() { oidcHydrateRetry = saved })

	store := &flakyCountingStore{MemoryOIDCProviderStore: NewMemoryOIDCProviderStore(), fail: true}
	mgr := startServer(t, store) // the startup load fails: a retry is running
	require.Error(t, mgr.RefreshOIDCProvidersFromStore(context.Background()), "a later refresh fails while it runs")
	store.set(false)

	deadline := time.Now().Add(2 * time.Second)
	for {
		mgr.oidcRetryMu.Lock()
		running := mgr.oidcRetryGen != 0
		mgr.oidcRetryMu.Unlock()
		if !running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the retry never ended")
		}
		time.Sleep(time.Millisecond)
	}
	assert.GreaterOrEqual(t, store.successes(), 2, "the retry ended on its first success, dropping the refresh that failed while it ran")
}
