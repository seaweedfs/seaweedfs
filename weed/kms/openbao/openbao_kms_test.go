package openbao

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	seaweedkms "github.com/seaweedfs/seaweedfs/weed/kms"
)

// mapConfig is a util.Configuration backed by a map.
type mapConfig map[string]interface{}

func (c mapConfig) GetString(key string) string        { s, _ := c[key].(string); return s }
func (c mapConfig) GetBool(key string) bool            { b, _ := c[key].(bool); return b }
func (c mapConfig) GetInt(key string) int              { i, _ := c[key].(int); return i }
func (c mapConfig) GetStringSlice(key string) []string { s, _ := c[key].([]string); return s }
func (c mapConfig) SetDefault(string, interface{})     {}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// fakeBao issues a new token on every AppRole login and accepts requests only with the
// latest one, like OpenBao once the previous token has expired.
type fakeBao struct {
	mu              sync.Mutex
	clock           *fakeClock
	logins          int
	failedLogins    int
	refused         int
	lookups         int
	valid           string
	denyAll         bool          // refuse every transit request, like a policy without the key
	failLogins      bool          // refuse every login, like a revoked secret ID
	noDefaultPolicy bool          // issue tokens without the default policy, so lookup-self is refused
	loginGate       chan struct{} // when set, a login hangs until it is closed
	loginStarted    chan struct{} // signalled when a login starts hanging
	loginAbandoned  chan struct{} // signalled when its client gives up on a hanging login
	holdRefusals    int           // when set, refusals are answered only once this many have arrived
	refusalsHeld    chan struct{} // closed when they have
}

func (f *fakeBao) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/v1/auth/token/lookup-self" {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.lookups++
		token := r.Header.Get("X-Vault-Token")
		if f.noDefaultPolicy || token == "" || token != f.valid {
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(`{"errors":["permission denied"]}`))
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"data": map[string]interface{}{"id": token}})
		return
	}
	if r.URL.Path == "/v1/auth/approle/login" {
		f.mu.Lock()
		gate, started, abandoned := f.loginGate, f.loginStarted, f.loginAbandoned
		f.mu.Unlock()
		if gate != nil {
			io.Copy(io.Discard, r.Body) // only a fully read request notices its client leaving
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-gate:
			case <-r.Context().Done():
				select {
				case abandoned <- struct{}{}:
				default:
				}
				return // the client gave up: no token
			}
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.failLogins {
			f.failedLogins++
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"errors":["invalid secret id"]}`))
			return
		}
		f.logins++
		f.valid = fmt.Sprintf("token-%d", f.logins)
		policies := []string{"default", "kms"}
		if f.noDefaultPolicy {
			policies = []string{"kms"}
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"auth": map[string]interface{}{"client_token": f.valid, "lease_duration": 3600, "policies": policies},
		})
		return
	}
	f.mu.Lock()
	token := r.Header.Get("X-Vault-Token")
	if f.denyAll || token == "" || token != f.valid {
		f.refused++
		if f.refusalsHeld != nil && f.refused == f.holdRefusals {
			close(f.refusalsHeld)
		}
		held := f.refusalsHeld
		f.mu.Unlock()
		if held != nil {
			select {
			case <-held:
			case <-r.Context().Done():
			}
		}
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"errors":["permission denied"]}`))
		return
	}
	f.mu.Unlock()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data": map[string]interface{}{"ciphertext": "vault:v1:c2VhbGVk"},
	})
}

// expire makes the current token invalid, as when its TTL runs out.
func (f *fakeBao) expire() {
	f.revoke()
	f.clock.advance(time.Hour)
}

// revoke makes the current token invalid at once, as when it is revoked or runs out of uses.
func (f *fakeBao) revoke() {
	f.mu.Lock()
	f.valid = ""
	f.mu.Unlock()
}

// hangLogins makes the following logins wait until release is called; a failed test
// releases them too, so the test server can shut down.
func (f *fakeBao) hangLogins(t *testing.T) (release func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	gate := make(chan struct{})
	f.loginGate = gate
	f.loginStarted = make(chan struct{}, 1)
	f.loginAbandoned = make(chan struct{}, 1)
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	return release
}

// holdRefusalsUntil answers no refusal before n requests have been refused, so they overlap.
func (f *fakeBao) holdRefusalsUntil(n int) {
	f.mu.Lock()
	f.holdRefusals = n
	f.refusalsHeld = make(chan struct{})
	f.mu.Unlock()
}

func (f *fakeBao) counts() (logins, failedLogins, refused int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.logins, f.failedLogins, f.refused
}

func (f *fakeBao) lookupCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lookups
}

func (f *fakeBao) loginCount() int {
	logins, _, _ := f.counts()
	return logins
}

func newTestProvider(t *testing.T, bao *fakeBao, config mapConfig) *OpenBaoKMSProvider {
	t.Helper()
	t.Setenv("VAULT_AGENT_ADDR", "") // would take the place of the test server's address
	server := httptest.NewServer(bao)
	t.Cleanup(server.Close)
	config["address"] = server.URL
	provider, err := NewOpenBaoKMSProvider(config)
	if err != nil {
		t.Fatalf("NewOpenBaoKMSProvider: %v", err)
	}
	t.Cleanup(func() { provider.Close() })
	p := provider.(*OpenBaoKMSProvider)
	bao.clock = &fakeClock{t: time.Now()}
	p.now = bao.clock.now
	return p
}

func generateDataKey(ctx context.Context, provider seaweedkms.KMSProvider) error {
	_, err := provider.GenerateDataKey(ctx, &seaweedkms.GenerateDataKeyRequest{
		KeyID:   "test-key",
		KeySpec: seaweedkms.KeySpecAES256,
	})
	return err
}

// waitFor fails the test if ch gives nothing within a few seconds.
func waitFor[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

// wantDeadline checks that a request ended by its 100ms deadline, not sooner and not much later.
func wantDeadline(t *testing.T, err error, elapsed time.Duration) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("err = %v, want the request's deadline", err)
	}
	if elapsed < 90*time.Millisecond || elapsed > time.Second {
		t.Fatalf("request returned after %v, want about its 100ms deadline", elapsed)
	}
}

func appRoleConfig() mapConfig {
	return mapConfig{"role_id": "role", "secret_id": "secret"}
}

func wantAccessDenied(t *testing.T, err error) {
	t.Helper()
	var kmsErr *seaweedkms.KMSError
	if !errors.As(err, &kmsErr) || kmsErr.Code != seaweedkms.ErrCodeAccessDenied {
		t.Fatalf("err = %v, want %s", err, seaweedkms.ErrCodeAccessDenied)
	}
}

// generateConcurrently runs n requests at once and returns their errors.
func generateConcurrently(provider seaweedkms.KMSProvider, n int) []error {
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = generateDataKey(context.Background(), provider)
		}(i)
	}
	wg.Wait()
	return errs
}

func TestAppRoleLogsInAgainWhenTokenExpires(t *testing.T) {
	bao := &fakeBao{}
	provider := newTestProvider(t, bao, appRoleConfig())

	if err := generateDataKey(context.Background(), provider); err != nil {
		t.Fatalf("first request: %v", err)
	}
	bao.expire()
	if err := generateDataKey(context.Background(), provider); err != nil {
		t.Fatalf("request after the token expired: %v", err)
	}
	if got := bao.loginCount(); got != 2 {
		t.Fatalf("logins = %d, want 2", got)
	}
}

func TestConcurrentRefusalsLogInOnce(t *testing.T) {
	bao := &fakeBao{}
	provider := newTestProvider(t, bao, appRoleConfig())
	bao.expire()
	bao.holdRefusalsUntil(20)

	for _, err := range generateConcurrently(provider, 20) {
		if err != nil {
			t.Fatalf("request after the token expired: %v", err)
		}
	}
	if got := bao.loginCount(); got != 2 {
		t.Fatalf("logins = %d, want 2", got)
	}
}

func TestConcurrentRefusalsShareAFailedLogin(t *testing.T) {
	bao := &fakeBao{}
	provider := newTestProvider(t, bao, appRoleConfig())
	release := bao.hangLogins(t)
	bao.mu.Lock()
	bao.failLogins = true
	bao.mu.Unlock()
	bao.expire()
	bao.holdRefusalsUntil(20)

	var waiting atomic.Int32
	provider.reloginWaiting = func() { waiting.Add(1) }

	done := make(chan []error, 1)
	go func() { done <- generateConcurrently(provider, 20) }()
	// Fail the hanging login only once all 20 requests are waiting for it.
	waitFor(t, bao.loginStarted, "the login")
	for deadline := time.Now().Add(5 * time.Second); waiting.Load() < 20; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d of 20 requests wait for the login", waiting.Load())
		}
	}
	release()

	for _, err := range waitFor(t, done, "the requests") {
		wantAccessDenied(t, err)
	}
	if _, failed, _ := bao.counts(); failed != 1 {
		t.Fatalf("failed logins = %d, want 1", failed)
	}
}

func TestFailedLoginBacksOff(t *testing.T) {
	bao := &fakeBao{}
	provider := newTestProvider(t, bao, appRoleConfig())
	bao.mu.Lock()
	bao.failLogins = true
	bao.mu.Unlock()
	bao.expire()

	// One failed login, then refused requests wait out the backoff instead of logging in each.
	for i := 0; i < 5; i++ {
		wantAccessDenied(t, generateDataKey(context.Background(), provider))
	}
	if _, failed, _ := bao.counts(); failed != 1 {
		t.Fatalf("failed logins = %d, want 1", failed)
	}

	// After the backoff the next refusal tries again, and recovers once OpenBao lets it log in.
	bao.clock.advance(reloginFailureBackoff)
	wantAccessDenied(t, generateDataKey(context.Background(), provider))
	if _, failed, _ := bao.counts(); failed != 2 {
		t.Fatalf("failed logins = %d, want 2", failed)
	}
	bao.mu.Lock()
	bao.failLogins = false
	bao.mu.Unlock()
	bao.clock.advance(reloginFailureBackoff)
	if err := generateDataKey(context.Background(), provider); err != nil {
		t.Fatalf("request once logins work again: %v", err)
	}
}

func TestHungLoginGivesUpWithoutAClientTimeout(t *testing.T) {
	config := appRoleConfig()
	config["request_timeout"] = -1 // leaves Vault requests without a timeout
	bao := &fakeBao{}
	provider := newTestProvider(t, bao, config)
	provider.reloginTimeout = 100 * time.Millisecond
	release := bao.hangLogins(t)
	bao.expire()

	// The shared login gives up on its own, so it cannot hold up every later recovery.
	result := make(chan error, 1)
	go func() { result <- generateDataKey(context.Background(), provider) }()
	wantAccessDenied(t, waitFor(t, result, "the request while the login hung"))
	waitFor(t, bao.loginAbandoned, "the hung login to be given up")

	release()
	bao.clock.advance(reloginFailureBackoff)
	if err := generateDataKey(context.Background(), provider); err != nil {
		t.Fatalf("request once OpenBao answers logins: %v", err)
	}
	if got := bao.loginCount(); got != 2 {
		t.Fatalf("logins = %d, want 2", got)
	}
}

func TestRevokedFreshTokenIsReplaced(t *testing.T) {
	bao := &fakeBao{}
	provider := newTestProvider(t, bao, appRoleConfig())

	bao.revoke()
	if err := generateDataKey(context.Background(), provider); err != nil {
		t.Fatalf("request after the token was revoked: %v", err)
	}
	if got := bao.loginCount(); got != 2 {
		t.Fatalf("logins = %d, want 2", got)
	}
}

func TestPolicyDenialDoesNotLogIn(t *testing.T) {
	bao := &fakeBao{denyAll: true}
	provider := newTestProvider(t, bao, appRoleConfig())

	// lookup-self finds the token valid, however old it is, so the refusal is a policy denial.
	for i := 0; i < 10; i++ {
		if i == 5 {
			bao.clock.advance(time.Hour)
		}
		wantAccessDenied(t, generateDataKey(context.Background(), provider))
	}
	if logins, lookups := bao.loginCount(), bao.lookupCount(); logins != 1 || lookups != 10 {
		t.Fatalf("logins = %d, lookups = %d, want 1 and 10", logins, lookups)
	}
}

func TestWithoutDefaultPolicyRefusalOfAFreshTokenIsAPolicyDenial(t *testing.T) {
	bao := &fakeBao{denyAll: true, noDefaultPolicy: true}
	provider := newTestProvider(t, bao, appRoleConfig())

	// A token just issued has not expired, so a refusal is not a reason to log in.
	for i := 0; i < 5; i++ {
		wantAccessDenied(t, generateDataKey(context.Background(), provider))
	}
	if got := bao.loginCount(); got != 1 {
		t.Fatalf("logins = %d, want 1", got)
	}

	// An older token may have expired: one new login and a retry with the new token, which
	// is not replaced again.
	bao.clock.advance(time.Hour)
	for i := 0; i < 5; i++ {
		wantAccessDenied(t, generateDataKey(context.Background(), provider))
	}
	if logins, _, refused := bao.counts(); logins != 2 || refused != 11 {
		t.Fatalf("logins = %d, refused requests = %d, want 2 and 11 (5 + the retry + 5)", logins, refused)
	}
	if got := bao.lookupCount(); got != 0 {
		t.Fatalf("lookups = %d, want 0: the token may not look itself up", got)
	}
}

func TestStaticTokenIsNotReplaced(t *testing.T) {
	bao := &fakeBao{}
	provider := newTestProvider(t, bao, mapConfig{"token": "static"})

	wantAccessDenied(t, generateDataKey(context.Background(), provider))
	if got := bao.loginCount(); got != 0 {
		t.Fatalf("logins = %d, want 0", got)
	}
}

func TestRequestStopsWaitingForItsLoginAtItsDeadline(t *testing.T) {
	bao := &fakeBao{}
	provider := newTestProvider(t, bao, appRoleConfig())
	release := bao.hangLogins(t)
	bao.expire()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := generateDataKey(ctx, provider)
	wantDeadline(t, err, time.Since(start))
	waitFor(t, bao.loginStarted, "the login")

	// The login it started is not cancelled with it: once OpenBao answers, requests work.
	release()
	if err := generateDataKey(context.Background(), provider); err != nil {
		t.Fatalf("request after the login finished: %v", err)
	}
	if got := bao.loginCount(); got != 2 {
		t.Fatalf("logins = %d, want 2", got)
	}
}

func TestRequestStopsWaitingForAnotherLoginAtItsDeadline(t *testing.T) {
	bao := &fakeBao{}
	provider := newTestProvider(t, bao, appRoleConfig())
	release := bao.hangLogins(t)
	bao.expire()

	first := make(chan error, 1)
	go func() { first <- generateDataKey(context.Background(), provider) }()
	waitFor(t, bao.loginStarted, "the login")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := generateDataKey(ctx, provider)
	wantDeadline(t, err, time.Since(start))

	release()
	if err := waitFor(t, first, "the request that started the login"); err != nil {
		t.Fatalf("request that started the login: %v", err)
	}
	if got := bao.loginCount(); got != 2 {
		t.Fatalf("logins = %d, want 2", got)
	}
}
