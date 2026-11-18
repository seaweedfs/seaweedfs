package openbao

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
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

// fakeBao issues a new token on every AppRole login and accepts transit requests only
// with the latest one, like OpenBao once the previous token has expired.
type fakeBao struct {
	mu           sync.Mutex
	clock        *fakeClock
	logins       int
	failedLogins int
	refused      int
	valid        string
	denyAll      bool          // refuse every transit request, like a policy without the key
	failLogins   bool          // refuse every login, like a revoked secret ID
	loginGate    chan struct{} // when set, a login hangs until it is closed
	loginStarted chan struct{} // signalled when a login starts hanging
	holdRefusals int           // when set, refusals are answered only once this many have arrived
	refusalsHeld chan struct{} // closed when they have
}

func (f *fakeBao) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/v1/auth/approle/login" {
		f.mu.Lock()
		gate, started := f.loginGate, f.loginStarted
		f.mu.Unlock()
		if gate != nil {
			select {
			case started <- struct{}{}:
			default:
			}
			<-gate
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
		json.NewEncoder(w).Encode(map[string]interface{}{
			"auth": map[string]interface{}{"client_token": f.valid, "lease_duration": 3600},
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
			<-held
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
	f.mu.Lock()
	f.valid = ""
	f.mu.Unlock()
	f.clock.advance(time.Hour)
}

// hangLogins makes the following logins wait until release is called; a failed test
// releases them too, so the test server can shut down.
func (f *fakeBao) hangLogins(t *testing.T) (release func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	gate := make(chan struct{})
	f.loginGate = gate
	f.loginStarted = make(chan struct{}, 1)
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

	done := make(chan []error)
	go func() { done <- generateConcurrently(provider, 20) }()
	// All 20 are refused at once; give them a moment to join the hanging login before it fails.
	<-bao.loginStarted
	time.Sleep(50 * time.Millisecond)
	release()

	for _, err := range <-done {
		wantAccessDenied(t, err)
	}
	if _, failed, _ := bao.counts(); failed != 1 {
		t.Fatalf("failed logins = %d, want 1", failed)
	}
}

func TestRefusalOfAFreshTokenIsAPolicyDenial(t *testing.T) {
	bao := &fakeBao{denyAll: true}
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
	if err := generateDataKey(ctx, provider); err == nil {
		t.Fatal("request succeeded while the login hung")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("request returned after %v, want about its 100ms deadline", elapsed)
	}

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
	<-bao.loginStarted

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := generateDataKey(ctx, provider); err == nil {
		t.Fatal("request succeeded while the login hung")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("request returned after %v, want about its 100ms deadline", elapsed)
	}

	release()
	if err := <-first; err != nil {
		t.Fatalf("request that started the login: %v", err)
	}
	if got := bao.loginCount(); got != 2 {
		t.Fatalf("logins = %d, want 2", got)
	}
}
