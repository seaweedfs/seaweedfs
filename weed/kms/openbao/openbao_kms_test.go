package openbao

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// fakeBao issues a token that expires after ttl seconds on every AppRole login and accepts
// transit requests only with a token that has not expired, like OpenBao.
type fakeBao struct {
	mu           sync.Mutex
	ttl          int // 0 issues tokens that never expire
	logins       int
	failedLogins int
	failLogins   int           // how many of the next logins to refuse
	hangLogins   bool          // logins after the first hang until their client leaves
	loginDelay   time.Duration // logins after the first answer this late; the token's TTL runs from the request
	loginHung    chan struct{} // signalled when such a login is given up
	expiry       map[string]time.Time
}

func (f *fakeBao) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	io.Copy(io.Discard, r.Body) // only a fully read request notices its client leaving
	f.mu.Lock()
	if r.URL.Path == "/v1/auth/approle/login" {
		if f.hangLogins && f.logins > 0 {
			hung := f.loginHung
			f.mu.Unlock()
			<-r.Context().Done()
			hung <- struct{}{}
			return
		}
		if f.failLogins > 0 {
			f.failLogins--
			f.failedLogins++
			f.mu.Unlock()
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"errors":["invalid secret id"]}`))
			return
		}
		f.logins++
		token := fmt.Sprintf("token-%d", f.logins)
		if f.expiry == nil {
			f.expiry = map[string]time.Time{}
		}
		f.expiry[token] = time.Time{}
		if f.ttl > 0 {
			f.expiry[token] = time.Now().Add(time.Duration(f.ttl) * time.Second)
		}
		ttl := f.ttl
		delay := time.Duration(0)
		if f.logins > 1 {
			delay = f.loginDelay
		}
		f.mu.Unlock()
		time.Sleep(delay)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"auth": map[string]interface{}{"client_token": token, "lease_duration": ttl},
		})
		return
	}
	expires, issued := f.expiry[r.Header.Get("X-Vault-Token")]
	f.mu.Unlock()
	if !issued || (!expires.IsZero() && time.Now().After(expires)) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"errors":["permission denied"]}`))
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data": map[string]interface{}{"ciphertext": "vault:v1:c2VhbGVk"},
	})
}

func (f *fakeBao) counts() (logins, failedLogins int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.logins, f.failedLogins
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
	return provider.(*OpenBaoKMSProvider)
}

func appRoleConfig() mapConfig {
	return mapConfig{"role_id": "role", "secret_id": "secret"}
}

// setForTest changes a package variable for the length of the test.
func setForTest[T any](t *testing.T, v *T, value T) {
	saved := *v
	*v = value
	t.Cleanup(func() { *v = saved })
}

// keepRequesting sends a request every 100ms for d and fails the test on the first error.
func keepRequesting(t *testing.T, provider seaweedkms.KMSProvider, d time.Duration) {
	t.Helper()
	start := time.Now()
	for time.Since(start) < d {
		_, err := provider.GenerateDataKey(context.Background(), &seaweedkms.GenerateDataKeyRequest{
			KeyID:   "test-key",
			KeySpec: seaweedkms.KeySpecAES256,
		})
		if err != nil {
			t.Fatalf("request %v after the start: %v", time.Since(start).Round(time.Millisecond), err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// closeWithin closes the provider and fails the test if that takes more than a few seconds.
func closeWithin(t *testing.T, provider *OpenBaoKMSProvider) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		provider.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}
}

func TestAppRoleTokenIsRefreshedBeforeItExpires(t *testing.T) {
	bao := &fakeBao{ttl: 2}
	provider := newTestProvider(t, bao, appRoleConfig())

	keepRequesting(t, provider, 3500*time.Millisecond)
	if logins, _ := bao.counts(); logins < 3 {
		t.Fatalf("logins = %d, want at least 3 in 3.5 s of 2 s tokens", logins)
	}
}

func TestRefreshIsTimedFromTheLoginStart(t *testing.T) {
	// 3 s tokens whose login answers after 0.75 s: timed from the response, the second
	// refresh would start at 4.75 s and its token arrive at 5.5 s, after the token issued at
	// 2 s expired at 5 s; timed from the login start it arrives at 4.75 s.
	bao := &fakeBao{ttl: 3, loginDelay: 750 * time.Millisecond}
	provider := newTestProvider(t, bao, appRoleConfig())

	keepRequesting(t, provider, 6*time.Second)
}

func TestRefreshSurvivesAConfigurationReload(t *testing.T) {
	t.Setenv("VAULT_AGENT_ADDR", "")
	bao := &fakeBao{ttl: 2}
	server := httptest.NewServer(bao)
	t.Cleanup(server.Close)
	manager := seaweedkms.InitializeKMSManager()
	t.Cleanup(func() { manager.RemoveKMSProvider("reload-test") })
	config := &seaweedkms.KMSConfig{Provider: "openbao", Config: map[string]interface{}{
		"address": server.URL, "role_id": "role", "secret_id": "secret",
	}}

	// A reload closes the provider it replaces; the one that stays must keep refreshing.
	for i := 0; i < 2; i++ {
		if err := manager.AddKMSProvider("reload-test", config); err != nil {
			t.Fatalf("AddKMSProvider: %v", err)
		}
	}
	provider, err := manager.GetKMSProviderByName("reload-test")
	if err != nil {
		t.Fatalf("GetKMSProviderByName: %v", err)
	}
	keepRequesting(t, provider, 3500*time.Millisecond)
}

func TestFailedRefreshIsRetried(t *testing.T) {
	setForTest(t, &refreshRetryInterval, 100*time.Millisecond)
	bao := &fakeBao{ttl: 2}
	provider := newTestProvider(t, bao, appRoleConfig())
	bao.mu.Lock()
	bao.failLogins = 2
	bao.mu.Unlock()

	// The refresh at 1.33 s fails twice and succeeds 200 ms later, before the token expires.
	keepRequesting(t, provider, 3*time.Second)
	if logins, failed := bao.counts(); failed != 2 || logins < 2 {
		t.Fatalf("logins = %d, failed logins = %d, want at least 2 and 2", logins, failed)
	}
}

func TestCloseStopsTheRefresh(t *testing.T) {
	bao := &fakeBao{ttl: 1}
	provider := newTestProvider(t, bao, appRoleConfig())

	closeWithin(t, provider)
	time.Sleep(time.Second) // past the refresh due at 0.67 s
	if logins, _ := bao.counts(); logins != 1 {
		t.Fatalf("logins = %d, want 1", logins)
	}
}

func TestHangingRefreshIsGivenUpAndRetried(t *testing.T) {
	setForTest(t, &refreshRetryInterval, 100*time.Millisecond)
	setForTest(t, &refreshLoginTimeout, 100*time.Millisecond)
	config := appRoleConfig()
	config["request_timeout"] = -1 // leaves the client without a timeout
	bao := &fakeBao{ttl: 1, hangLogins: true, loginHung: make(chan struct{}, 10)}
	provider := newTestProvider(t, bao, config)

	for i := 0; i < 2; i++ {
		select {
		case <-bao.loginHung:
		case <-time.After(5 * time.Second):
			t.Fatalf("%d hanging refresh logins were given up, want 2", i)
		}
	}
	closeWithin(t, provider)
}

func TestNoRefreshWithoutAnExpiringToken(t *testing.T) {
	for name, config := range map[string]mapConfig{
		"static token":          {"token": "static"},
		"AppRole without a TTL": appRoleConfig(),
	} {
		t.Run(name, func(t *testing.T) {
			provider := newTestProvider(t, &fakeBao{ttl: 0}, config)
			if provider.stopRefresh != nil {
				t.Fatal("a refresh was started")
			}
		})
	}
}
