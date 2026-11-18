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

	seaweedkms "github.com/seaweedfs/seaweedfs/weed/kms"
)

// mapConfig is a util.Configuration backed by a map.
type mapConfig map[string]interface{}

func (c mapConfig) GetString(key string) string        { s, _ := c[key].(string); return s }
func (c mapConfig) GetBool(key string) bool            { b, _ := c[key].(bool); return b }
func (c mapConfig) GetInt(key string) int              { i, _ := c[key].(int); return i }
func (c mapConfig) GetStringSlice(key string) []string { s, _ := c[key].([]string); return s }
func (c mapConfig) SetDefault(string, interface{})     {}

// fakeBao issues a new token on every AppRole login and accepts transit requests only
// with the latest one, like OpenBao once the previous token has expired.
type fakeBao struct {
	mu      sync.Mutex
	logins  int
	valid   string
	denyAll bool // refuse every transit request, like a policy without the key
}

func (f *fakeBao) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.URL.Path == "/v1/auth/approle/login" {
		f.logins++
		f.valid = fmt.Sprintf("token-%d", f.logins)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"auth": map[string]interface{}{"client_token": f.valid, "lease_duration": 3600},
		})
		return
	}
	token := r.Header.Get("X-Vault-Token")
	if f.denyAll || token == "" || token != f.valid {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"errors":["permission denied"]}`))
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data": map[string]interface{}{"ciphertext": "vault:v1:c2VhbGVk"},
	})
}

// expire makes the current token invalid, as when its TTL runs out.
func (f *fakeBao) expire() {
	f.mu.Lock()
	f.valid = ""
	f.mu.Unlock()
}

func (f *fakeBao) loginCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.logins
}

func newTestProvider(t *testing.T, bao *fakeBao, config mapConfig) seaweedkms.KMSProvider {
	t.Helper()
	server := httptest.NewServer(bao)
	t.Cleanup(server.Close)
	config["address"] = server.URL
	provider, err := NewOpenBaoKMSProvider(config)
	if err != nil {
		t.Fatalf("NewOpenBaoKMSProvider: %v", err)
	}
	t.Cleanup(func() { provider.Close() })
	return provider
}

func generateDataKey(provider seaweedkms.KMSProvider) error {
	_, err := provider.GenerateDataKey(context.Background(), &seaweedkms.GenerateDataKeyRequest{
		KeyID:   "test-key",
		KeySpec: seaweedkms.KeySpecAES256,
	})
	return err
}

func appRoleConfig() mapConfig {
	return mapConfig{"role_id": "role", "secret_id": "secret"}
}

func TestAppRoleLogsInAgainWhenTokenExpires(t *testing.T) {
	bao := &fakeBao{}
	provider := newTestProvider(t, bao, appRoleConfig())

	if err := generateDataKey(provider); err != nil {
		t.Fatalf("first request: %v", err)
	}
	bao.expire()
	if err := generateDataKey(provider); err != nil {
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

	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- generateDataKey(provider)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("request after the token expired: %v", err)
		}
	}
	if got := bao.loginCount(); got != 2 {
		t.Fatalf("logins = %d, want 2", got)
	}
}

func TestPersistentRefusalIsReturnedAfterOneLogin(t *testing.T) {
	bao := &fakeBao{denyAll: true}
	provider := newTestProvider(t, bao, appRoleConfig())

	err := generateDataKey(provider)
	var kmsErr *seaweedkms.KMSError
	if !errors.As(err, &kmsErr) || kmsErr.Code != seaweedkms.ErrCodeAccessDenied {
		t.Fatalf("err = %v, want %s", err, seaweedkms.ErrCodeAccessDenied)
	}
	if got := bao.loginCount(); got != 2 {
		t.Fatalf("logins = %d, want 2", got)
	}
}

func TestStaticTokenIsNotReplaced(t *testing.T) {
	bao := &fakeBao{}
	provider := newTestProvider(t, bao, mapConfig{"token": "static"})

	err := generateDataKey(provider)
	var kmsErr *seaweedkms.KMSError
	if !errors.As(err, &kmsErr) || kmsErr.Code != seaweedkms.ErrCodeAccessDenied {
		t.Fatalf("err = %v, want %s", err, seaweedkms.ErrCodeAccessDenied)
	}
	if got := bao.loginCount(); got != 0 {
		t.Fatalf("logins = %d, want 0", got)
	}
}
