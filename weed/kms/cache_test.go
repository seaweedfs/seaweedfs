package kms

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// countingKMSProvider is a minimal KMSProvider that records how many times each
// method is invoked so tests can assert the cache actually avoids round-trips.
type countingKMSProvider struct {
	decryptCalls  int64
	generateCalls int64

	mu    sync.Mutex
	store map[string][]byte // ciphertext(string) -> plaintext data key
}

func newCountingKMSProvider() *countingKMSProvider {
	return &countingKMSProvider{store: make(map[string][]byte)}
}

func (p *countingKMSProvider) GenerateDataKey(ctx context.Context, req *GenerateDataKeyRequest) (*GenerateDataKeyResponse, error) {
	n := atomic.AddInt64(&p.generateCalls, 1)
	plaintext := []byte(fmt.Sprintf("datakey-%d-abcdefghijklmnopqrstuv", n))[:32]
	ciphertext := []byte(fmt.Sprintf("cipher-%d", n))

	p.mu.Lock()
	p.store[string(ciphertext)] = append([]byte(nil), plaintext...)
	p.mu.Unlock()

	return &GenerateDataKeyResponse{
		KeyID:          req.KeyID,
		Plaintext:      plaintext,
		CiphertextBlob: ciphertext,
	}, nil
}

func (p *countingKMSProvider) Decrypt(ctx context.Context, req *DecryptRequest) (*DecryptResponse, error) {
	atomic.AddInt64(&p.decryptCalls, 1)

	p.mu.Lock()
	plaintext, ok := p.store[string(req.CiphertextBlob)]
	p.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("ciphertext not found")
	}

	// Return a fresh copy, mirroring real providers that allocate per call.
	return &DecryptResponse{
		KeyID:     "test-key",
		Plaintext: append([]byte(nil), plaintext...),
	}, nil
}

func (p *countingKMSProvider) DescribeKey(ctx context.Context, req *DescribeKeyRequest) (*DescribeKeyResponse, error) {
	return &DescribeKeyResponse{KeyID: req.KeyID, KeyState: KeyStateEnabled, KeyUsage: KeyUsageEncryptDecrypt}, nil
}

func (p *countingKMSProvider) GetKeyID(ctx context.Context, keyIdentifier string) (string, error) {
	return keyIdentifier, nil
}

func (p *countingKMSProvider) Close() error { return nil }

// TestCachedProviderDecryptHitsProviderOnce is the regression test for the
// reported bug: with caching enabled, repeated Decrypt of the same data key
// must consult the underlying KMS only once.
func TestCachedProviderDecryptHitsProviderOnce(t *testing.T) {
	inner := newCountingKMSProvider()
	cached := NewCachedKMSProvider(inner, time.Hour, 100)

	ctx := context.Background()
	dk, err := cached.GenerateDataKey(ctx, &GenerateDataKeyRequest{KeyID: "k1", KeySpec: KeySpecAES256})
	if err != nil {
		t.Fatalf("GenerateDataKey: %v", err)
	}

	req := &DecryptRequest{CiphertextBlob: dk.CiphertextBlob}
	first, err := cached.Decrypt(ctx, req)
	if err != nil {
		t.Fatalf("first Decrypt: %v", err)
	}

	// Emulate the S3 read path clearing the returned plaintext after use. A
	// correct cache must hand back an independent copy so this does not wipe
	// the cached key material.
	ClearSensitiveData(first.Plaintext)

	second, err := cached.Decrypt(ctx, req)
	if err != nil {
		t.Fatalf("second Decrypt: %v", err)
	}

	if got := atomic.LoadInt64(&inner.decryptCalls); got != 1 {
		t.Fatalf("expected underlying Decrypt to be called once, got %d", got)
	}

	// The cached result must still be the real data key. If the cache handed
	// back the same buffer it stored, the ClearSensitiveData above would have
	// zeroed it.
	if bytes.Equal(second.Plaintext, make([]byte, len(second.Plaintext))) {
		t.Fatalf("cached plaintext was zeroed by a previous caller clearing its copy")
	}
}

// TestCachedProviderContextSensitivity ensures the encryption context is part
// of the cache key: a different context must not return a stale plaintext.
func TestCachedProviderContextSensitivity(t *testing.T) {
	inner := newCountingKMSProvider()
	cached := NewCachedKMSProvider(inner, time.Hour, 100)
	ctx := context.Background()

	dk, err := cached.GenerateDataKey(ctx, &GenerateDataKeyRequest{KeyID: "k1", KeySpec: KeySpecAES256})
	if err != nil {
		t.Fatalf("GenerateDataKey: %v", err)
	}

	// Same ciphertext, two different contexts. The counting provider ignores
	// context, but the cache must still treat these as distinct entries so a
	// real provider's context check is never short-circuited.
	if _, err := cached.Decrypt(ctx, &DecryptRequest{CiphertextBlob: dk.CiphertextBlob, EncryptionContext: map[string]string{"a": "1"}}); err != nil {
		t.Fatalf("Decrypt ctx a: %v", err)
	}
	if _, err := cached.Decrypt(ctx, &DecryptRequest{CiphertextBlob: dk.CiphertextBlob, EncryptionContext: map[string]string{"a": "2"}}); err != nil {
		t.Fatalf("Decrypt ctx b: %v", err)
	}
	if got := atomic.LoadInt64(&inner.decryptCalls); got != 2 {
		t.Fatalf("expected 2 underlying Decrypt calls for distinct contexts, got %d", got)
	}
}

// TestCachedProviderTTLExpiry verifies expired entries fall through to the
// underlying provider.
func TestCachedProviderTTLExpiry(t *testing.T) {
	inner := newCountingKMSProvider()
	cached := NewCachedKMSProvider(inner, 20*time.Millisecond, 100)
	ctx := context.Background()

	dk, err := cached.GenerateDataKey(ctx, &GenerateDataKeyRequest{KeyID: "k1", KeySpec: KeySpecAES256})
	if err != nil {
		t.Fatalf("GenerateDataKey: %v", err)
	}
	req := &DecryptRequest{CiphertextBlob: dk.CiphertextBlob}

	if _, err := cached.Decrypt(ctx, req); err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	time.Sleep(40 * time.Millisecond)
	if _, err := cached.Decrypt(ctx, req); err != nil {
		t.Fatalf("Decrypt after expiry: %v", err)
	}
	if got := atomic.LoadInt64(&inner.decryptCalls); got != 2 {
		t.Fatalf("expected 2 underlying Decrypt calls across TTL expiry, got %d", got)
	}
}

// TestCachedProviderMaxEntries verifies the cache is size-bounded.
func TestCachedProviderMaxEntries(t *testing.T) {
	inner := newCountingKMSProvider()
	cached := NewCachedKMSProvider(inner, time.Hour, 2)
	ctx := context.Background()

	var blobs [][]byte
	for i := 0; i < 3; i++ {
		dk, err := cached.GenerateDataKey(ctx, &GenerateDataKeyRequest{KeyID: "k1", KeySpec: KeySpecAES256})
		if err != nil {
			t.Fatalf("GenerateDataKey: %v", err)
		}
		blobs = append(blobs, dk.CiphertextBlob)
		if _, err := cached.Decrypt(ctx, &DecryptRequest{CiphertextBlob: dk.CiphertextBlob}); err != nil {
			t.Fatalf("Decrypt: %v", err)
		}
	}

	cached.mu.Lock()
	size := len(cached.entries)
	cached.mu.Unlock()
	if size > 2 {
		t.Fatalf("cache exceeded max entries: got %d, want <= 2", size)
	}
}

// TestCachedProviderClearsOverwrittenPlaintext verifies that when an entry is
// replaced (two readers racing on the same miss), the superseded key material
// is zeroed rather than left lingering in memory.
func TestCachedProviderClearsOverwrittenPlaintext(t *testing.T) {
	inner := newCountingKMSProvider()
	cached := NewCachedKMSProvider(inner, time.Hour, 10)

	cached.set("dupkey", &DecryptResponse{KeyID: "k", Plaintext: []byte("0123456789abcdef0123456789abcdef")})

	cached.mu.Lock()
	oldBuf := cached.entries["dupkey"].plaintext
	cached.mu.Unlock()

	cached.set("dupkey", &DecryptResponse{KeyID: "k", Plaintext: []byte("ffffffffffffffffffffffffffffffff")})

	if !bytes.Equal(oldBuf, make([]byte, len(oldBuf))) {
		t.Fatalf("superseded cache plaintext was not zeroed: %v", oldBuf)
	}
}

// TestCachedProviderNoWriteAfterClose verifies a store that lands after Close
// does not repopulate the scrubbed cache with key material.
func TestCachedProviderNoWriteAfterClose(t *testing.T) {
	inner := newCountingKMSProvider()
	cached := NewCachedKMSProvider(inner, time.Hour, 10)

	if err := cached.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	cached.set("k", &DecryptResponse{KeyID: "k", Plaintext: []byte("0123456789abcdef0123456789abcdef")})

	cached.mu.Lock()
	n := len(cached.entries)
	cached.mu.Unlock()
	if n != 0 {
		t.Fatalf("cache repopulated after Close: %d entries", n)
	}
}

// TestAddKMSProviderWiresCache is the wiring regression test: a provider
// configured with cache_enabled must actually be handed back wrapped in a
// caching provider (the bug was that the setting was parsed but never applied).
func TestAddKMSProviderWiresCache(t *testing.T) {
	RegisterProvider("cache-wiring-test", func(config util.Configuration) (KMSProvider, error) {
		return newCountingKMSProvider(), nil
	})

	mgr := &KMSManager{
		providers: make(map[string]KMSProvider),
		configs:   make(map[string]*KMSConfig),
		bucketKMS: make(map[string]string),
	}

	if err := mgr.AddKMSProvider("p-cached", &KMSConfig{
		Provider:     "cache-wiring-test",
		CacheEnabled: true,
		CacheTTL:     time.Hour,
		MaxCacheSize: 10,
	}); err != nil {
		t.Fatalf("AddKMSProvider (cached): %v", err)
	}
	p, err := mgr.GetKMSProviderByName("p-cached")
	if err != nil {
		t.Fatalf("GetKMSProviderByName: %v", err)
	}
	if _, ok := p.(*CachedKMSProvider); !ok {
		t.Fatalf("expected *CachedKMSProvider when cache enabled, got %T", p)
	}

	if err := mgr.AddKMSProvider("p-plain", &KMSConfig{
		Provider:     "cache-wiring-test",
		CacheEnabled: false,
	}); err != nil {
		t.Fatalf("AddKMSProvider (plain): %v", err)
	}
	p2, err := mgr.GetKMSProviderByName("p-plain")
	if err != nil {
		t.Fatalf("GetKMSProviderByName: %v", err)
	}
	if _, ok := p2.(*CachedKMSProvider); ok {
		t.Fatalf("expected unwrapped provider when cache disabled, got %T", p2)
	}
}
