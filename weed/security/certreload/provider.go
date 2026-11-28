package certreload

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"google.golang.org/grpc/credentials/tls/certprovider"
)

// Options configures a reloadable PEM certificate provider.
type Options struct {
	// CertFile and KeyFile hold the identity certificate and its private
	// key. Both must be set, or neither.
	CertFile string
	KeyFile  string
	// RootFile holds the trusted CA certificates.
	RootFile string
	// RefreshDuration is how often the provider re-reads the files on its
	// own. A zero value falls back to DefaultRefreshInterval.
	RefreshDuration time.Duration
}

func (o Options) validate() error {
	if (o.CertFile == "") != (o.KeyFile == "") {
		return fmt.Errorf("certFile and keyFile must both be set")
	}
	if o.CertFile == "" && o.RootFile == "" {
		return fmt.Errorf("at least one of cert/key or rootFile must be set")
	}
	return nil
}

func (o Options) describe() string {
	if o.CertFile != "" {
		return o.CertFile + " / " + o.KeyFile
	}
	return o.RootFile
}

// Provider is a certprovider.Provider backed by PEM files on disk. It keeps
// the current key material in memory, refreshes it on a timer, and can be
// forced to re-read the files with Reload. Providers register themselves so
// ReloadAll can refresh every TLS listener and client in the process on
// SIGHUP.
type Provider struct {
	opts Options

	mu sync.RWMutex
	km *certprovider.KeyMaterial

	stop      chan struct{}
	closeOnce sync.Once
}

var (
	registryMu sync.Mutex
	registry   = make(map[*Provider]struct{})
)

// NewProvider returns a provider that reads the configured files lazily on
// the first KeyMaterial call, matching the behavior of gRPC's pemfile
// provider. The provider must be closed to stop its refresh goroutine.
func NewProvider(opts Options) (*Provider, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	if opts.RefreshDuration <= 0 {
		opts.RefreshDuration = DefaultRefreshInterval
	}
	p := &Provider{
		opts: opts,
		stop: make(chan struct{}),
	}
	registryMu.Lock()
	registry[p] = struct{}{}
	registryMu.Unlock()
	go p.run()
	return p, nil
}

// KeyMaterial returns the latest key material. The first call reads the
// files; later calls return the cached value until Reload or the refresh
// timer replaces it.
func (p *Provider) KeyMaterial(ctx context.Context) (*certprovider.KeyMaterial, error) {
	p.mu.RLock()
	km := p.km
	p.mu.RUnlock()
	if km != nil {
		return km, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.km == nil {
		km, err := p.load()
		if err != nil {
			return nil, err
		}
		p.km = km
	}
	return p.km, nil
}

// Reload re-reads the configured files and atomically replaces the key
// material. On error the previous material is kept, so a broken rotation
// does not take the listener down.
func (p *Provider) Reload() error {
	km, err := p.load()
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.km = km
	p.mu.Unlock()
	return nil
}

// Close stops the refresh goroutine and removes the provider from the
// SIGHUP reload registry.
func (p *Provider) Close() {
	p.closeOnce.Do(func() {
		close(p.stop)
		registryMu.Lock()
		delete(registry, p)
		registryMu.Unlock()
	})
}

func (p *Provider) run() {
	ticker := time.NewTicker(p.opts.RefreshDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := p.Reload(); err != nil {
				glog.Warningf("certreload: periodic refresh %s: %v", p.opts.describe(), err)
			}
		case <-p.stop:
			return
		}
	}
}

func (p *Provider) load() (*certprovider.KeyMaterial, error) {
	km := &certprovider.KeyMaterial{}
	if p.opts.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(p.opts.CertFile, p.opts.KeyFile)
		if err != nil {
			return nil, err
		}
		km.Certs = []tls.Certificate{cert}
	}
	if p.opts.RootFile != "" {
		rootPEM, err := os.ReadFile(p.opts.RootFile)
		if err != nil {
			return nil, err
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(rootPEM) {
			return nil, fmt.Errorf("no CA certificates found in %s", p.opts.RootFile)
		}
		km.Roots = roots
	}
	return km, nil
}

// ReloadAll forces every live provider to re-read its files. It is meant to
// be called from a SIGHUP handler so rotated certificates take effect
// immediately instead of at the next refresh tick.
func ReloadAll() {
	registryMu.Lock()
	providers := make([]*Provider, 0, len(registry))
	for p := range registry {
		providers = append(providers, p)
	}
	registryMu.Unlock()

	for _, p := range providers {
		if err := p.Reload(); err != nil {
			glog.Warningf("certreload: SIGHUP refresh %s: %v", p.opts.describe(), err)
		}
	}
}
