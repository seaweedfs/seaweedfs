package operation

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

const (
	volumeServerImplementationGo   = "go"
	volumeServerImplementationRust = "rust"
)

const volumeServerKindCacheTTL = 5 * time.Minute

type probeWrapper struct {
	f func(host string) string
}

type volumeServerKindCacheEntry struct {
	implementation string
	expiresAt      time.Time
}

var volumeServerImplementationProbe atomic.Value // stores *probeWrapper

var volumeServerKindCache sync.Map // host -> volumeServerKindCacheEntry

// setVolumeServerImplementationProbe overrides the HTTP /status probe (tests only).
func setVolumeServerImplementationProbe(probe func(host string) string) {
	if probe == nil {
		volumeServerImplementationProbe.Store((*probeWrapper)(nil))
		return
	}
	volumeServerImplementationProbe.Store(&probeWrapper{f: probe})
}

// getVolumeServerImplementationProbe returns the test probe override, if any.
func getVolumeServerImplementationProbe() func(host string) string {
	p := volumeServerImplementationProbe.Load()
	if p == nil {
		return nil
	}
	w, ok := p.(*probeWrapper)
	if !ok || w == nil {
		return nil
	}
	return w.f
}

// volumeServerImplementation returns the normalized Implementation value from a
// volume server's /status endpoint (go, rust, or empty when unknown).
func volumeServerImplementation(host string) string {
	if host == "" {
		return ""
	}
	now := time.Now()
	if cached, ok := volumeServerKindCache.Load(host); ok {
		entry := cached.(volumeServerKindCacheEntry)
		if now.Before(entry.expiresAt) {
			return entry.implementation
		}
	}
	kind, err := probeVolumeServerImplementation(host)
	if err != nil {
		return ""
	}
	volumeServerKindCache.Store(host, volumeServerKindCacheEntry{
		implementation: kind,
		expiresAt:      now.Add(volumeServerKindCacheTTL),
	})
	return kind
}

// probeVolumeServerImplementation fetches /status and parses the Implementation field.
func probeVolumeServerImplementation(host string) (string, error) {
	if probe := getVolumeServerImplementationProbe(); probe != nil {
		return probe(host), nil
	}
	body, _, err := util_http.Get(fmt.Sprintf("http://%s/status", host))
	if err != nil {
		glog.V(4).Infof("volume server %s status probe: %v", host, err)
		return "", err
	}
	var status struct {
		Implementation string `json:"Implementation"`
	}
	if err := json.Unmarshal(body, &status); err != nil {
		glog.V(4).Infof("volume server %s status JSON: %v", host, err)
		return "", err
	}
	return strings.ToLower(strings.TrimSpace(status.Implementation)), nil
}

// chunkUploadReplicaFanoutEnabled reports whether chunked uploads should write
// every replica holder directly (PR #10078). Fan-out is enabled only when every
// holder identifies as a Go volume server; Rust weed-volume replicates on the
// primary write and rejects type=replicate uploads from gateways.
func chunkUploadReplicaFanoutEnabled(holders []string) bool {
	if len(holders) <= 1 {
		return false
	}
	for _, host := range holders {
		switch volumeServerImplementation(host) {
		case volumeServerImplementationGo:
			continue
		default:
			return false
		}
	}
	return true
}
