package operation

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

const (
	volumeServerImplementationGo   = "go"
	volumeServerImplementationRust = "rust"
)

// volumeServerImplementationProbe, when non-nil, overrides the HTTP /status
// probe (used in tests).
var volumeServerImplementationProbe func(host string) string

var volumeServerKindCache sync.Map // host -> string (go/rust/"")

func volumeServerImplementation(host string) string {
	if host == "" {
		return ""
	}
	if cached, ok := volumeServerKindCache.Load(host); ok {
		return cached.(string)
	}
	kind := probeVolumeServerImplementation(host)
	volumeServerKindCache.Store(host, kind)
	return kind
}

func probeVolumeServerImplementation(host string) string {
	if volumeServerImplementationProbe != nil {
		return volumeServerImplementationProbe(host)
	}
	body, _, err := util_http.Get(fmt.Sprintf("http://%s/status", host))
	if err != nil {
		glog.V(4).Infof("volume server %s status probe: %v", host, err)
		return ""
	}
	var status struct {
		Implementation string `json:"Implementation"`
	}
	if err := json.Unmarshal(body, &status); err != nil {
		glog.V(4).Infof("volume server %s status JSON: %v", host, err)
		return ""
	}
	return strings.ToLower(strings.TrimSpace(status.Implementation))
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
