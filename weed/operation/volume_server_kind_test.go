package operation

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func TestProbeVolumeServerImplementation(t *testing.T) {
	util_http.InitGlobalHttpClient()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"Implementation": "Rust"})
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")

	oldProbe := getVolumeServerImplementationProbe()
	defer func() {
		setVolumeServerImplementationProbe(oldProbe)
		volumeServerKindCache = sync.Map{}
	}()
	setVolumeServerImplementationProbe(nil)
	volumeServerKindCache = sync.Map{}

	kind, err := probeVolumeServerImplementation(host)
	if err != nil {
		t.Fatalf("probeVolumeServerImplementation(%q): %v", host, err)
	}
	if kind != volumeServerImplementationRust {
		t.Fatalf("got %q, want %q", kind, volumeServerImplementationRust)
	}
}
