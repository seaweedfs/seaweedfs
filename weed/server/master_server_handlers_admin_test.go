package weed_server

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// A /col/delete that does not name a collection must not be read as a request
// to delete the default (empty-named) collection.
func TestCollectionDeleteHandlerRequiresCollection(t *testing.T) {
	ms := newLeaderMaster()
	rp, err := super_block.NewReplicaPlacementFromString("000")
	require.NoError(t, err)
	dn := ms.Topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "127.0.0.1", "dn1", map[string]uint32{"": 10})
	v := storage.VolumeInfo{
		Id:               needle.VolumeId(1),
		Collection:       "",
		ReplicaPlacement: rp,
		Ttl:              needle.EMPTY_TTL,
		Version:          needle.GetCurrentVersion(),
	}
	dn.AddOrUpdateVolume(v)
	ms.Topo.RegisterVolumeLayout(v, dn)
	_, ok := ms.Topo.FindCollection("")
	require.True(t, ok, "test setup: the default collection should exist")

	requests := map[string]*http.Request{
		"GET without parameter":  httptest.NewRequest(http.MethodGet, "/col/delete", nil),
		"GET with empty value":   httptest.NewRequest(http.MethodGet, "/col/delete?collection=", nil),
		"POST without parameter": httptest.NewRequest(http.MethodPost, "/col/delete", nil),
		"POST with empty form value": func() *http.Request {
			r := httptest.NewRequest(http.MethodPost, "/col/delete",
				strings.NewReader(url.Values{"collection": {""}}.Encode()))
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			return r
		}(),
	}
	for name, r := range requests {
		t.Run(name, func(t *testing.T) {
			w := httptest.NewRecorder()
			ms.collectionDeleteHandler(w, r)
			assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			assert.Contains(t, w.Body.String(), "collection parameter is required")
			_, ok := ms.Topo.FindCollection("")
			assert.True(t, ok, "the default collection must survive")
		})
	}
}

func TestCollectionToDelete(t *testing.T) {
	cases := []struct {
		query   string
		want    string
		wantErr bool
	}{
		{query: "", wantErr: true},
		{query: "collection=", wantErr: true},
		{query: "collection=_default", want: ""},
		{query: "collection=photos", want: "photos"},
	}
	for _, c := range cases {
		got, err := collectionToDelete(httptest.NewRequest(http.MethodGet, "/col/delete?"+c.query, nil))
		if c.wantErr {
			assert.Error(t, err, c.query)
			continue
		}
		require.NoError(t, err, c.query)
		assert.Equal(t, c.want, got, c.query)
	}
}
