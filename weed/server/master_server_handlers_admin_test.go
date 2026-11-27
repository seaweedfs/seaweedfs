package weed_server

import (
	"net/http"
	"net/http/httptest"
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
	addCollectionVolume(t, ms, 1, "")
	_, ok := ms.Topo.FindCollection("")
	require.True(t, ok, "test setup: the default collection should exist")

	requests := map[string]*http.Request{
		"DELETE without parameter": httptest.NewRequest(http.MethodDelete, "/col/delete", nil),
		"DELETE with empty value":  httptest.NewRequest(http.MethodDelete, "/col/delete?collection=", nil),
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

func TestCollectionDeleteHandlerRequiresDeleteMethod(t *testing.T) {
	ms := newLeaderMaster()
	addCollectionVolume(t, ms, 1, "photos")
	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut} {
		t.Run(method, func(t *testing.T) {
			w := httptest.NewRecorder()
			ms.collectionDeleteHandler(w, httptest.NewRequest(method, "/col/delete?collection=photos", nil))
			assert.Equal(t, http.StatusMethodNotAllowed, w.Code, w.Body.String())
			assert.Equal(t, http.MethodDelete, w.Header().Get("Allow"))
			_, ok := ms.Topo.FindCollection("photos")
			assert.True(t, ok, "the collection must survive")
		})
	}
}

func TestCollectionToDelete(t *testing.T) {
	cases := []struct {
		name        string
		collections []string
		query       string
		want        string
		wantErr     bool
	}{
		{name: "missing", collections: []string{""}, query: "", wantErr: true},
		{name: "empty", collections: []string{""}, query: "collection=", wantErr: true},
		{name: "default keyword", collections: []string{""}, query: "collection=_default", want: ""},
		{name: "named", collections: []string{"", "photos"}, query: "collection=photos", want: "photos"},
		{name: "literal keyword name", collections: []string{"_default"}, query: "collection=_default", want: "_default"},
		{name: "literal keyword name and default", collections: []string{"", "_default"}, query: "collection=_default", wantErr: true},
		{name: "legacy default keyword", collections: []string{""}, query: "collection=_default_", want: ""},
		{name: "legacy default keyword next to literal keyword name", collections: []string{"", "_default"}, query: "collection=_default_", want: ""},
		{name: "literal legacy keyword name", collections: []string{"_default_"}, query: "collection=_default_", want: "_default_"},
		{name: "literal legacy keyword name and default", collections: []string{"", "_default_"}, query: "collection=_default_", wantErr: true},
		{name: "default keyword next to literal legacy keyword name", collections: []string{"", "_default_"}, query: "collection=_default", want: ""},
		{name: "both literal keyword names and default", collections: []string{"", "_default", "_default_"}, query: "collection=_default", wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ms := newLeaderMaster()
			for i, collection := range c.collections {
				addCollectionVolume(t, ms, needle.VolumeId(i+1), collection)
			}
			got, err := ms.collectionToDelete(httptest.NewRequest(http.MethodGet, "/col/delete?"+c.query, nil))
			if c.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, c.want, got)
		})
	}
}

func addCollectionVolume(t *testing.T, ms *MasterServer, id needle.VolumeId, collection string) {
	t.Helper()
	rp, err := super_block.NewReplicaPlacementFromString("000")
	require.NoError(t, err)
	dn := ms.Topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "127.0.0.1", "dn1", map[string]uint32{"": 10})
	v := storage.VolumeInfo{
		Id:               id,
		Collection:       collection,
		ReplicaPlacement: rp,
		Ttl:              needle.EMPTY_TTL,
		Version:          needle.GetCurrentVersion(),
	}
	dn.AddOrUpdateVolume(v)
	ms.Topo.RegisterVolumeLayout(v, dn)
}
