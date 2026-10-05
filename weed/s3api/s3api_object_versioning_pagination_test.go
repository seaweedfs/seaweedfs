package s3api

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// versionedDir builds the .versions directory entry of a versioned object whose
// latest version has the given id.
func versionedDir(object, latestVersionId string) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:        object + s3_constants.VersionsFolder,
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{Mtime: time.Now().Unix()},
		Extended: map[string][]byte{
			s3_constants.ExtLatestVersionIdKey: []byte(latestVersionId),
		},
	}
}

// versionFile builds one version entry inside a .versions directory.
func versionFile(versionId string, deleteMarker bool) *filer_pb.Entry {
	e := &filer_pb.Entry{
		Name:       "v_" + versionId,
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix()},
		Extended: map[string][]byte{
			s3_constants.ExtVersionIdKey: []byte(versionId),
		},
	}
	if deleteMarker {
		e.Extended[s3_constants.ExtDeleteMarkerKey] = []byte("true")
	}
	return e
}

func listVersionsPage(t *testing.T, s3a *S3ApiServer, client filer_pb.SeaweedFilerClient, bucketDir string, maxKeys int, keyMarker, versionIdMarker string) ([]versionListItem, string, string, bool) {
	t.Helper()
	var allVersions []interface{}
	vc := &versionCollector{
		s3a:              s3a,
		filerClient:      client,
		bucket:           "b",
		keyMarker:        keyMarker,
		versionIdMarker:  versionIdMarker,
		maxCollect:       maxKeys + 1,
		allVersions:      &allVersions,
		processedObjects: map[string]bool{},
		seenVersionIds:   map[string]bool{},
		commonPrefixes:   map[string]bool{},
	}
	require.NoError(t, vc.collectVersions(bucketDir, ""))
	combined := s3a.buildSortedCombinedList(allVersions, vc.commonPrefixes)
	return s3a.truncateAndSetMarkers(combined, maxKeys)
}

func itemId(item versionListItem) string {
	return item.key + ":" + item.versionId
}

// TestListObjectVersionsPagination covers issue 11594: "a.copy.versions" sorts
// before "a.versions" in the filer even though key "a.copy" sorts after "a", so
// cutting the walk at maxKeys+1 can put the later key on the first page and the
// marker then skips the earlier key for good. Directory "d" and file "d.x"
// exercise the mirror-image resume case: a marker on "d.x" must not skip the
// subtree of "d", whose keys all sort after it.
func TestListObjectVersionsPagination(t *testing.T) {
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/b": {
				versionedDir("a.copy", "c2"),
				versionedDir("a", "a2"),
				newDir("d"),
				{Name: "d.x", Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix()}},
			},
			"/buckets/b/a.copy.versions": {versionFile("c1", false), versionFile("c2", true)},
			"/buckets/b/a.versions":      {versionFile("a1", false), versionFile("a2", false)},
			"/buckets/b/d": {
				versionedDir("f", "f1"),
				versionedDir("g", "g1"),
			},
			"/buckets/b/d/f.versions": {versionFile("f1", false)},
			"/buckets/b/d/g.versions": {versionFile("g1", false)},
		},
	}
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}

	want, _, _, truncated := listVersionsPage(t, s3a, client, "/buckets/b", 100, "", "")
	require.False(t, truncated)
	wantIds := make([]string, 0, len(want))
	for _, item := range want {
		wantIds = append(wantIds, itemId(item))
	}
	require.Equal(t, []string{"a:a2", "a:a1", "a.copy:c2", "a.copy:c1", "d.x:null", "d/f:f1", "d/g:g1"}, wantIds)

	for maxKeys := 1; maxKeys <= len(wantIds)+1; maxKeys++ {
		var got []string
		keyMarker, versionIdMarker := "", ""
		for i := 0; i < 20; i++ {
			page, nextKey, nextVersion, trunc := listVersionsPage(t, s3a, client, "/buckets/b", maxKeys, keyMarker, versionIdMarker)
			for _, item := range page {
				got = append(got, itemId(item))
			}
			if !trunc {
				break
			}
			keyMarker, versionIdMarker = nextKey, nextVersion
		}
		assert.Equal(t, wantIds, got, "maxKeys=%d", maxKeys)
	}
}
