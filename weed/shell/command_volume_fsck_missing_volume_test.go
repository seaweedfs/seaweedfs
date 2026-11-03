package shell

import (
	"bytes"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
)

// A selected volume can already be absent from the master when fsck is used to
// clean stale Filer references after physical volume deletion. Only that
// explicitly selected missing ID should enter the purge queue.
func TestVolumeFsckFindMissingFilerEntriesForDeletedSelectedVolume(t *testing.T) {
	tempDir := t.TempDir()
	chunk := func(volumeID uint32, fileKey uint64) *filer_pb.FileChunk {
		return &filer_pb.FileChunk{
			Fid:  &filer_pb.FileId{VolumeId: volumeID, FileKey: fileKey, Cookie: 1},
			Size: 1,
		}
	}
	stub := &stubFilerServer{entries: map[string]*filer_pb.Entry{
		"/":            {IsDirectory: true},
		"/archive":     {IsDirectory: true},
		"/archive/old": {Chunks: []*filer_pb.FileChunk{chunk(99, 1), chunk(99, 4), chunk(100, 2), chunk(7, 5)}},
		"/live":        {Chunks: []*filer_pb.FileChunk{chunk(7, 3)}},
	}}

	verbose := false
	findMissing := true
	var output bytes.Buffer
	c := &commandVolumeFsck{
		tempFolder:               tempDir,
		verbose:                  &verbose,
		findMissingChunksInFiler: &findMissing,
		writer:                   &output,
		env:                      startStubFiler(t, stub),
		volumeIds:                map[uint32]bool{99: true},
		knownVolumeIds:           map[uint32]bool{7: true},
	}
	currentVolumes := map[string]map[uint32]VInfo{"dn1": {7: {collection: "live"}}}
	var purged []purgeItem
	if err := c.collectFilerFileIdAndPaths(currentVolumes, true, func(item purgeItem) {
		purged = append(purged, item)
	}, 0, 0); err != nil {
		t.Fatalf("collect filer references: %v", err)
	}

	if len(purged) != 1 || purged[0].path != util.FullPath("/archive/old") {
		t.Fatalf("purged paths = %v, want only /archive/old for selected missing volume 99", purged)
	}
	// The same logical file also references a live volume. The scan preserves
	// that reference while queuing the path only once for the missing chunks.
	foundSurvivingChunk := false
	if err := c.readFilerFileIdFile(7, func(_ types.NeedleId, itemPath util.FullPath, _ []byte) {
		if itemPath == util.FullPath("/archive/old") {
			foundSurvivingChunk = true
		}
	}); err != nil {
		t.Fatalf("read surviving volume references: %v", err)
	}
	if !foundSurvivingChunk {
		t.Fatal("the surviving chunk reference was not recorded for volume 7")
	}
	for _, line := range []string{
		"99,100000001 /archive/old volume not found\n",
		"99,400000001 /archive/old volume not found\n",
	} {
		if !strings.Contains(output.String(), line) {
			t.Errorf("missing-volume report %q lacks %q", output.String(), line)
		}
	}
	if strings.Contains(output.String(), "100,200000001") {
		t.Errorf("missing-volume report %q includes unselected volume 100", output.String())
	}
}

// Unknown IDs remain errors outside the reverse Filer scan, preserving typo detection.
func TestVolumeFsckSelectedVolumeIDValidation(t *testing.T) {
	known := map[uint32]bool{7: true}
	requested := map[uint32]bool{99: true, 100: true}

	if _, err := validateSelectedVolumeIDs(requested, known, false); err == nil {
		t.Fatal("normal fsck accepted volume IDs not present on the master")
	}
	missing, err := validateSelectedVolumeIDs(requested, known, true)
	if err != nil {
		t.Fatalf("reverse Filer scan rejected missing volume IDs: %v", err)
	}
	if len(missing) != 2 || missing[0] != 99 || missing[1] != 100 {
		t.Fatalf("missing IDs = %v, want sorted [99 100]", missing)
	}
}

// A missing volume has no trustworthy collection metadata; a collection
// restriction must fail closed instead of silently widening or narrowing scope.
func TestValidateMissingVolumeCollectionFilter(t *testing.T) {
	missing := []uint32{12}
	compile := func(filter string) *wildcard.CollectionMatcher {
		matcher, err := wildcard.CompileCollectionMatcher(filter)
		if err != nil {
			t.Fatalf("compile %q: %v", filter, err)
		}
		return matcher
	}
	for _, filter := range []string{"", "*", "ALL_COLLECTIONS", "EACH_COLLECTION"} {
		if err := validateMissingVolumeCollectionFilter(missing, compile(filter)); err != nil {
			t.Errorf("filter %q: %v", filter, err)
		}
	}
	if err := validateMissingVolumeCollectionFilter(missing, compile("photos")); err == nil || !strings.Contains(err.Error(), "collection is unknown") {
		t.Fatalf("expected an unknown-collection error, got %v", err)
	}
}

// Missing IDs require an extra acknowledgement only when metadata deletion
// is requested; read-only diagnosis and known-volume purges are unchanged.
func TestValidateMissingVolumePurgeConfirmation(t *testing.T) {
	missing := []uint32{12}
	for _, test := range []struct {
		name        string
		missing     []uint32
		purgeAbsent bool
		confirmed   bool
		wantError   bool
	}{
		{name: "read-only scan", missing: missing},
		{name: "purge known volumes only", purgeAbsent: true},
		{name: "confirmed missing volume purge", missing: missing, purgeAbsent: true, confirmed: true},
		{name: "unconfirmed missing volume purge", missing: missing, purgeAbsent: true, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateMissingVolumePurgeConfirmation(test.missing, test.purgeAbsent, test.confirmed)
			if (err != nil) != test.wantError {
				t.Fatalf("validateMissingVolumePurgeConfirmation() error = %v, wantError %v", err, test.wantError)
			}
			if err != nil && !strings.Contains(err.Error(), "confirmMissingVolumeDeletion") {
				t.Fatalf("error %q does not name the required acknowledgement flag", err)
			}
		})
	}
}

func TestVolumeFsckPurgesEntryWhoseManifestIsOnMissingVolume(t *testing.T) {
	manifest := &filer_pb.FileChunk{
		Fid:             &filer_pb.FileId{VolumeId: 99, FileKey: 1, Cookie: 1},
		Size:            1,
		IsChunkManifest: true,
	}
	stub := &stubFilerServer{entries: map[string]*filer_pb.Entry{
		"/":     {IsDirectory: true},
		"/big":  {Chunks: []*filer_pb.FileChunk{manifest}},
		"/live": {Chunks: []*filer_pb.FileChunk{{Fid: &filer_pb.FileId{VolumeId: 7, FileKey: 2, Cookie: 1}, Size: 1}}},
	}}

	verbose := false
	findMissing := true
	var output bytes.Buffer
	c := &commandVolumeFsck{
		tempFolder:               t.TempDir(),
		verbose:                  &verbose,
		findMissingChunksInFiler: &findMissing,
		writer:                   &output,
		env:                      startStubFiler(t, stub),
		volumeIds:                map[uint32]bool{99: true},
		knownVolumeIds:           map[uint32]bool{7: true},
	}
	var purged []purgeItem
	if err := c.collectFilerFileIdAndPaths(map[string]map[uint32]VInfo{"dn1": {7: {}}}, true, func(item purgeItem) {
		purged = append(purged, item)
	}, 0, 0); err != nil {
		t.Fatalf("collect filer references: %v", err)
	}

	if !strings.Contains(output.String(), "ResolveChunkManifest failed for /big") {
		t.Fatalf("manifest on the missing volume resolved unexpectedly: %q", output.String())
	}
	if len(purged) != 1 || purged[0].path != util.FullPath("/big") {
		t.Fatalf("purged paths = %v, want /big, whose manifest is on missing volume 99", purged)
	}
}
