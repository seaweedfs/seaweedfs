package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	icebergmanifest "github.com/apache/iceberg-go"
	"github.com/linkedin/goavro/v2"
)

// The testdata fixtures were written by ClickHouse 25.8's experimental
// Iceberg insert against a SeaweedFS table bucket. clickhouse_rel holds the
// default output (bucket-relative paths, no field-id annotations);
// clickhouse_abs was written with write_full_path_in_iceberg_metadata=1
// (absolute paths, still no field-ids).

type fakeManifestStore struct {
	files map[string][]byte
	saved map[string][]byte
}

func newFakeManifestStore() *fakeManifestStore {
	return &fakeManifestStore{files: map[string][]byte{}, saved: map[string][]byte{}}
}

func (f *fakeManifestStore) loadFile(_ context.Context, location string) ([]byte, error) {
	if data, ok := f.files[location]; ok {
		return data, nil
	}
	return nil, fmt.Errorf("no file at %s", location)
}

func (f *fakeManifestStore) saveFile(_ context.Context, location string, data []byte) error {
	f.files[location] = data
	f.saved[location] = data
	return nil
}

func loadFixture(t *testing.T, parts ...string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(append([]string{"testdata"}, parts...)...))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return data
}

const (
	relTableLocation = "s3://iceberg-tables/wprobe/target_rel"
	relListName      = "snap-449670589-2-48b874af-90f6-4e5e-8002-df91e59c8a0c.avro"
	relManifestName  = "690958a2-9936-4a04-8117-d8104c977195.avro"
	absTableLocation = "s3://iceberg-tables/wprobe/target"
	absListName      = "snap-331611856-2-51798e6c-427b-4fc2-9149-99e75d96e045.avro"
	absManifestName  = "b317f0e5-10f3-41d6-86bc-8b42f01f3e86.avro"
)

// verifyRepairedManifests checks the repaired files with iceberg-go's strict
// typed readers: the manifest list parses, references the repaired manifest
// with the right length, and the manifest's data file path is absolute.
func verifyRepairedManifests(t *testing.T, store *fakeManifestStore, listLocation, tableLocation string) {
	t.Helper()

	listBytes, ok := store.files[listLocation]
	if !ok {
		t.Fatalf("repaired manifest list %s was not saved", listLocation)
	}
	if !bytes.Contains(listBytes, []byte(`"field-id"`)) {
		t.Fatalf("repaired manifest list schema has no field-id annotations")
	}

	manifests, err := icebergmanifest.ReadManifestList(bytes.NewReader(listBytes))
	if err != nil {
		t.Fatalf("iceberg-go cannot read repaired manifest list: %v", err)
	}
	if len(manifests) != 1 {
		t.Fatalf("manifest list has %d entries, want 1", len(manifests))
	}
	manifestPath := manifests[0].FilePath()
	if !strings.HasPrefix(manifestPath, tableLocation+"/metadata/") {
		t.Fatalf("manifest path %s is not absolute under the table location", manifestPath)
	}

	manifestBytes, ok := store.files[manifestPath]
	if !ok {
		t.Fatalf("manifest %s not present in store", manifestPath)
	}
	if manifests[0].Length() != int64(len(manifestBytes)) {
		t.Fatalf("manifest_length %d != stored size %d", manifests[0].Length(), len(manifestBytes))
	}
	if !bytes.Contains(manifestBytes, []byte(`"field-id"`)) {
		t.Fatalf("repaired manifest schema has no field-id annotations")
	}

	entries, err := icebergmanifest.ReadManifest(manifests[0], bytes.NewReader(manifestBytes), false)
	if err != nil {
		t.Fatalf("iceberg-go cannot read repaired manifest: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("manifest has %d entries, want 1", len(entries))
	}
	dataFile := entries[0].DataFile()
	if !strings.HasPrefix(dataFile.FilePath(), "s3://iceberg-tables/") {
		t.Fatalf("data file path %s is not absolute", dataFile.FilePath())
	}
	if dataFile.Count() != 2 {
		t.Fatalf("record count %d, want 2", dataFile.Count())
	}
	if len(dataFile.LowerBoundValues()) == 0 {
		t.Fatalf("lower bounds were lost in repair")
	}
}

func addSnapshotUpdateJSON(t *testing.T, manifestList string) []json.RawMessage {
	t.Helper()
	update := fmt.Sprintf(`{"action":"add-snapshot","snapshot":{"snapshot-id":449670589,"sequence-number":1,"timestamp-ms":1754625869000,"manifest-list":%q,"summary":{"operation":"append"},"schema-id":0}}`, manifestList)
	return []json.RawMessage{json.RawMessage(update)}
}

func TestRepairClickHouseRelativeManifests(t *testing.T) {
	store := newFakeManifestStore()
	store.files[relTableLocation+"/metadata/"+relListName] = loadFixture(t, "clickhouse_rel", relListName)
	store.files[relTableLocation+"/metadata/"+relManifestName] = loadFixture(t, "clickhouse_rel", relManifestName)

	rawUpdates := addSnapshotUpdateJSON(t, "wprobe/target_rel/metadata/"+relListName)
	repaired, changed := repairAddSnapshotUpdates(context.Background(), store, relTableLocation, rawUpdates)
	if !changed {
		t.Fatal("expected repair to change the update")
	}

	var update struct {
		Snapshot struct {
			ManifestList string `json:"manifest-list"`
			SnapshotID   int64  `json:"snapshot-id"`
			Summary      struct {
				Operation string `json:"operation"`
			} `json:"summary"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal(repaired[0], &update); err != nil {
		t.Fatalf("unmarshal repaired update: %v", err)
	}
	wantList := relTableLocation + "/metadata/" + repairedManifestPrefix + relListName
	if update.Snapshot.ManifestList != wantList {
		t.Fatalf("manifest-list = %s, want %s", update.Snapshot.ManifestList, wantList)
	}
	// The rest of the snapshot survives the rewrite.
	if update.Snapshot.SnapshotID != 449670589 || update.Snapshot.Summary.Operation != "append" {
		t.Fatalf("snapshot fields were disturbed: %s", repaired[0])
	}

	verifyRepairedManifests(t, store, wantList, relTableLocation)
}

func TestRepairClickHouseAbsoluteManifests(t *testing.T) {
	store := newFakeManifestStore()
	listLocation := absTableLocation + "/metadata/" + absListName
	store.files[listLocation] = loadFixture(t, "clickhouse_abs", absListName)
	store.files[absTableLocation+"/metadata/"+absManifestName] = loadFixture(t, "clickhouse_abs", absManifestName)

	repaired, changed, err := repairManifestList(context.Background(), store, absTableLocation, listLocation)
	if err != nil {
		t.Fatalf("repairManifestList: %v", err)
	}
	if !changed {
		t.Fatal("expected repair for missing field-ids")
	}
	verifyRepairedManifests(t, store, repaired, absTableLocation)
}

func TestRepairedManifestsAreLeftAlone(t *testing.T) {
	store := newFakeManifestStore()
	listLocation := absTableLocation + "/metadata/" + absListName
	store.files[listLocation] = loadFixture(t, "clickhouse_abs", absListName)
	store.files[absTableLocation+"/metadata/"+absManifestName] = loadFixture(t, "clickhouse_abs", absManifestName)

	firstPass, changed, err := repairManifestList(context.Background(), store, absTableLocation, listLocation)
	if err != nil || !changed {
		t.Fatalf("first pass: changed=%v err=%v", changed, err)
	}

	store.saved = map[string][]byte{}
	secondPass, changed, err := repairManifestList(context.Background(), store, absTableLocation, firstPass)
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if changed || secondPass != firstPass {
		t.Fatalf("second pass rewrote an already-repaired list: changed=%v location=%s", changed, secondPass)
	}
	if len(store.saved) != 0 {
		t.Fatalf("second pass saved files: %v", store.saved)
	}
}

func TestRepairKeepsDeleteManifestContent(t *testing.T) {
	// The fixture lacks the OCF "content" key; the repaired copy must take the
	// content declared by the manifest-list entry, not default to data.
	raw := loadFixture(t, "clickhouse_rel", relManifestName)
	repaired, changed, err := repairManifest(raw, relTableLocation, 1)
	if err != nil || !changed {
		t.Fatalf("repairManifest: changed=%v err=%v", changed, err)
	}
	reader, err := goavro.NewOCFReader(bytes.NewReader(repaired))
	if err != nil {
		t.Fatalf("read repaired manifest: %v", err)
	}
	if got := string(reader.MetaData()["content"]); got != "deletes" {
		t.Fatalf(`OCF content = %q, want "deletes"`, got)
	}

	repaired, _, err = repairManifest(raw, relTableLocation, 0)
	if err != nil {
		t.Fatalf("repairManifest: %v", err)
	}
	reader, err = goavro.NewOCFReader(bytes.NewReader(repaired))
	if err != nil {
		t.Fatalf("read repaired manifest: %v", err)
	}
	if got := string(reader.MetaData()["content"]); got != "data" {
		t.Fatalf(`OCF content = %q, want "data"`, got)
	}
}

func TestRepairSkipsLocationsOutsideMetadataDir(t *testing.T) {
	store := newFakeManifestStore()
	rawUpdates := addSnapshotUpdateJSON(t, "s3://other-bucket/elsewhere/list.avro")
	_, changed := repairAddSnapshotUpdates(context.Background(), store, relTableLocation, rawUpdates)
	if changed {
		t.Fatal("repair touched a manifest list outside the table metadata directory")
	}
}

func TestRepairLeavesUnreadableListAlone(t *testing.T) {
	store := newFakeManifestStore()
	store.files[relTableLocation+"/metadata/junk.avro"] = []byte("not avro")
	rawUpdates := addSnapshotUpdateJSON(t, relTableLocation+"/metadata/junk.avro")
	repaired, changed := repairAddSnapshotUpdates(context.Background(), store, relTableLocation, rawUpdates)
	if changed {
		t.Fatal("repair claimed to change an unreadable manifest list")
	}
	if !bytes.Equal(repaired[0], rawUpdates[0]) {
		t.Fatal("update was modified despite repair failure")
	}
}
