package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/linkedin/goavro/v2"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// Some engines (ClickHouse's experimental Iceberg writes among them) commit
// snapshots whose manifest avro files omit the spec's field-id annotations and
// reference files by bucket-relative path. Both make the snapshot unreadable
// for other engines even though the data itself is fine. Since the catalog is
// the commit chokepoint, add-snapshot updates are repaired here: the manifest
// list and manifests are transcoded with the annotations added and the paths
// absolutized, and the snapshot is pointed at the repaired copies.

// avroFieldIDs describes the spec-mandated ids for one avro field and the
// fields nested beneath it (through unions, arrays, and records).
type avroFieldIDs struct {
	id        int
	elementID int
	children  map[string]avroFieldIDs
}

// manifestListFieldIDs returns the fixed v2 manifest_file schema ids.
func manifestListFieldIDs() map[string]avroFieldIDs {
	return map[string]avroFieldIDs{
		"manifest_path":        {id: 500},
		"manifest_length":      {id: 501},
		"partition_spec_id":    {id: 502},
		"content":              {id: 517},
		"sequence_number":      {id: 515},
		"min_sequence_number":  {id: 516},
		"added_snapshot_id":    {id: 503},
		"added_files_count":    {id: 504},
		"existing_files_count": {id: 505},
		"deleted_files_count":  {id: 506},
		"added_rows_count":     {id: 512},
		"existing_rows_count":  {id: 513},
		"deleted_rows_count":   {id: 514},
		"partitions": {id: 507, elementID: 508, children: map[string]avroFieldIDs{
			"contains_null": {id: 509},
			"contains_nan":  {id: 518},
			"lower_bound":   {id: 510},
			"upper_bound":   {id: 511},
		}},
		"key_metadata": {id: 519},
		"first_row_id": {id: 520},
	}
}

// manifestEntryFieldIDs returns the fixed v2 manifest_entry schema ids. The
// partition struct is table-specific; its ids come from the partition-spec
// JSON embedded in the manifest's OCF metadata.
func manifestEntryFieldIDs(partitionIDs map[string]avroFieldIDs) map[string]avroFieldIDs {
	return map[string]avroFieldIDs{
		"status":               {id: 0},
		"snapshot_id":          {id: 1},
		"sequence_number":      {id: 3},
		"file_sequence_number": {id: 4},
		"data_file": {id: 2, children: map[string]avroFieldIDs{
			"content":               {id: 134},
			"file_path":             {id: 100},
			"file_format":           {id: 101},
			"partition":             {id: 102, children: partitionIDs},
			"record_count":          {id: 103},
			"file_size_in_bytes":    {id: 104},
			"column_sizes":          {id: 108, children: kvFieldIDs(117, 118)},
			"value_counts":          {id: 109, children: kvFieldIDs(119, 120)},
			"null_value_counts":     {id: 110, children: kvFieldIDs(121, 122)},
			"nan_value_counts":      {id: 137, children: kvFieldIDs(138, 139)},
			"lower_bounds":          {id: 125, children: kvFieldIDs(126, 127)},
			"upper_bounds":          {id: 128, children: kvFieldIDs(129, 130)},
			"key_metadata":          {id: 131},
			"split_offsets":         {id: 132, elementID: 133},
			"equality_ids":          {id: 135, elementID: 136},
			"sort_order_id":         {id: 140},
			"first_row_id":          {id: 142},
			"referenced_data_file":  {id: 143},
			"content_offset":        {id: 144},
			"content_size_in_bytes": {id: 145},
		}},
	}
}

func kvFieldIDs(keyID, valueID int) map[string]avroFieldIDs {
	return map[string]avroFieldIDs{
		"key":   {id: keyID},
		"value": {id: valueID},
	}
}

// partitionFieldIDsFromSpec extracts name -> field-id from the partition-spec
// JSON carried in a manifest's OCF metadata.
func partitionFieldIDsFromSpec(specJSON []byte) (map[string]avroFieldIDs, error) {
	if len(specJSON) == 0 {
		return map[string]avroFieldIDs{}, nil
	}
	var fields []struct {
		Name    string `json:"name"`
		FieldID int    `json:"field-id"`
	}
	if err := json.Unmarshal(specJSON, &fields); err != nil {
		return nil, fmt.Errorf("parse partition-spec metadata: %w", err)
	}
	ids := make(map[string]avroFieldIDs, len(fields))
	for _, f := range fields {
		ids[f.Name] = avroFieldIDs{id: f.FieldID}
	}
	return ids, nil
}

// annotateAvroRecordFields adds "field-id" to every field of a parsed avro
// record schema, recursing into nested types. Returns whether anything was
// added, and ok=false when a field has no known id (the schema is left for an
// unknown writer dialect rather than half-annotated).
func annotateAvroRecordFields(record map[string]any, ids map[string]avroFieldIDs) (changed, ok bool) {
	fields, _ := record["fields"].([]any)
	if len(fields) == 0 {
		return false, true
	}
	for _, f := range fields {
		field, isMap := f.(map[string]any)
		if !isMap {
			return changed, false
		}
		name, _ := field["name"].(string)
		spec, known := ids[name]
		if !known {
			return changed, false
		}
		if _, has := field["field-id"]; !has {
			field["field-id"] = spec.id
			changed = true
		}
		typeChanged, typeOK := annotateAvroType(field["type"], spec)
		changed = changed || typeChanged
		if !typeOK {
			return changed, false
		}
	}
	return changed, true
}

// annotateAvroType annotates the schema node of a single field: unions are
// followed through their non-null branches, arrays get "element-id", and
// nested records recurse with the field's child ids.
func annotateAvroType(node any, spec avroFieldIDs) (changed, ok bool) {
	switch t := node.(type) {
	case []any: // union
		for _, branch := range t {
			branchChanged, branchOK := annotateAvroType(branch, spec)
			changed = changed || branchChanged
			if !branchOK {
				return changed, false
			}
		}
		return changed, true
	case map[string]any:
		switch t["type"] {
		case "record":
			childIDs := spec.children
			if childIDs == nil {
				childIDs = map[string]avroFieldIDs{}
			}
			return annotateAvroRecordFields(t, childIDs)
		case "array":
			if spec.elementID != 0 {
				if _, has := t["element-id"]; !has {
					t["element-id"] = spec.elementID
					changed = true
				}
			}
			itemChanged, itemOK := annotateAvroType(t["items"], spec)
			return changed || itemChanged, itemOK
		case "map":
			// The spec wants key-id/value-id on genuine avro maps; this repair
			// only knows the array-of-key_value encoding, so leave the file to
			// its writer rather than half-annotating it.
			return changed, false
		default:
			return false, true
		}
	default: // primitive name or named-type reference
		return false, true
	}
}

// transcodeManifestAvro decodes an avro OCF file, annotates its schema with
// the given ids, applies mutate to each datum and fixMeta to the OCF metadata,
// and re-encodes. When nothing changes, the original bytes are returned
// unchanged.
func transcodeManifestAvro(raw []byte, ids map[string]avroFieldIDs, mutate func(map[string]any) bool, fixMeta func(map[string][]byte) bool) ([]byte, bool, error) {
	reader, err := goavro.NewOCFReader(bytes.NewReader(raw))
	if err != nil {
		return nil, false, fmt.Errorf("open avro: %w", err)
	}
	meta := reader.MetaData()

	outMeta := make(map[string][]byte, len(meta))
	for k, v := range meta {
		if k == "avro.schema" || k == "avro.codec" {
			continue
		}
		outMeta[k] = v
	}
	metaChanged := false
	if fixMeta != nil {
		metaChanged = fixMeta(outMeta)
	}

	var schema map[string]any
	if err := json.Unmarshal(meta["avro.schema"], &schema); err != nil {
		return nil, false, fmt.Errorf("parse avro schema: %w", err)
	}
	schemaChanged, ok := annotateAvroRecordFields(schema, ids)
	if !ok {
		return nil, false, fmt.Errorf("avro schema has fields outside the Iceberg manifest spec")
	}

	datumChanged := false
	var datums []any
	for reader.Scan() {
		datum, err := reader.Read()
		if err != nil {
			return nil, false, fmt.Errorf("read avro datum: %w", err)
		}
		if record, isMap := datum.(map[string]any); isMap && mutate != nil {
			if mutate(record) {
				datumChanged = true
			}
		}
		datums = append(datums, datum)
	}
	if err := reader.Err(); err != nil {
		return nil, false, fmt.Errorf("scan avro: %w", err)
	}

	if !schemaChanged && !datumChanged && !metaChanged {
		return raw, false, nil
	}

	annotatedSchema, err := json.Marshal(schema)
	if err != nil {
		return nil, false, err
	}
	var buf bytes.Buffer
	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:        &buf,
		Schema:   string(annotatedSchema),
		MetaData: outMeta,
	})
	if err != nil {
		return nil, false, fmt.Errorf("create avro writer: %w", err)
	}
	if err := writer.Append(datums); err != nil {
		return nil, false, fmt.Errorf("write avro datums: %w", err)
	}
	return buf.Bytes(), true, nil
}

// manifestStore abstracts reading and writing files in a table's metadata
// directory so the repair logic is testable without a filer.
type manifestStore interface {
	loadFile(ctx context.Context, location string) ([]byte, error)
	saveFile(ctx context.Context, location string, data []byte) error
}

// absolutizeLocation resolves a bucket-relative path against the bucket of the
// table location. Locations that already carry a scheme pass through.
func absolutizeLocation(location, tableLocation string) string {
	if location == "" || strings.Contains(location, "://") {
		return location
	}
	bucket, _, err := parseS3Location(tableLocation)
	if err != nil {
		return location
	}
	return "s3://" + bucket + "/" + strings.TrimPrefix(location, "/")
}

// metadataFileName returns the file name when location is a direct child of
// the table's metadata directory, which is the only place repair will read
// from or write to.
func metadataFileName(tableLocation, location string) (string, bool) {
	prefix := strings.TrimSuffix(tableLocation, "/") + "/metadata/"
	name := strings.TrimPrefix(location, prefix)
	if name == location || name == "" || strings.Contains(name, "/") {
		return "", false
	}
	return name, true
}

const repairedManifestPrefix = "repaired-"

// maxRepairableManifestSize bounds how much manifest data the repair path
// buffers in memory; anything larger fails the repair and the commit
// proceeds with the writer's original files.
const maxRepairableManifestSize = 64 << 20

// hasFieldIDAnnotations is the cheap compliance probe: spec-compliant writers
// annotate every manifest avro schema with field-id, and a writer that
// annotates the manifest list annotates its manifests too.
func hasFieldIDAnnotations(raw []byte) bool {
	reader, err := goavro.NewOCFReader(bytes.NewReader(raw))
	if err != nil {
		return false
	}
	return bytes.Contains(reader.MetaData()["avro.schema"], []byte(`"field-id"`))
}

// repairManifestList loads a snapshot's manifest list and, when it needs
// repair, rewrites it (and the manifests it references) into spec-compliant
// copies next to the originals. Returns the manifest list location the
// snapshot should reference.
func repairManifestList(ctx context.Context, store manifestStore, tableLocation, listLocation string) (string, bool, error) {
	absListLocation := absolutizeLocation(listLocation, tableLocation)
	listName, inMetadataDir := metadataFileName(tableLocation, absListLocation)
	if !inMetadataDir {
		return listLocation, false, nil
	}

	listBytes, err := store.loadFile(ctx, absListLocation)
	if err != nil {
		return listLocation, false, fmt.Errorf("load manifest list: %w", err)
	}
	if absListLocation == listLocation && hasFieldIDAnnotations(listBytes) {
		return listLocation, false, nil
	}

	metadataDir := strings.TrimSuffix(tableLocation, "/") + "/metadata/"
	var manifestErr error
	repairedList, listChanged, err := transcodeManifestAvro(listBytes, manifestListFieldIDs(), func(record map[string]any) bool {
		manifestPath, _ := record["manifest_path"].(string)
		if manifestPath == "" || manifestErr != nil {
			return false
		}
		absManifest := absolutizeLocation(manifestPath, tableLocation)
		recordChanged := absManifest != manifestPath
		if recordChanged {
			record["manifest_path"] = absManifest
		}

		manifestName, ok := metadataFileName(tableLocation, absManifest)
		if !ok {
			return recordChanged
		}
		manifestBytes, err := store.loadFile(ctx, absManifest)
		if err != nil {
			manifestErr = fmt.Errorf("load manifest %s: %w", absManifest, err)
			return recordChanged
		}
		repaired, changed, err := repairManifest(manifestBytes, tableLocation, manifestContentValue(record))
		if err != nil {
			manifestErr = fmt.Errorf("repair manifest %s: %w", absManifest, err)
			return recordChanged
		}
		if !changed {
			return recordChanged
		}
		repairedLocation := metadataDir + repairedManifestPrefix + manifestName
		if err := store.saveFile(ctx, repairedLocation, repaired); err != nil {
			manifestErr = fmt.Errorf("save repaired manifest %s: %w", repairedLocation, err)
			return recordChanged
		}
		record["manifest_path"] = repairedLocation
		record["manifest_length"] = int64(len(repaired))
		return true
	}, nil)
	if err != nil {
		return listLocation, false, err
	}
	if manifestErr != nil {
		return listLocation, false, manifestErr
	}
	if !listChanged && absListLocation == listLocation {
		return listLocation, false, nil
	}

	repairedListLocation := metadataDir + repairedManifestPrefix + listName
	if listChanged {
		if err := store.saveFile(ctx, repairedListLocation, repairedList); err != nil {
			return listLocation, false, fmt.Errorf("save repaired manifest list: %w", err)
		}
		return repairedListLocation, true, nil
	}
	// Only the pointer was relative; the file itself is fine.
	return absListLocation, true, nil
}

// manifestContentValue reads the manifest-list entry's content field
// (0 = data, 1 = deletes); v1 lists have no such field and default to data.
// Union-typed values arrive from goavro as a single-branch map.
func manifestContentValue(record map[string]any) int {
	value := record["content"]
	if union, isUnion := value.(map[string]any); isUnion {
		for _, branch := range union {
			value = branch
			break
		}
	}
	switch v := value.(type) {
	case int32:
		return int(v)
	case int64:
		return int(v)
	case int:
		return v
	}
	return 0
}

// fixManifestOCFMetadata fills in OCF metadata keys the spec requires of
// manifests but lax writers omit: "content" (readers reject v2 manifests
// without it; the value must agree with the manifest-list entry) and
// "partition-spec-id" (ClickHouse writes the underscore variant).
func fixManifestOCFMetadata(listContent int) func(map[string][]byte) bool {
	return func(meta map[string][]byte) bool {
		changed := false
		if _, ok := meta["content"]; !ok {
			content := "data"
			if listContent == 1 {
				content = "deletes"
			}
			meta["content"] = []byte(content)
			changed = true
		}
		if _, ok := meta["partition-spec-id"]; !ok {
			if v, ok := meta["partition_spec_id"]; ok {
				meta["partition-spec-id"] = v
				changed = true
			}
		}
		return changed
	}
}

// repairManifest annotates a single manifest's schema and absolutizes the
// data file paths inside it. listContent is the content declared by the
// manifest-list entry referencing this manifest.
func repairManifest(raw []byte, tableLocation string, listContent int) ([]byte, bool, error) {
	reader, err := goavro.NewOCFReader(bytes.NewReader(raw))
	if err != nil {
		return nil, false, fmt.Errorf("open manifest avro: %w", err)
	}
	partitionIDs, err := partitionFieldIDsFromSpec(reader.MetaData()["partition-spec"])
	if err != nil {
		return nil, false, err
	}
	return transcodeManifestAvro(raw, manifestEntryFieldIDs(partitionIDs), func(record map[string]any) bool {
		dataFile, _ := record["data_file"].(map[string]any)
		if dataFile == nil {
			return false
		}
		filePath, _ := dataFile["file_path"].(string)
		absPath := absolutizeLocation(filePath, tableLocation)
		if absPath == filePath {
			return false
		}
		dataFile["file_path"] = absPath
		return true
	}, fixManifestOCFMetadata(listContent))
}

// repairAddSnapshotUpdates rewrites the manifest-list references of
// add-snapshot updates whose manifests need repair. Repair is best effort: on
// any error the original update is kept and the commit proceeds as it would
// have without repair.
func repairAddSnapshotUpdates(ctx context.Context, store manifestStore, tableLocation string, rawUpdates []json.RawMessage) ([]json.RawMessage, bool) {
	changedAny := false
	out := make([]json.RawMessage, len(rawUpdates))
	copy(out, rawUpdates)
	for i, rawUpdate := range rawUpdates {
		var probe struct {
			Action string `json:"action"`
		}
		if err := json.Unmarshal(rawUpdate, &probe); err != nil || probe.Action != "add-snapshot" {
			continue
		}
		var update map[string]json.RawMessage
		if err := json.Unmarshal(rawUpdate, &update); err != nil {
			continue
		}
		var snapshot map[string]json.RawMessage
		if err := json.Unmarshal(update["snapshot"], &snapshot); err != nil {
			continue
		}
		var listLocation string
		if err := json.Unmarshal(snapshot["manifest-list"], &listLocation); err != nil || listLocation == "" {
			continue
		}

		repairedLocation, changed, err := repairManifestList(ctx, store, tableLocation, listLocation)
		if err != nil {
			glog.Warningf("Iceberg: manifest repair for %s skipped: %v", listLocation, err)
			continue
		}
		if !changed {
			continue
		}

		locationJSON, err := json.Marshal(repairedLocation)
		if err != nil {
			continue
		}
		snapshot["manifest-list"] = locationJSON
		snapshotJSON, err := json.Marshal(snapshot)
		if err != nil {
			continue
		}
		update["snapshot"] = snapshotJSON
		updateJSON, err := json.Marshal(update)
		if err != nil {
			continue
		}
		out[i] = updateJSON
		changedAny = true
		glog.V(1).Infof("Iceberg: repaired manifest list %s -> %s", listLocation, repairedLocation)
	}
	return out, changedAny
}

// serverManifestStore reads and writes table metadata files through the filer.
type serverManifestStore struct {
	server        *Server
	tableLocation string
}

func (st *serverManifestStore) split(location string) (bucket, tablePath, fileName string, err error) {
	fileName, ok := metadataFileName(st.tableLocation, location)
	if !ok {
		return "", "", "", fmt.Errorf("%s is outside the table metadata directory", location)
	}
	bucket, tablePath, err = parseS3Location(st.tableLocation)
	return bucket, tablePath, fileName, err
}

func (st *serverManifestStore) loadFile(ctx context.Context, location string) ([]byte, error) {
	bucket, tablePath, fileName, err := st.split(location)
	if err != nil {
		return nil, err
	}
	return st.server.loadTableMetadataBlob(ctx, bucket, tablePath, fileName)
}

func (st *serverManifestStore) saveFile(ctx context.Context, location string, data []byte) error {
	bucket, tablePath, fileName, err := st.split(location)
	if err != nil {
		return err
	}
	return st.server.saveMetadataBlob(ctx, bucket, tablePath, fileName, data, "application/avro")
}

// repairAddSnapshotManifests is the server entry point used by the commit
// handlers.
func (s *Server) repairAddSnapshotManifests(ctx context.Context, tableLocation string, rawUpdates []json.RawMessage) ([]json.RawMessage, bool) {
	store := &serverManifestStore{server: s, tableLocation: tableLocation}
	return repairAddSnapshotUpdates(ctx, store, tableLocation, rawUpdates)
}

// lookupFileIDAdapter lets filer.StreamContent resolve volume locations
// through the iceberg server's filer client.
type lookupFileIDAdapter struct {
	fn wdclient.LookupFileIdFunctionType
}

func (a *lookupFileIDAdapter) GetLookupFileIdFunction() wdclient.LookupFileIdFunctionType {
	return a.fn
}

// loadTableMetadataBlob reads a file from the table's metadata directory,
// following chunks for files that were written through the S3 gateway rather
// than inline by the catalog itself.
func (s *Server) loadTableMetadataBlob(ctx context.Context, bucketName, tablePath, fileName string) ([]byte, error) {
	var entry *filer_pb.Entry
	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: metadataDirPath(bucketName, tablePath),
			Name:      fileName,
		})
		if err != nil {
			return err
		}
		entry = resp.Entry
		return nil
	})
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, fmt.Errorf("no entry for %s", fileName)
	}
	if len(entry.Content) > 0 || len(entry.GetChunks()) == 0 {
		return entry.Content, nil
	}
	if size := filer.FileSize(entry); size > maxRepairableManifestSize {
		return nil, fmt.Errorf("%s is %d bytes, larger than the %d byte repair limit", fileName, size, maxRepairableManifestSize)
	}

	fullClient, ok := s.filerClient.(filer_pb.FilerClient)
	if !ok {
		return nil, fmt.Errorf("filer client cannot resolve chunk locations")
	}
	var buf bytes.Buffer
	lookup := &lookupFileIDAdapter{fn: filer.LookupFn(fullClient)}
	if err := filer.StreamContent(lookup, &buf, entry.GetChunks(), 0, int64(filer.FileSize(entry))); err != nil {
		return nil, fmt.Errorf("read %s: %w", fileName, err)
	}
	return buf.Bytes(), nil
}
