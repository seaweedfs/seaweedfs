package lance

import (
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// handleListTables lists the Lance tables under a namespace. The spec asks for
// full string identifiers, not bare names, so a recursive listing stays
// unambiguous.
func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	id, delimiter, ok := routeIdentifier(w, r)
	if !ok {
		return
	}
	bucket, ns := id.namespace()
	if bucket == "" {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, "listing tables needs a namespace, not the root")
		return
	}

	req := &s3tables.ListTablesRequest{
		TableBucketARN:    bucketARN(bucket),
		Namespace:         ns,
		ContinuationToken: r.URL.Query().Get("page_token"),
		MaxTables:         pageSize(r),
	}
	var resp s3tables.ListTablesResponse
	if err := s.execute(r, "ListTables", req, &resp); err != nil {
		writeStorageError(w, r, err)
		return
	}

	includeDeclared := true
	if raw := r.URL.Query().Get("include_declared"); raw != "" {
		includeDeclared = boolQuery(r, "include_declared")
	}

	writeJSON(w, http.StatusOK, ListTablesResponse{
		Tables:    s.lanceTableIDs(r, bucket, resp.Tables, delimiter, includeDeclared),
		PageToken: resp.ContinuationToken,
	})
}

// handleListAllTables lists every Lance table the caller can see, across every
// table bucket.
func (s *Server) handleListAllTables(w http.ResponseWriter, r *http.Request) {
	delimiter := requestDelimiter(r)

	var buckets s3tables.ListTableBucketsResponse
	if err := s.execute(r, "ListTableBuckets", &s3tables.ListTableBucketsRequest{MaxBuckets: pageSize(r)}, &buckets); err != nil {
		writeStorageError(w, r, err)
		return
	}

	// The spec marks `tables` required, so an empty catalog answers with an
	// empty list rather than null.
	all := []string{}
	for _, bucket := range buckets.TableBuckets {
		var tables s3tables.ListTablesResponse
		req := &s3tables.ListTablesRequest{TableBucketARN: bucketARN(bucket.Name), MaxTables: pageSize(r)}
		if err := s.execute(r, "ListTables", req, &tables); err != nil {
			// One unreadable bucket must not hide the rest; a caller with access
			// to some buckets still gets those.
			glog.V(2).Infof("lance: skipping bucket %s in ListAllTables: %v", bucket.Name, err)
			continue
		}
		all = append(all, s.lanceTableIDs(r, bucket.Name, tables.Tables, delimiter, true)...)
	}
	writeJSON(w, http.StatusOK, ListTablesResponse{Tables: all})
}

// lanceTableIDs keeps the Lance tables out of a listing that also carries
// Iceberg tables, and drops deregistered ones because they are meant to be
// invisible until re-registered.
func (s *Server) lanceTableIDs(r *http.Request, bucket string, summaries []s3tables.TableSummary, delimiter string, includeDeclared bool) []string {
	ids := make([]string, 0, len(summaries))
	for _, summary := range summaries {
		if summary.Format != s3tables.FormatLance {
			continue
		}
		location := summary.MetadataLocation
		if location == "" {
			location = tableLocation(bucket, summary.Namespace, summary.Name)
		}
		deregistered, hasData, err := s.datasetState(r, location)
		if err != nil {
			glog.V(2).Infof("lance: cannot read dataset state for %s: %v", location, err)
			continue
		}
		if deregistered {
			continue
		}
		if !hasData && !includeDeclared {
			continue
		}
		parts := append([]string{bucket}, summary.Namespace...)
		ids = append(ids, identifier(append(parts, summary.Name)).String(delimiter))
	}
	return ids
}

// handleDeclareTable records a table that does not exist on storage yet. This
// is what a Lance client calls on CREATE TABLE, before it writes any data.
func (s *Server) handleDeclareTable(w http.ResponseWriter, r *http.Request) {
	id, _, ok := routeIdentifier(w, r)
	if !ok {
		return
	}
	var req DeclareTableRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, err.Error())
		return
	}
	if len(id) == 0 && len(req.ID) > 0 {
		id = req.ID
	}
	if !checkBodyIdentifier(w, r, id, req.ID) {
		return
	}
	bucket, ns, name, err := id.table()
	if err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, err.Error())
		return
	}

	location := strings.TrimSuffix(req.Location, "/")
	if location == "" {
		location = tableLocation(bucket, ns, name)
	}

	if err := s.createTable(r, bucket, ns, name, location); err != nil {
		writeStorageError(w, r, err)
		return
	}
	if err := s.writeMarker(r, location, reservedMarker); err != nil {
		// The catalog entry is the authority; the marker only mirrors it for
		// clients that read the storage prefix directly.
		glog.V(1).Infof("lance: could not write %s for %s: %v", reservedMarker, location, err)
	}
	// Declaring a name that was deregistered brings it back, the same way
	// registering it does.
	if err := s.removeMarker(r, location, deregisteredMarker); err != nil {
		glog.V(2).Infof("lance: could not clear %s for %s: %v", deregisteredMarker, location, err)
	}

	options, err := s.storageOptions(r, bucket, location, wants(r, "vend_credentials", req.VendCredentials))
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, codeInternal, err.Error())
		return
	}
	// Properties are not persisted for a table, so neither response carries
	// them: null says "this catalog does not keep them", where echoing the
	// request back or answering {} would say they were stored and are empty.
	writeJSON(w, http.StatusOK, DeclareTableResponse{
		Location:       location,
		StorageOptions: options,
	})
}

// handleDescribeTable resolves a table to a location, and to credentials when
// the caller asks for them.
func (s *Server) handleDescribeTable(w http.ResponseWriter, r *http.Request) {
	id, _, ok := routeIdentifier(w, r)
	if !ok {
		return
	}
	var req DescribeTableRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, err.Error())
		return
	}
	if len(id) == 0 && len(req.ID) > 0 {
		id = req.ID
	}
	if !checkBodyIdentifier(w, r, id, req.ID) {
		return
	}
	bucket, ns, name, err := id.table()
	if err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, err.Error())
		return
	}
	if req.Version != nil || req.Tag != "" || req.Branch != "" {
		writeError(w, r, http.StatusNotImplemented, codeUnsupported,
			"this namespace does not resolve versions, tags or branches; the dataset owns them")
		return
	}

	table, err := s.loadLanceTable(r, bucket, ns, name)
	if err != nil {
		writeStorageError(w, r, err)
		return
	}
	location := table.location

	deregistered, hasData, err := s.datasetState(r, location)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, codeInternal, err.Error())
		return
	}
	if deregistered {
		writeError(w, r, http.StatusNotFound, codeTableNotFound, "table is deregistered")
		return
	}

	options, err := s.storageOptions(r, bucket, location, wants(r, "vend_credentials", req.VendCredentials))
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, codeInternal, err.Error())
		return
	}

	resp := DescribeTableResponse{
		Location: location,
	}
	if len(options) > 0 {
		resp.StorageOptions = options
	}
	if wants(r, "with_table_uri", req.WithTableURI) {
		resp.TableURI = location
	}
	if wants(r, "check_declared", req.CheckDeclared) {
		onlyDeclared := !hasData
		resp.IsOnlyDeclared = &onlyDeclared
	}
	if wants(r, "load_detailed_metadata", req.LoadDetailedMetadata) {
		resp.Table = name
		resp.Namespace = ns
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleTableExists answers with the status code and no body.
func (s *Server) handleTableExists(w http.ResponseWriter, r *http.Request) {
	id, _, ok := routeIdentifier(w, r)
	if !ok {
		return
	}
	var req TableExistsRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, err.Error())
		return
	}
	if len(id) == 0 && len(req.ID) > 0 {
		id = req.ID
	}
	if !checkBodyIdentifier(w, r, id, req.ID) {
		return
	}
	bucket, ns, name, err := id.table()
	if err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, err.Error())
		return
	}

	table, err := s.loadLanceTable(r, bucket, ns, name)
	if err != nil {
		if isNotFound(err) {
			writeError(w, r, http.StatusNotFound, codeTableNotFound, "table does not exist")
			return
		}
		writeStorageError(w, r, err)
		return
	}
	deregistered, _, err := s.datasetState(r, table.location)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, codeInternal, err.Error())
		return
	}
	if deregistered {
		writeError(w, r, http.StatusNotFound, codeTableNotFound, "table is deregistered")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleRegisterTable points a table name at an existing dataset.
func (s *Server) handleRegisterTable(w http.ResponseWriter, r *http.Request) {
	id, _, ok := routeIdentifier(w, r)
	if !ok {
		return
	}
	var req RegisterTableRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, err.Error())
		return
	}
	if len(id) == 0 && len(req.ID) > 0 {
		id = req.ID
	}
	if !checkBodyIdentifier(w, r, id, req.ID) {
		return
	}
	bucket, ns, name, err := id.table()
	if err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, err.Error())
		return
	}
	location := strings.TrimSuffix(req.Location, "/")
	if location == "" {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, "location is required")
		return
	}

	mode := normalizeMode(req.Mode, modeCreate)
	if mode != modeCreate && mode != modeOverwrite {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, "mode must be Create or Overwrite")
		return
	}

	existing, err := s.loadLanceTable(r, bucket, ns, name)
	switch {
	case err != nil && !isNotFound(err):
		writeStorageError(w, r, err)
		return
	case err == nil:
		// A deregistered table is absent as far as the spec is concerned, so
		// registering over it is a re-registration rather than a conflict.
		deregistered, _, stateErr := s.datasetState(r, existing.location)
		if stateErr != nil {
			writeError(w, r, http.StatusInternalServerError, codeInternal, stateErr.Error())
			return
		}
		if !deregistered && mode == modeCreate {
			writeError(w, r, http.StatusConflict, codeTableAlreadyExists, "table already exists")
			return
		}
		if existing.location != location {
			// Repointing the name at another dataset is an update. Dropping and
			// recreating the entry would take the old dataset's files with it,
			// because the entry is the directory holding them.
			if err := s.repointTable(r, bucket, ns, name, location, existing.versionToken); err != nil {
				writeStorageError(w, r, err)
				return
			}
		}
	default:
		if err := s.createTable(r, bucket, ns, name, location); err != nil {
			writeStorageError(w, r, err)
			return
		}
	}

	// Registering a deregistered dataset brings it back.
	if err := s.removeMarker(r, location, deregisteredMarker); err != nil {
		glog.V(2).Infof("lance: could not clear %s for %s: %v", deregisteredMarker, location, err)
	}

	writeJSON(w, http.StatusOK, RegisterTableResponse{
		Location:   location,
		Properties: normalizeProperties(req.Properties),
	})
}

// handleDeregisterTable forgets a table without touching its data.
func (s *Server) handleDeregisterTable(w http.ResponseWriter, r *http.Request) {
	id, _, ok := routeIdentifier(w, r)
	if !ok {
		return
	}
	var req DeregisterTableRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, err.Error())
		return
	}
	if len(id) == 0 && len(req.ID) > 0 {
		id = req.ID
	}
	if !checkBodyIdentifier(w, r, id, req.ID) {
		return
	}
	bucket, ns, name, err := id.table()
	if err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, err.Error())
		return
	}

	table, err := s.loadLanceTable(r, bucket, ns, name)
	if err != nil {
		writeStorageError(w, r, err)
		return
	}

	// Deregistering is a state, not a deletion. Dropping the catalog entry would
	// take the dataset with it, because the entry is the dataset directory and
	// DeleteTable purges what it holds. The marker is what hides the table, and
	// it is also what a directory-catalog client reads.
	if err := s.writeMarker(r, table.location, deregisteredMarker); err != nil {
		writeError(w, r, http.StatusInternalServerError, codeInternal,
			"could not mark the table deregistered: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, DeregisterTableResponse{
		ID:         append(append([]string{bucket}, ns...), name),
		Location:   table.location,
		Properties: map[string]string{},
	})
}

// handleDropTable removes the table and its data.
func (s *Server) handleDropTable(w http.ResponseWriter, r *http.Request) {
	id, _, ok := routeIdentifier(w, r)
	if !ok {
		return
	}
	var req DropTableRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, err.Error())
		return
	}
	if len(id) == 0 && len(req.ID) > 0 {
		id = req.ID
	}
	if !checkBodyIdentifier(w, r, id, req.ID) {
		return
	}
	bucket, ns, name, err := id.table()
	if err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, err.Error())
		return
	}

	table, err := s.loadLanceTable(r, bucket, ns, name)
	if err != nil {
		writeStorageError(w, r, err)
		return
	}
	if err := s.dropTableEntry(r, bucket, ns, name); err != nil {
		writeStorageError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, DropTableResponse{
		ID:         append(append([]string{bucket}, ns...), name),
		Location:   table.location,
		Properties: map[string]string{},
	})
}

// handleRenameTable moves a table's catalog entry. The dataset stays put, which
// is what the storage layer already does for a renamed Iceberg table.
func (s *Server) handleRenameTable(w http.ResponseWriter, r *http.Request) {
	id, _, ok := routeIdentifier(w, r)
	if !ok {
		return
	}
	var req RenameTableRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, err.Error())
		return
	}
	if len(id) == 0 && len(req.ID) > 0 {
		id = req.ID
	}
	if !checkBodyIdentifier(w, r, id, req.ID) {
		return
	}
	bucket, ns, name, err := id.table()
	if err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, err.Error())
		return
	}
	destBucket, destNS, destName, err := identifier(req.NewID).table()
	if err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, "new_id: "+err.Error())
		return
	}
	if destBucket != bucket {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput,
			"a table cannot move between table buckets")
		return
	}

	if _, err := s.loadLanceTable(r, bucket, ns, name); err != nil {
		writeStorageError(w, r, err)
		return
	}

	renameReq := &s3tables.RenameTableRequest{
		TableBucketARN:  bucketARN(bucket),
		SourceNamespace: ns,
		SourceName:      name,
		DestNamespace:   destNS,
		DestName:        destName,
	}
	var renameResp s3tables.RenameTableResponse
	if err := s.execute(r, "RenameTable", renameReq, &renameResp); err != nil {
		writeStorageError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, RenameTableResponse{})
}

// lanceTable is the catalog's record of one Lance table.
type lanceTable struct {
	location     string
	versionToken string
}

// loadLanceTable reads a table and refuses one that is not a Lance table, so a
// Lance client never resolves an Iceberg table's location and writes over it.
func (s *Server) loadLanceTable(r *http.Request, bucket string, ns []string, name string) (*lanceTable, error) {
	req := &s3tables.GetTableRequest{
		TableBucketARN: bucketARN(bucket),
		Namespace:      ns,
		Name:           name,
	}
	var resp s3tables.GetTableResponse
	if err := s.execute(r, "GetTable", req, &resp); err != nil {
		return nil, err
	}
	if resp.Format != s3tables.FormatLance {
		return nil, &s3tables.S3TablesError{
			Type:    s3tables.ErrCodeNoSuchTable,
			Message: "table " + name + " is not a lance table",
		}
	}
	location := resp.MetadataLocation
	if location == "" {
		location = tableLocation(bucket, resp.Namespace, resp.Name)
	}
	return &lanceTable{location: location, versionToken: resp.VersionToken}, nil
}

func (s *Server) createTable(r *http.Request, bucket string, ns []string, name, location string) error {
	req := &s3tables.CreateTableRequest{
		TableBucketARN: bucketARN(bucket),
		Namespace:      ns,
		Name:           name,
		Format:         s3tables.FormatLance,
		// A Lance table has no metadata file, so the location the catalog stores
		// is the dataset root itself.
		MetadataLocation: location,
	}
	var resp s3tables.CreateTableResponse
	return s.execute(r, "CreateTable", req, &resp)
}

// repointTable moves an existing entry to another dataset location without
// touching either dataset's files.
func (s *Server) repointTable(r *http.Request, bucket string, ns []string, name, location, versionToken string) error {
	req := &s3tables.UpdateTableRequest{
		TableBucketARN:   bucketARN(bucket),
		Namespace:        ns,
		Name:             name,
		VersionToken:     versionToken,
		MetadataLocation: location,
	}
	var resp s3tables.UpdateTableResponse
	return s.execute(r, "UpdateTable", req, &resp)
}

// dropTableEntry removes the catalog entry and the dataset under it. Only
// DropTable wants this; deregistering and repointing must leave the files.
func (s *Server) dropTableEntry(r *http.Request, bucket string, ns []string, name string) error {
	req := &s3tables.DeleteTableRequest{
		TableBucketARN: bucketARN(bucket),
		Namespace:      ns,
		Name:           name,
	}
	return s.execute(r, "DeleteTable", req, nil)
}
