package lance

import (
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// handleCreateNamespace creates a table bucket for a one-part identifier and a
// namespace inside one for anything deeper. The root cannot be created.
func (s *Server) handleCreateNamespace(w http.ResponseWriter, r *http.Request) {
	id, _, ok := routeIdentifier(w, r)
	if !ok {
		return
	}

	var req CreateNamespaceRequest
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
	if len(id) == 0 {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, "the root namespace always exists and cannot be created")
		return
	}

	mode := normalizeMode(req.Mode, modeCreate)
	switch mode {
	case modeCreate, modeExistOk, modeOverwrite:
	default:
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, "mode must be Create, ExistOk or Overwrite")
		return
	}

	bucket, ns := id.namespace()
	exists, err := s.namespaceExists(r, bucket, ns)
	if err != nil {
		writeStorageError(w, r, err)
		return
	}
	if exists {
		switch mode {
		case modeExistOk:
			writeJSON(w, http.StatusOK, CreateNamespaceResponse{Properties: req.Properties})
			return
		case modeCreate:
			writeError(w, r, http.StatusConflict, codeNamespaceAlreadyExists, "namespace already exists")
			return
		}
		// Overwrite replaces the namespace with an empty one, so the drop has to
		// succeed first. A namespace holding tables refuses, which is the point.
		if err := s.dropNamespace(r, bucket, ns); err != nil {
			writeStorageError(w, r, err)
			return
		}
	}

	if err := s.createNamespace(r, bucket, ns, req.Properties); err != nil {
		writeStorageError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, CreateNamespaceResponse{Properties: normalizeProperties(req.Properties)})
}

// handleListNamespaces lists the children of a namespace: table buckets at the
// root, and the next path component below that.
func (s *Server) handleListNamespaces(w http.ResponseWriter, r *http.Request) {
	id, _, ok := routeIdentifier(w, r)
	if !ok {
		return
	}
	bucket, ns := id.namespace()

	if bucket == "" {
		var resp s3tables.ListTableBucketsResponse
		req := &s3tables.ListTableBucketsRequest{
			ContinuationToken: r.URL.Query().Get("page_token"),
			MaxBuckets:        pageSize(r),
		}
		if err := s.execute(r, "ListTableBuckets", req, &resp); err != nil {
			writeStorageError(w, r, err)
			return
		}
		names := make([]string, 0, len(resp.TableBuckets))
		for _, b := range resp.TableBuckets {
			names = append(names, b.Name)
		}
		writeJSON(w, http.StatusOK, ListNamespacesResponse{Namespaces: names, PageToken: resp.ContinuationToken})
		return
	}

	var resp s3tables.ListNamespacesResponse
	req := &s3tables.ListNamespacesRequest{
		TableBucketARN:    bucketARN(bucket),
		ContinuationToken: r.URL.Query().Get("page_token"),
		MaxNamespaces:     pageSize(r),
	}
	if len(ns) > 0 {
		req.Prefix = strings.Join(ns, ".") + "."
	}
	if err := s.execute(r, "ListNamespaces", req, &resp); err != nil {
		writeStorageError(w, r, err)
		return
	}

	// Storage namespaces are full paths; the spec wants the child name relative
	// to the parent, so take the next component and drop repeats.
	children := make([]string, 0, len(resp.Namespaces))
	seen := make(map[string]struct{}, len(resp.Namespaces))
	for _, summary := range resp.Namespaces {
		if len(summary.Namespace) <= len(ns) {
			continue
		}
		child := summary.Namespace[len(ns)]
		if _, done := seen[child]; done {
			continue
		}
		seen[child] = struct{}{}
		children = append(children, child)
	}
	writeJSON(w, http.StatusOK, ListNamespacesResponse{Namespaces: children, PageToken: resp.ContinuationToken})
}

// handleDescribeNamespace returns a namespace's properties.
func (s *Server) handleDescribeNamespace(w http.ResponseWriter, r *http.Request) {
	id, _, ok := routeIdentifier(w, r)
	if !ok {
		return
	}
	var req DescribeNamespaceRequest
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

	bucket, ns := id.namespace()
	if bucket == "" {
		writeJSON(w, http.StatusOK, DescribeNamespaceResponse{Properties: map[string]string{}})
		return
	}
	if len(ns) == 0 {
		var resp s3tables.GetTableBucketResponse
		if err := s.execute(r, "GetTableBucket", &s3tables.GetTableBucketRequest{TableBucketARN: bucketARN(bucket)}, &resp); err != nil {
			writeStorageError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, DescribeNamespaceResponse{Properties: map[string]string{}})
		return
	}

	var resp s3tables.GetNamespaceResponse
	req2 := &s3tables.GetNamespaceRequest{TableBucketARN: bucketARN(bucket), Namespace: ns}
	if err := s.execute(r, "GetNamespace", req2, &resp); err != nil {
		writeStorageError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, DescribeNamespaceResponse{Properties: normalizeProperties(resp.Properties)})
}

// handleDropNamespace removes a namespace or table bucket.
func (s *Server) handleDropNamespace(w http.ResponseWriter, r *http.Request) {
	id, _, ok := routeIdentifier(w, r)
	if !ok {
		return
	}
	var req DropNamespaceRequest
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
	if len(id) == 0 {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, "the root namespace cannot be dropped")
		return
	}

	if normalizeMode(req.Behavior, behaviorRestrict) == behaviorCascade {
		writeError(w, r, http.StatusNotImplemented, codeUnsupported,
			"cascade drop is not supported; drop the tables in the namespace first")
		return
	}

	bucket, ns := id.namespace()
	err := s.dropNamespace(r, bucket, ns)
	if err == nil {
		writeJSON(w, http.StatusOK, DropNamespaceResponse{})
		return
	}
	if isNotFound(err) {
		// Skip reports success on a missing namespace; Fail reports 400 rather
		// than 404, which is what the spec asks for on this operation alone.
		if normalizeMode(req.Mode, modeFail) == modeSkip {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, r, http.StatusBadRequest, codeNamespaceNotFound, "namespace does not exist")
		return
	}
	writeStorageError(w, r, err)
}

// handleNamespaceExists answers with the status code and no body.
func (s *Server) handleNamespaceExists(w http.ResponseWriter, r *http.Request) {
	id, _, ok := routeIdentifier(w, r)
	if !ok {
		return
	}
	var req NamespaceExistsRequest
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

	bucket, ns := id.namespace()
	if bucket == "" {
		w.WriteHeader(http.StatusOK)
		return
	}
	exists, err := s.namespaceExists(r, bucket, ns)
	if err != nil {
		writeStorageError(w, r, err)
		return
	}
	if !exists {
		writeError(w, r, http.StatusNotFound, codeNamespaceNotFound, "namespace does not exist")
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) namespaceExists(r *http.Request, bucket string, ns []string) (bool, error) {
	var err error
	if len(ns) == 0 {
		var resp s3tables.GetTableBucketResponse
		err = s.execute(r, "GetTableBucket", &s3tables.GetTableBucketRequest{TableBucketARN: bucketARN(bucket)}, &resp)
	} else {
		var resp s3tables.GetNamespaceResponse
		err = s.execute(r, "GetNamespace", &s3tables.GetNamespaceRequest{TableBucketARN: bucketARN(bucket), Namespace: ns}, &resp)
	}
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, err
}

func (s *Server) createNamespace(r *http.Request, bucket string, ns []string, properties map[string]string) error {
	if len(ns) == 0 {
		// A bucket made through this surface holds Lance tables. Saying so is
		// what stops it from being described to a client as an Iceberg catalog.
		var resp s3tables.CreateTableBucketResponse
		return s.execute(r, "CreateTableBucket", &s3tables.CreateTableBucketRequest{
			Name:   bucket,
			Format: s3tables.FormatLance,
		}, &resp)
	}
	// A table bucket is a tenant resource with its own policy and lifecycle, so
	// it is created deliberately, never as a side effect of naming a namespace
	// inside it.
	// The immediate parent has to exist too. Storage keeps a namespace's parts
	// flattened, so creating "a.b" without "a" leaves an intermediate that
	// listing derives from the name and describe then denies exists. The spec
	// asks for NamespaceNotFound here, which also keeps the two consistent.
	if len(ns) > 1 {
		parent := ns[:len(ns)-1]
		if exists, err := s.namespaceExists(r, bucket, parent); err != nil {
			return err
		} else if !exists {
			return &s3tables.S3TablesError{
				Type:    s3tables.ErrCodeNoSuchNamespace,
				Message: "parent namespace " + strings.Join(parent, ".") + " does not exist",
			}
		}
	}
	if exists, err := s.namespaceExists(r, bucket, nil); err != nil {
		return err
	} else if !exists {
		return &s3tables.S3TablesError{
			Type:    s3tables.ErrCodeNoSuchBucket,
			Message: "table bucket " + bucket + " does not exist",
		}
	}
	var resp s3tables.CreateNamespaceResponse
	req := &s3tables.CreateNamespaceRequest{
		TableBucketARN: bucketARN(bucket),
		Namespace:      ns,
		Properties:     properties,
	}
	return s.execute(r, "CreateNamespace", req, &resp)
}

func (s *Server) dropNamespace(r *http.Request, bucket string, ns []string) error {
	if len(ns) == 0 {
		return s.execute(r, "DeleteTableBucket", &s3tables.DeleteTableBucketRequest{TableBucketARN: bucketARN(bucket)}, nil)
	}
	req := &s3tables.DeleteNamespaceRequest{TableBucketARN: bucketARN(bucket), Namespace: ns}
	return s.execute(r, "DeleteNamespace", req, nil)
}

// normalizeMode folds a spec mode or behavior value, which is case-insensitive
// and spelled either PascalCase or snake_case, onto its lowercase form.
func normalizeMode(value, fallback string) string {
	value = strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), "_", ""))
	if value == "" {
		return fallback
	}
	return value
}

func normalizeProperties(properties map[string]string) map[string]string {
	if properties == nil {
		return map[string]string{}
	}
	return properties
}
