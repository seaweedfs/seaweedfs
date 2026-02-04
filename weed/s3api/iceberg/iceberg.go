// Package iceberg provides Iceberg REST Catalog API support.
// It implements the Apache Iceberg REST Catalog specification
// backed by S3 Tables metadata storage.
package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

func (s *Server) checkAuth(w http.ResponseWriter, r *http.Request, action s3api.Action, bucketName string) bool {
	identityName := s3_constants.GetIdentityNameFromContext(r)
	if identityName == "" {
		writeError(w, http.StatusUnauthorized, "NotAuthorizedException", "Authentication required")
		return false
	}

	identityObj := s3_constants.GetIdentityFromContext(r)
	if identityObj == nil {
		writeError(w, http.StatusForbidden, "ForbiddenException", "Access denied: missing identity")
		return false
	}
	identity, ok := identityObj.(*s3api.Identity)
	if !ok {
		writeError(w, http.StatusForbidden, "ForbiddenException", "Access denied: invalid identity")
		return false
	}

	if !identity.CanDo(action, bucketName, "") {
		writeError(w, http.StatusForbidden, "ForbiddenException", "Access denied")
		return false
	}
	return true
}

// FilerClient provides access to the filer for storage operations.
type FilerClient interface {
	WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error
}

type S3Authenticator interface {
	AuthenticateRequest(r *http.Request) (string, interface{}, s3err.ErrorCode)
}

// Server implements the Iceberg REST Catalog API.
type Server struct {
	filerClient   FilerClient
	tablesManager *s3tables.Manager
	prefix        string // optional prefix for routes
	authenticator S3Authenticator
}

// NewServer creates a new Iceberg REST Catalog server.
func NewServer(filerClient FilerClient, authenticator S3Authenticator) *Server {
	manager := s3tables.NewManager()
	return &Server{
		filerClient:   filerClient,
		tablesManager: manager,
		prefix:        "",
		authenticator: authenticator,
	}
}

// RegisterRoutes registers Iceberg REST API routes on the provided router.
func (s *Server) RegisterRoutes(router *mux.Router) {
	// Configuration endpoint
	router.HandleFunc("/v1/config", s.Auth(s.handleConfig)).Methods(http.MethodGet)

	// Namespace endpoints
	router.HandleFunc("/v1/namespaces", s.Auth(s.handleListNamespaces)).Methods(http.MethodGet)
	router.HandleFunc("/v1/namespaces", s.Auth(s.handleCreateNamespace)).Methods(http.MethodPost)
	router.HandleFunc("/v1/namespaces/{namespace}", s.Auth(s.handleGetNamespace)).Methods(http.MethodGet)
	router.HandleFunc("/v1/namespaces/{namespace}", s.Auth(s.handleNamespaceExists)).Methods(http.MethodHead)
	router.HandleFunc("/v1/namespaces/{namespace}", s.Auth(s.handleDropNamespace)).Methods(http.MethodDelete)

	// Table endpoints
	router.HandleFunc("/v1/namespaces/{namespace}/tables", s.Auth(s.handleListTables)).Methods(http.MethodGet)
	router.HandleFunc("/v1/namespaces/{namespace}/tables", s.Auth(s.handleCreateTable)).Methods(http.MethodPost)
	router.HandleFunc("/v1/namespaces/{namespace}/tables/{table}", s.Auth(s.handleLoadTable)).Methods(http.MethodGet)
	router.HandleFunc("/v1/namespaces/{namespace}/tables/{table}", s.Auth(s.handleTableExists)).Methods(http.MethodHead)
	router.HandleFunc("/v1/namespaces/{namespace}/tables/{table}", s.Auth(s.handleDropTable)).Methods(http.MethodDelete)
	router.HandleFunc("/v1/namespaces/{namespace}/tables/{table}", s.Auth(s.handleUpdateTable)).Methods(http.MethodPost)

	// With prefix support
	router.HandleFunc("/v1/{prefix}/namespaces", s.Auth(s.handleListNamespaces)).Methods(http.MethodGet)
	router.HandleFunc("/v1/{prefix}/namespaces", s.Auth(s.handleCreateNamespace)).Methods(http.MethodPost)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}", s.Auth(s.handleGetNamespace)).Methods(http.MethodGet)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}", s.Auth(s.handleNamespaceExists)).Methods(http.MethodHead)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}", s.Auth(s.handleDropNamespace)).Methods(http.MethodDelete)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables", s.Auth(s.handleListTables)).Methods(http.MethodGet)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables", s.Auth(s.handleCreateTable)).Methods(http.MethodPost)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables/{table}", s.Auth(s.handleLoadTable)).Methods(http.MethodGet)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables/{table}", s.Auth(s.handleTableExists)).Methods(http.MethodHead)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables/{table}", s.Auth(s.handleDropTable)).Methods(http.MethodDelete)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables/{table}", s.Auth(s.handleUpdateTable)).Methods(http.MethodPost)

	glog.V(0).Infof("Registered Iceberg REST Catalog routes")
}

func (s *Server) Auth(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.authenticator == nil {
			writeError(w, http.StatusUnauthorized, "NotAuthorizedException", "Authentication required")
			return
		}

		identityName, identity, errCode := s.authenticator.AuthenticateRequest(r)
		if errCode != s3err.ErrNone {
			apiErr := s3err.GetAPIError(errCode)
			errorType := "RESTException"
			switch apiErr.HTTPStatusCode {
			case http.StatusForbidden:
				errorType = "ForbiddenException"
			case http.StatusUnauthorized:
				errorType = "NotAuthorizedException"
			case http.StatusBadRequest:
				errorType = "BadRequestException"
			case http.StatusInternalServerError:
				errorType = "InternalServerError"
			}
			writeError(w, apiErr.HTTPStatusCode, errorType, apiErr.Description)
			return
		}

		if identityName != "" || identity != nil {
			ctx := r.Context()
			if identityName != "" {
				ctx = s3_constants.SetIdentityNameInContext(ctx, identityName)
			}
			if identity != nil {
				ctx = s3_constants.SetIdentityInContext(ctx, identity)
			}
			r = r.WithContext(ctx)
		}

		handler(w, r)
	}
}

// saveMetadataFile saves the Iceberg metadata JSON file to the filer.
// It constructs the correct filler path from the S3 location components.
func (s *Server) saveMetadataFile(ctx context.Context, bucketName, namespace, tableName, metadataFileName string, content []byte) error {
	// Construct filer path: /table-buckets/<bucket>/<namespace>/<table>/metadata/<filename>
	// Note: s3tables.TablesPath is "/table-buckets"

	// Create context with timeout for file operations
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// 1. Ensure table directory exists: /table-buckets/<bucket>/<namespace>/<table>
		tableDir := fmt.Sprintf("/table-buckets/%s/%s/%s", bucketName, namespace, tableName)
		_, err := filer_pb.LookupEntry(opCtx, client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: fmt.Sprintf("/table-buckets/%s/%s", bucketName, namespace),
			Name:      tableName,
		})
		if err != nil {
			resp, err := client.CreateEntry(opCtx, &filer_pb.CreateEntryRequest{
				Directory: fmt.Sprintf("/table-buckets/%s/%s", bucketName, namespace),
				Entry: &filer_pb.Entry{
					Name:        tableName,
					IsDirectory: true,
					Attributes: &filer_pb.FuseAttributes{
						Mtime:    time.Now().Unix(),
						Crtime:   time.Now().Unix(),
						FileMode: uint32(0755 | os.ModeDir),
					},
				},
			})
			if err != nil {
				return fmt.Errorf("failed to create table directory: %w", err)
			}
			if resp.Error != "" && !strings.Contains(resp.Error, "exist") {
				return fmt.Errorf("failed to create table directory: %s", resp.Error)
			}
		}

		// 2. Ensure metadata directory exists: /table-buckets/<bucket>/<namespace>/<table>/metadata
		metadataDir := fmt.Sprintf("%s/metadata", tableDir)
		_, err = filer_pb.LookupEntry(opCtx, client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: tableDir,
			Name:      "metadata",
		})
		if err != nil {
			resp, err := client.CreateEntry(opCtx, &filer_pb.CreateEntryRequest{
				Directory: tableDir,
				Entry: &filer_pb.Entry{
					Name:        "metadata",
					IsDirectory: true,
					Attributes: &filer_pb.FuseAttributes{
						Mtime:    time.Now().Unix(),
						Crtime:   time.Now().Unix(),
						FileMode: uint32(0755 | os.ModeDir),
					},
				},
			})
			if err != nil {
				return fmt.Errorf("failed to create metadata directory: %w", err)
			}
			if resp.Error != "" && !strings.Contains(resp.Error, "exist") {
				return fmt.Errorf("failed to create metadata directory: %s", resp.Error)
			}
		}

		// 3. Write the file
		resp, err := client.CreateEntry(opCtx, &filer_pb.CreateEntryRequest{
			Directory: metadataDir,
			Entry: &filer_pb.Entry{
				Name: metadataFileName,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					Crtime:   time.Now().Unix(),
					FileMode: uint32(0644),
					FileSize: uint64(len(content)),
				},
				Content: content,
				Extended: map[string][]byte{
					"Mime-Type": []byte("application/json"),
				},
			},
		})
		if err != nil {
			return fmt.Errorf("failed to write metadata file context: %w", err)
		}
		if resp.Error != "" {
			return fmt.Errorf("failed to write metadata file: %s", resp.Error)
		}
		return nil
	})
}

// parseNamespace parses the namespace from path parameter.
// Iceberg uses unit separator (0x1F) for multi-level namespaces.
// Note: mux already decodes URL-encoded path parameters, so we only split by unit separator.
func parseNamespace(encoded string) []string {
	if encoded == "" {
		return nil
	}
	parts := strings.Split(encoded, "\x1F")
	// Filter empty parts
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// encodeNamespace encodes namespace parts for response.
func encodeNamespace(parts []string) string {
	return strings.Join(parts, "\x1F")
}

// writeJSON writes a JSON response.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		data, err := json.Marshal(v)
		if err != nil {
			glog.Errorf("Iceberg: failed to encode response: %v", err)
			return
		}
		w.Write(data)
	}
}

// writeError writes an Iceberg error response.
func writeError(w http.ResponseWriter, status int, errType, message string) {
	resp := ErrorResponse{
		Error: ErrorModel{
			Message: message,
			Type:    errType,
			Code:    status,
		},
	}
	writeJSON(w, status, resp)
}

// getBucketFromPrefix extracts table bucket name from prefix parameter.
// For now, we use the prefix as the table bucket name.
func getBucketFromPrefix(r *http.Request) string {
	vars := mux.Vars(r)
	if prefix := vars["prefix"]; prefix != "" {
		return prefix
	}
	// Default bucket if no prefix
	return "default"
}

// buildTableBucketARN builds an ARN for a table bucket.
func buildTableBucketARN(bucketName string) string {
	arn, _ := s3tables.BuildBucketARN(s3tables.DefaultRegion, s3_constants.AccountAdminId, bucketName)
	return arn
}

// handleConfig returns catalog configuration.
func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	bucketName := getBucketFromPrefix(r)
	if !s.checkAuth(w, r, s3_constants.ACTION_READ, bucketName) {
		return
	}
	config := CatalogConfig{
		Defaults:  map[string]string{},
		Overrides: map[string]string{},
	}
	writeJSON(w, http.StatusOK, config)
}

// handleListNamespaces lists namespaces in a catalog.
func (s *Server) handleListNamespaces(w http.ResponseWriter, r *http.Request) {
	bucketName := getBucketFromPrefix(r)
	if !s.checkAuth(w, r, s3_constants.ACTION_LIST, bucketName) {
		return
	}
	bucketARN := buildTableBucketARN(bucketName)

	// Use S3 Tables manager to list namespaces
	var resp s3tables.ListNamespacesResponse
	req := &s3tables.ListNamespacesRequest{
		TableBucketARN: bucketARN,
		MaxNamespaces:  1000,
	}

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "ListNamespaces", req, &resp, "")
	})

	if err != nil {
		glog.V(1).Infof("Iceberg: ListNamespaces error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	// Convert to Iceberg format
	namespaces := make([]Namespace, 0, len(resp.Namespaces))
	for _, ns := range resp.Namespaces {
		namespaces = append(namespaces, Namespace(ns.Namespace))
	}

	result := ListNamespacesResponse{
		Namespaces: namespaces,
	}
	writeJSON(w, http.StatusOK, result)
}

// handleCreateNamespace creates a new namespace.
func (s *Server) handleCreateNamespace(w http.ResponseWriter, r *http.Request) {
	bucketName := getBucketFromPrefix(r)
	if !s.checkAuth(w, r, s3_constants.ACTION_WRITE, bucketName) {
		return
	}
	bucketARN := buildTableBucketARN(bucketName)

	var req CreateNamespaceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid request body")
		return
	}

	if len(req.Namespace) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace is required")
		return
	}

	// Use S3 Tables manager to create namespace
	createReq := &s3tables.CreateNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      req.Namespace,
	}
	var createResp s3tables.CreateNamespaceResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "CreateNamespace", createReq, &createResp, "")
	})

	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			writeError(w, http.StatusConflict, "AlreadyExistsException", err.Error())
			return
		}
		glog.V(1).Infof("Iceberg: CreateNamespace error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	// Standardize property initialization for consistency with GetNamespace
	props := req.Properties
	if props == nil {
		props = make(map[string]string)
	}

	result := CreateNamespaceResponse{
		Namespace:  req.Namespace,
		Properties: props,
	}
	writeJSON(w, http.StatusOK, result)
}

// handleGetNamespace gets namespace metadata.
func (s *Server) handleGetNamespace(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	if len(namespace) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace is required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	if !s.checkAuth(w, r, s3_constants.ACTION_READ, bucketName) {
		return
	}
	bucketARN := buildTableBucketARN(bucketName)

	// Use S3 Tables manager to get namespace
	getReq := &s3tables.GetNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
	}
	var getResp s3tables.GetNamespaceResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetNamespace", getReq, &getResp, "")
	})

	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "NoSuchNamespaceException", fmt.Sprintf("Namespace does not exist: %v", namespace))
			return
		}
		glog.V(1).Infof("Iceberg: GetNamespace error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	result := GetNamespaceResponse{
		Namespace:  namespace,
		Properties: make(map[string]string),
	}
	writeJSON(w, http.StatusOK, result)
}

// handleNamespaceExists checks if a namespace exists.
func (s *Server) handleNamespaceExists(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	if len(namespace) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	bucketName := getBucketFromPrefix(r)
	if !s.checkAuth(w, r, s3_constants.ACTION_READ, bucketName) {
		return
	}
	bucketARN := buildTableBucketARN(bucketName)

	getReq := &s3tables.GetNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
	}
	var getResp s3tables.GetNamespaceResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetNamespace", getReq, &getResp, "")
	})

	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		glog.V(1).Infof("Iceberg: NamespaceExists error: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleDropNamespace deletes a namespace.
func (s *Server) handleDropNamespace(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	if len(namespace) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace is required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	if !s.checkAuth(w, r, s3_constants.ACTION_DELETE_BUCKET, bucketName) {
		return
	}
	bucketARN := buildTableBucketARN(bucketName)

	deleteReq := &s3tables.DeleteNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
	}

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "DeleteNamespace", deleteReq, nil, "")
	})

	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "NoSuchNamespaceException", fmt.Sprintf("Namespace does not exist: %v", namespace))
			return
		}
		if strings.Contains(err.Error(), "not empty") {
			writeError(w, http.StatusConflict, "NamespaceNotEmptyException", "Namespace is not empty")
			return
		}
		glog.V(1).Infof("Iceberg: DropNamespace error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleListTables lists tables in a namespace.
func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	if len(namespace) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace is required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	if !s.checkAuth(w, r, s3_constants.ACTION_LIST, bucketName) {
		return
	}
	bucketARN := buildTableBucketARN(bucketName)

	listReq := &s3tables.ListTablesRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		MaxTables:      1000,
	}
	var listResp s3tables.ListTablesResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "ListTables", listReq, &listResp, "")
	})

	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "NoSuchNamespaceException", fmt.Sprintf("Namespace does not exist: %v", namespace))
			return
		}
		glog.V(1).Infof("Iceberg: ListTables error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	// Convert to Iceberg format
	identifiers := make([]TableIdentifier, 0, len(listResp.Tables))
	for _, t := range listResp.Tables {
		identifiers = append(identifiers, TableIdentifier{
			Namespace: namespace,
			Name:      t.Name,
		})
	}

	result := ListTablesResponse{
		Identifiers: identifiers,
	}
	writeJSON(w, http.StatusOK, result)
}

// handleCreateTable creates a new table.
func (s *Server) handleCreateTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	if len(namespace) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace is required")
		return
	}

	var req CreateTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid request body")
		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Table name is required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	if !s.checkAuth(w, r, s3_constants.ACTION_WRITE, bucketName) {
		return
	}
	bucketARN := buildTableBucketARN(bucketName)

	// Generate UUID for the new table
	tableUUID := uuid.New()
	location := fmt.Sprintf("s3://%s/%s/%s", bucketName, encodeNamespace(namespace), req.Name)

	// Build proper Iceberg table metadata using iceberg-go types
	metadata := newTableMetadata(tableUUID, location, req.Schema, req.PartitionSpec, req.WriteOrder, req.Properties)
	if metadata == nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to build table metadata")
		return
	}

	// Serialize metadata to JSON
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to serialize metadata: "+err.Error())
		return
	}

	// 1. Save metadata file to filer
	tableName := req.Name
	metadataFileName := "v1.metadata.json" // Initial version is always 1
	if err := s.saveMetadataFile(r.Context(), bucketName, encodeNamespace(namespace), tableName, metadataFileName, metadataBytes); err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to save metadata file: "+err.Error())
		return
	}

	metadataLocation := fmt.Sprintf("s3://%s/%s/%s/metadata/%s", bucketName, encodeNamespace(namespace), tableName, metadataFileName)

	// Use S3 Tables manager to create table
	createReq := &s3tables.CreateTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
		Format:         "ICEBERG",
		Metadata: &s3tables.TableMetadata{
			Iceberg: &s3tables.IcebergMetadata{
				TableUUID: tableUUID.String(),
			},
			FullMetadata: metadataBytes,
		},
		MetadataLocation: metadataLocation,
		MetadataVersion:  1,
	}
	var createResp s3tables.CreateTableResponse

	err = s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "CreateTable", createReq, &createResp, "")
	})

	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			writeError(w, http.StatusConflict, "AlreadyExistsException", err.Error())
			return
		}
		glog.V(1).Infof("Iceberg: CreateTable error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	// Use returned location if available, otherwise fallback to local one
	finalLocation := createResp.MetadataLocation
	if finalLocation == "" {
		finalLocation = metadataLocation
	}

	result := LoadTableResult{
		MetadataLocation: finalLocation,
		Metadata:         metadata,
		Config:           make(iceberg.Properties),
	}
	writeJSON(w, http.StatusOK, result)
}

// handleLoadTable loads table metadata.
func (s *Server) handleLoadTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	tableName := vars["table"]

	if len(namespace) == 0 || tableName == "" {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace and table name are required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	if !s.checkAuth(w, r, s3_constants.ACTION_READ, bucketName) {
		return
	}
	bucketARN := buildTableBucketARN(bucketName)

	getReq := &s3tables.GetTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
	}
	var getResp s3tables.GetTableResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", getReq, &getResp, "")
	})

	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "NoSuchTableException", fmt.Sprintf("Table does not exist: %s", tableName))
			return
		}
		glog.V(1).Infof("Iceberg: LoadTable error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	// Build table metadata using iceberg-go types
	location := fmt.Sprintf("s3://%s/%s/%s", bucketName, encodeNamespace(namespace), tableName)
	tableUUID := uuid.Nil
	if getResp.Metadata != nil && getResp.Metadata.Iceberg != nil && getResp.Metadata.Iceberg.TableUUID != "" {
		if parsed, err := uuid.Parse(getResp.Metadata.Iceberg.TableUUID); err == nil {
			tableUUID = parsed
		}
	}
	// Use Nil UUID if not found in storage (legacy table)
	// Stability is guaranteed by not generating random UUIDs on read

	var metadata table.Metadata
	if getResp.Metadata != nil && len(getResp.Metadata.FullMetadata) > 0 {
		var err error
		metadata, err = table.ParseMetadataBytes(getResp.Metadata.FullMetadata)
		if err != nil {
			glog.Warningf("Iceberg: Failed to parse persisted metadata for %s: %v", tableName, err)
			// Attempt to reconstruct from IcebergMetadata if available, otherwise synthetic
			// TODO: Extract schema/spec from getResp.Metadata.Iceberg if FullMetadata fails but partial info exists?
			// For now, fallback to empty metadata
			metadata = newTableMetadata(tableUUID, location, nil, nil, nil, nil)
		}
	} else {
		// No full metadata, create synthetic
		// TODO: If we had stored schema in IcebergMetadata, we would pass it here
		metadata = newTableMetadata(tableUUID, location, nil, nil, nil, nil)
	}

	result := LoadTableResult{
		MetadataLocation: getResp.MetadataLocation,
		Metadata:         metadata,
		Config:           make(iceberg.Properties),
	}
	writeJSON(w, http.StatusOK, result)
}

// handleTableExists checks if a table exists.
func (s *Server) handleTableExists(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	tableName := vars["table"]

	if len(namespace) == 0 || tableName == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	bucketName := getBucketFromPrefix(r)
	if !s.checkAuth(w, r, s3_constants.ACTION_READ, bucketName) {
		return
	}
	bucketARN := buildTableBucketARN(bucketName)

	getReq := &s3tables.GetTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
	}
	var getResp s3tables.GetTableResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", getReq, &getResp, "")
	})

	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleDropTable deletes a table.
func (s *Server) handleDropTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	tableName := vars["table"]

	if len(namespace) == 0 || tableName == "" {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace and table name are required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	if !s.checkAuth(w, r, s3_constants.ACTION_DELETE_BUCKET, bucketName) {
		return
	}
	bucketARN := buildTableBucketARN(bucketName)

	deleteReq := &s3tables.DeleteTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
	}

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "DeleteTable", deleteReq, nil, "")
	})

	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "NoSuchTableException", fmt.Sprintf("Table does not exist: %s", tableName))
			return
		}
		glog.V(1).Infof("Iceberg: DropTable error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleUpdateTable commits updates to a table.
// Implements the Iceberg REST Catalog commit protocol.
func (s *Server) handleUpdateTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	tableName := vars["table"]

	if len(namespace) == 0 || tableName == "" {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace and table name are required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	if !s.checkAuth(w, r, s3_constants.ACTION_WRITE, bucketName) {
		return
	}

	// Parse the commit request
	var req CommitTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid request body: "+err.Error())
		return
	}

	bucketARN := buildTableBucketARN(bucketName)

	// First, load current table metadata
	getReq := &s3tables.GetTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
	}
	var getResp s3tables.GetTableResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", getReq, &getResp, "")
	})

	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "NoSuchTableException", fmt.Sprintf("Table does not exist: %s", tableName))
			return
		}
		glog.V(1).Infof("Iceberg: CommitTable GetTable error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	// Build the current metadata
	location := fmt.Sprintf("s3://%s/%s/%s", bucketName, encodeNamespace(namespace), tableName)
	tableUUID := uuid.Nil
	if getResp.Metadata != nil && getResp.Metadata.Iceberg != nil && getResp.Metadata.Iceberg.TableUUID != "" {
		if parsed, err := uuid.Parse(getResp.Metadata.Iceberg.TableUUID); err == nil {
			tableUUID = parsed
		}
	}
	if tableUUID == uuid.Nil {
		tableUUID = uuid.New()
	}

	var currentMetadata table.Metadata
	if getResp.Metadata != nil && len(getResp.Metadata.FullMetadata) > 0 {
		var err error
		currentMetadata, err = table.ParseMetadataBytes(getResp.Metadata.FullMetadata)
		if err != nil {
			glog.Errorf("Iceberg: Failed to parse current metadata for %s: %v", tableName, err)
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to parse current metadata")
			return
		}
	} else {
		// Fallback for tables without persisted full metadata (legacy or error state)
		currentMetadata = newTableMetadata(tableUUID, location, nil, nil, nil, nil)
	}

	if currentMetadata == nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to build current metadata")
		return
	}

	// Validate all requirements against current metadata
	for _, requirement := range req.Requirements {
		if err := requirement.Validate(currentMetadata); err != nil {
			writeError(w, http.StatusConflict, "CommitFailedException", "Requirement failed: "+err.Error())
			return
		}
	}

	// Apply updates using MetadataBuilder
	builder, err := table.MetadataBuilderFromBase(currentMetadata, getResp.MetadataLocation)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to create metadata builder: "+err.Error())
		return
	}

	for _, update := range req.Updates {
		if err := update.Apply(builder); err != nil {
			writeError(w, http.StatusBadRequest, "BadRequestException", "Failed to apply update: "+err.Error())
			return
		}
	}

	// Build the new metadata
	newMetadata, err := builder.Build()
	if err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Failed to build new metadata: "+err.Error())
		return
	}

	// Determine next metadata version
	metadataVersion := getResp.MetadataVersion + 1
	metadataFileName := fmt.Sprintf("v%d.metadata.json", metadataVersion)
	newMetadataLocation := fmt.Sprintf("s3://%s/%s/%s/metadata/%s",
		bucketName, encodeNamespace(namespace), tableName, metadataFileName)

	// Serialize metadata to JSON
	metadataBytes, err := json.Marshal(newMetadata)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to serialize metadata: "+err.Error())
		return
	}

	// 1. Save metadata file to filer
	if err := s.saveMetadataFile(r.Context(), bucketName, encodeNamespace(namespace), tableName, metadataFileName, metadataBytes); err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to save metadata file: "+err.Error())
		return
	}

	// Persist the new metadata and update the table reference
	updateReq := &s3tables.UpdateTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
		VersionToken:   getResp.VersionToken,
		Metadata: &s3tables.TableMetadata{
			Iceberg: &s3tables.IcebergMetadata{
				TableUUID: tableUUID.String(),
			},
			FullMetadata: metadataBytes,
		},
		MetadataVersion:  metadataVersion,
		MetadataLocation: newMetadataLocation,
	}

	err = s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		// 1. Write metadata file (this would normally be an S3 PutObject,
		// but s3tables manager handles the metadata storage logic)
		// For now, we assume s3tables.UpdateTable handles the reference update.
		return s.tablesManager.Execute(r.Context(), mgrClient, "UpdateTable", updateReq, nil, "")
	})

	if err != nil {
		glog.Errorf("Iceberg: CommitTable UpdateTable error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to commit table update: "+err.Error())
		return
	}

	// Return the new metadata
	result := CommitTableResponse{
		MetadataLocation: newMetadataLocation,
		Metadata:         newMetadata,
	}
	writeJSON(w, http.StatusOK, result)
}

// loadTableResultJSON is used for JSON serialization of LoadTableResult.
// It wraps table.Metadata (which is an interface) for proper JSON output.
type loadTableResultJSON struct {
	MetadataLocation string             `json:"metadata-location,omitempty"`
	Metadata         table.Metadata     `json:"metadata"`
	Config           iceberg.Properties `json:"config,omitempty"`
}

// newTableMetadata creates a new table.Metadata object with the given parameters.
// Uses iceberg-go's MetadataBuilder pattern for proper spec compliance.
func newTableMetadata(
	tableUUID uuid.UUID,
	location string,
	schema *iceberg.Schema,
	partitionSpec *iceberg.PartitionSpec,
	sortOrder *table.SortOrder,
	props iceberg.Properties,
) table.Metadata {
	// Add schema - use provided or create empty schema
	var s *iceberg.Schema
	if schema != nil {
		s = schema
	} else {
		s = iceberg.NewSchema(0)
	}

	// Add partition spec
	var pSpec *iceberg.PartitionSpec
	if partitionSpec != nil {
		pSpec = partitionSpec
	} else {
		unpartitioned := iceberg.NewPartitionSpecID(0)
		pSpec = &unpartitioned
	}

	// Add sort order
	var so table.SortOrder
	if sortOrder != nil {
		so = *sortOrder
	} else {
		so = table.UnsortedSortOrder
	}

	// Create properties map if nil
	if props == nil {
		props = make(iceberg.Properties)
	}

	// Create metadata directly using the constructor which ensures spec compliance for V2
	metadata, err := table.NewMetadataWithUUID(s, pSpec, so, location, props, tableUUID)
	if err != nil {
		glog.Errorf("Failed to create metadata: %v", err)
		return nil
	}

	return metadata
}
