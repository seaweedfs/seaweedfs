// Package iceberg provides Iceberg REST Catalog API support.
// It implements the Apache Iceberg REST Catalog specification
// backed by S3 Tables metadata storage.
package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

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
	// Add middleware to log all requests/responses
	router.Use(loggingMiddleware)

	// Configuration endpoint - no auth needed for config
	router.HandleFunc("/v1/config", s.handleConfig).Methods(http.MethodGet)

	// Namespace endpoints - wrapped with Auth middleware
	router.HandleFunc("/v1/namespaces", s.Auth(s.handleListNamespaces)).Methods(http.MethodGet)
	router.HandleFunc("/v1/namespaces", s.Auth(s.handleCreateNamespace)).Methods(http.MethodPost)
	router.HandleFunc("/v1/namespaces/{namespace}", s.Auth(s.handleGetNamespace)).Methods(http.MethodGet)
	router.HandleFunc("/v1/namespaces/{namespace}", s.Auth(s.handleNamespaceExists)).Methods(http.MethodHead)
	router.HandleFunc("/v1/namespaces/{namespace}", s.Auth(s.handleDropNamespace)).Methods(http.MethodDelete)

	// Table endpoints - wrapped with Auth middleware
	router.HandleFunc("/v1/namespaces/{namespace}/tables", s.Auth(s.handleListTables)).Methods(http.MethodGet)
	router.HandleFunc("/v1/namespaces/{namespace}/tables", s.Auth(s.handleCreateTable)).Methods(http.MethodPost)
	router.HandleFunc("/v1/namespaces/{namespace}/tables/{table}", s.Auth(s.handleLoadTable)).Methods(http.MethodGet)
	router.HandleFunc("/v1/namespaces/{namespace}/tables/{table}", s.Auth(s.handleTableExists)).Methods(http.MethodHead)
	router.HandleFunc("/v1/namespaces/{namespace}/tables/{table}", s.Auth(s.handleDropTable)).Methods(http.MethodDelete)
	router.HandleFunc("/v1/namespaces/{namespace}/tables/{table}", s.Auth(s.handleUpdateTable)).Methods(http.MethodPost)

	// With prefix support - wrapped with Auth middleware
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

	// Catch-all for debugging
	router.PathPrefix("/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		glog.V(2).Infof("Catch-all route hit: %s %s", r.Method, r.RequestURI)
		writeError(w, http.StatusNotFound, "NotFound", "Path not found")
	})

	glog.V(2).Infof("Registered Iceberg REST Catalog routes")
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		glog.V(2).Infof("Iceberg REST request: %s %s from %s", r.Method, r.RequestURI, r.RemoteAddr)

		// Log all headers for debugging
		glog.V(2).Infof("Iceberg REST headers:")
		for name, values := range r.Header {
			for _, value := range values {
				// Redact sensitive headers
				if name == "Authorization" && len(value) > 20 {
					glog.V(2).Infof("  %s: %s...%s", name, value[:20], value[len(value)-10:])
				} else {
					glog.V(2).Infof("  %s: %s", name, value)
				}
			}
		}

		// Create a response writer that captures the status code
		wrapped := &responseWriter{ResponseWriter: w}
		next.ServeHTTP(wrapped, r)

		glog.V(2).Infof("Iceberg REST response: %s %s -> %d", r.Method, r.RequestURI, wrapped.statusCode)
	})
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *responseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
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
// It constructs the filer path from the S3 location components.
func (s *Server) saveMetadataFile(ctx context.Context, bucketName, tablePath, metadataFileName string, content []byte) error {

	// Create context with timeout for file operations
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		bucketsPath := s3tables.TablesPath

		ensureDir := func(parent, name, errorContext string) error {
			_, err := filer_pb.LookupEntry(opCtx, client, &filer_pb.LookupDirectoryEntryRequest{
				Directory: parent,
				Name:      name,
			})
			if err == nil {
				return nil
			}
			if err != filer_pb.ErrNotFound {
				return fmt.Errorf("lookup %s failed: %w", errorContext, err)
			}

			// If lookup fails with ErrNotFound, try to create the directory.
			resp, createErr := client.CreateEntry(opCtx, &filer_pb.CreateEntryRequest{
				Directory: parent,
				Entry: &filer_pb.Entry{
					Name:        name,
					IsDirectory: true,
					Attributes: &filer_pb.FuseAttributes{
						Mtime:    time.Now().Unix(),
						Crtime:   time.Now().Unix(),
						FileMode: uint32(0755 | os.ModeDir),
					},
				},
			})
			if createErr != nil {
				return fmt.Errorf("failed to create %s: %w", errorContext, createErr)
			}
			if resp.Error != "" && !strings.Contains(resp.Error, "exist") {
				return fmt.Errorf("failed to create %s: %s", errorContext, resp.Error)
			}
			return nil
		}

		bucketDir := path.Join(bucketsPath, bucketName)
		// 1. Ensure bucket directory exists: <bucketsPath>/<bucket>
		if err := ensureDir(bucketsPath, bucketName, "bucket directory"); err != nil {
			return err
		}

		// 2. Ensure table path exists under the bucket directory
		tableDir := bucketDir
		if tablePath != "" {
			segments := strings.Split(tablePath, "/")
			for _, segment := range segments {
				if segment == "" {
					continue
				}
				if err := ensureDir(tableDir, segment, "table directory"); err != nil {
					return err
				}
				tableDir = path.Join(tableDir, segment)
			}
		}

		// 3. Ensure metadata directory exists: <bucketsPath>/<bucket>/<tablePath>/metadata
		metadataDir := path.Join(tableDir, "metadata")
		if err := ensureDir(tableDir, "metadata", "metadata directory"); err != nil {
			return err
		}

		// 4. Write the file
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
			return fmt.Errorf("failed to write metadata file: %w", err)
		}
		if resp.Error != "" {
			return fmt.Errorf("failed to write metadata file: %s", resp.Error)
		}
		return nil
	})
}

func (s *Server) deleteMetadataFile(ctx context.Context, bucketName, tablePath, metadataFileName string) error {
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	metadataDir := path.Join(s3tables.TablesPath, bucketName)
	if tablePath != "" {
		metadataDir = path.Join(metadataDir, tablePath)
	}
	metadataDir = path.Join(metadataDir, "metadata")
	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.DoRemove(opCtx, client, metadataDir, metadataFileName, true, false, true, false, nil)
	})
}

type statisticsUpdate struct {
	set    *table.StatisticsFile
	remove *int64
}

var ErrIncompleteSetStatistics = errors.New("set-statistics requires snapshot-id, statistics-path, file-size-in-bytes, and file-footer-size-in-bytes")

type commitAction struct {
	Action string `json:"action"`
}

type setStatisticsUpdate struct {
	Action                string                `json:"action"`
	SnapshotID            *int64                `json:"snapshot-id,omitempty"`
	StatisticsPath        string                `json:"statistics-path,omitempty"`
	FileSizeInBytes       *int64                `json:"file-size-in-bytes,omitempty"`
	FileFooterSizeInBytes *int64                `json:"file-footer-size-in-bytes,omitempty"`
	KeyMetadata           *string               `json:"key-metadata,omitempty"`
	BlobMetadata          []table.BlobMetadata  `json:"blob-metadata,omitempty"`
	Statistics            *table.StatisticsFile `json:"statistics,omitempty"`
}

func (u *setStatisticsUpdate) asStatisticsFile() (*table.StatisticsFile, error) {
	if u.Statistics != nil {
		if u.Statistics.BlobMetadata == nil {
			u.Statistics.BlobMetadata = []table.BlobMetadata{}
		}
		return u.Statistics, nil
	}
	if u.SnapshotID == nil || u.StatisticsPath == "" || u.FileSizeInBytes == nil || u.FileFooterSizeInBytes == nil {
		return nil, ErrIncompleteSetStatistics
	}

	stats := &table.StatisticsFile{
		SnapshotID:            *u.SnapshotID,
		StatisticsPath:        u.StatisticsPath,
		FileSizeInBytes:       *u.FileSizeInBytes,
		FileFooterSizeInBytes: *u.FileFooterSizeInBytes,
		KeyMetadata:           u.KeyMetadata,
		BlobMetadata:          u.BlobMetadata,
	}
	if stats.BlobMetadata == nil {
		stats.BlobMetadata = []table.BlobMetadata{}
	}
	return stats, nil
}

type removeStatisticsUpdate struct {
	Action     string `json:"action"`
	SnapshotID int64  `json:"snapshot-id"`
}

func parseCommitUpdates(rawUpdates []json.RawMessage) (table.Updates, []statisticsUpdate, error) {
	filtered := make([]json.RawMessage, 0, len(rawUpdates))
	statisticsUpdates := make([]statisticsUpdate, 0)

	for _, raw := range rawUpdates {
		var action commitAction
		if err := json.Unmarshal(raw, &action); err != nil {
			return nil, nil, err
		}

		switch action.Action {
		case "set-statistics":
			var setUpdate setStatisticsUpdate
			if err := json.Unmarshal(raw, &setUpdate); err != nil {
				return nil, nil, err
			}
			stats, err := setUpdate.asStatisticsFile()
			if err != nil {
				return nil, nil, err
			}
			statisticsUpdates = append(statisticsUpdates, statisticsUpdate{set: stats})
		case "remove-statistics":
			var removeUpdate removeStatisticsUpdate
			if err := json.Unmarshal(raw, &removeUpdate); err != nil {
				return nil, nil, err
			}
			snapshotID := removeUpdate.SnapshotID
			statisticsUpdates = append(statisticsUpdates, statisticsUpdate{remove: &snapshotID})
		default:
			filtered = append(filtered, raw)
		}
	}

	if len(filtered) == 0 {
		return nil, statisticsUpdates, nil
	}

	data, err := json.Marshal(filtered)
	if err != nil {
		return nil, nil, err
	}
	var updates table.Updates
	if err := json.Unmarshal(data, &updates); err != nil {
		return nil, nil, err
	}

	return updates, statisticsUpdates, nil
}

func applyStatisticsUpdates(metadataBytes []byte, updates []statisticsUpdate) ([]byte, error) {
	if len(updates) == 0 {
		return metadataBytes, nil
	}

	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return nil, err
	}

	var statistics []table.StatisticsFile
	if rawStatistics, ok := metadata["statistics"]; ok && len(rawStatistics) > 0 {
		if err := json.Unmarshal(rawStatistics, &statistics); err != nil {
			return nil, err
		}
	}

	statisticsBySnapshot := make(map[int64]table.StatisticsFile, len(statistics))
	orderedSnapshotIDs := make([]int64, 0, len(statistics))
	inOrder := make(map[int64]bool, len(statistics))
	for _, stat := range statistics {
		statisticsBySnapshot[stat.SnapshotID] = stat
		if !inOrder[stat.SnapshotID] {
			orderedSnapshotIDs = append(orderedSnapshotIDs, stat.SnapshotID)
			inOrder[stat.SnapshotID] = true
		}
	}

	for _, update := range updates {
		if update.set != nil {
			statisticsBySnapshot[update.set.SnapshotID] = *update.set
			if !inOrder[update.set.SnapshotID] {
				orderedSnapshotIDs = append(orderedSnapshotIDs, update.set.SnapshotID)
				inOrder[update.set.SnapshotID] = true
			}
			continue
		}
		if update.remove != nil {
			delete(statisticsBySnapshot, *update.remove)
		}
	}

	statistics = make([]table.StatisticsFile, 0, len(statisticsBySnapshot))
	for _, snapshotID := range orderedSnapshotIDs {
		stat, ok := statisticsBySnapshot[snapshotID]
		if !ok {
			continue
		}
		statistics = append(statistics, stat)
	}

	if len(statistics) == 0 {
		delete(metadata, "statistics")
	} else {
		data, err := json.Marshal(statistics)
		if err != nil {
			return nil, err
		}
		metadata["statistics"] = data
	}

	return json.Marshal(metadata)
}

func isS3TablesConflict(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, s3tables.ErrVersionTokenMismatch) {
		return true
	}
	var tableErr *s3tables.S3TablesError
	return errors.As(err, &tableErr) && tableErr.Type == s3tables.ErrCodeConflict
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

func parseS3Location(location string) (bucketName, tablePath string, err error) {
	if !strings.HasPrefix(location, "s3://") {
		return "", "", fmt.Errorf("unsupported location: %s", location)
	}
	trimmed := strings.TrimPrefix(location, "s3://")
	trimmed = strings.TrimSuffix(trimmed, "/")
	if trimmed == "" {
		return "", "", fmt.Errorf("invalid location: %s", location)
	}
	parts := strings.SplitN(trimmed, "/", 2)
	bucketName = parts[0]
	if bucketName == "" {
		return "", "", fmt.Errorf("invalid location bucket: %s", location)
	}
	if len(parts) == 2 {
		tablePath = parts[1]
	}
	return bucketName, tablePath, nil
}

func tableLocationFromMetadataLocation(metadataLocation string) string {
	trimmed := strings.TrimSuffix(metadataLocation, "/")
	if idx := strings.LastIndex(trimmed, "/metadata/"); idx != -1 {
		return trimmed[:idx]
	}
	return trimmed
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
	if bucket := os.Getenv("S3TABLES_DEFAULT_BUCKET"); bucket != "" {
		return bucket
	}
	// Default bucket if no prefix - use "warehouse" for Iceberg
	return "warehouse"
}

// buildTableBucketARN builds an ARN for a table bucket.
func buildTableBucketARN(bucketName string) string {
	arn, _ := s3tables.BuildBucketARN(s3tables.DefaultRegion, s3_constants.AccountAdminId, bucketName)
	return arn
}

const (
	defaultListPageSize = 1000
	maxListPageSize     = 1000
)

var errStageCreateUnsupported = errors.New("stage-create is not supported")

func getPaginationQueryParam(r *http.Request, primary, fallback string) string {
	if v := strings.TrimSpace(r.URL.Query().Get(primary)); v != "" {
		return v
	}
	return strings.TrimSpace(r.URL.Query().Get(fallback))
}

func parsePagination(r *http.Request) (pageToken string, pageSize int, err error) {
	pageToken = getPaginationQueryParam(r, "pageToken", "page-token")
	pageSize = defaultListPageSize

	pageSizeValue := getPaginationQueryParam(r, "pageSize", "page-size")
	if pageSizeValue == "" {
		return pageToken, pageSize, nil
	}

	parsedPageSize, parseErr := strconv.Atoi(pageSizeValue)
	if parseErr != nil || parsedPageSize <= 0 {
		return pageToken, 0, fmt.Errorf("invalid pageSize %q: must be a positive integer", pageSizeValue)
	}
	if parsedPageSize > maxListPageSize {
		return pageToken, 0, fmt.Errorf("invalid pageSize %q: must be <= %d", pageSizeValue, maxListPageSize)
	}

	return pageToken, parsedPageSize, nil
}

func normalizeNamespaceProperties(properties map[string]string) map[string]string {
	if properties == nil {
		return map[string]string{}
	}
	return properties
}

func validateCreateTableRequest(req CreateTableRequest) error {
	if req.Name == "" {
		return errors.New("table name is required")
	}
	if req.StageCreate {
		return errStageCreateUnsupported
	}
	return nil
}

// handleConfig returns catalog configuration.
func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	config := CatalogConfig{
		Defaults:  map[string]string{},
		Overrides: map[string]string{},
	}
	if err := json.NewEncoder(w).Encode(config); err != nil {
		glog.Warningf("handleConfig: Failed to encode config: %v", err)
	}
}

// handleListNamespaces lists namespaces in a catalog.
func (s *Server) handleListNamespaces(w http.ResponseWriter, r *http.Request) {
	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	pageToken, pageSize, err := parsePagination(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", err.Error())
		return
	}

	// Use S3 Tables manager to list namespaces
	var resp s3tables.ListNamespacesResponse
	req := &s3tables.ListNamespacesRequest{
		TableBucketARN:    bucketARN,
		ContinuationToken: pageToken,
		MaxNamespaces:     pageSize,
	}

	err = s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "ListNamespaces", req, &resp, identityName)
	})

	if err != nil {
		glog.Infof("Iceberg: ListNamespaces error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	// Convert to Iceberg format
	namespaces := make([]Namespace, 0, len(resp.Namespaces))
	for _, ns := range resp.Namespaces {
		namespaces = append(namespaces, Namespace(ns.Namespace))
	}

	result := ListNamespacesResponse{
		NextPageToken: resp.ContinuationToken,
		Namespaces:    namespaces,
	}
	writeJSON(w, http.StatusOK, result)
}

// handleCreateNamespace creates a new namespace.
func (s *Server) handleCreateNamespace(w http.ResponseWriter, r *http.Request) {
	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

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
		Properties:     normalizeNamespaceProperties(req.Properties),
	}
	var createResp s3tables.CreateNamespaceResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		glog.V(2).Infof("Iceberg: handleCreateNamespace calling Execute with identityName=%s", identityName)
		return s.tablesManager.Execute(r.Context(), mgrClient, "CreateNamespace", createReq, &createResp, identityName)
	})

	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			writeError(w, http.StatusConflict, "AlreadyExistsException", err.Error())
			return
		}
		glog.Errorf("Iceberg: CreateNamespace error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	result := CreateNamespaceResponse{
		Namespace:  Namespace(createResp.Namespace),
		Properties: normalizeNamespaceProperties(createResp.Properties),
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
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	// Use S3 Tables manager to get namespace
	getReq := &s3tables.GetNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
	}
	var getResp s3tables.GetNamespaceResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetNamespace", getReq, &getResp, identityName)
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
		Namespace:  Namespace(getResp.Namespace),
		Properties: normalizeNamespaceProperties(getResp.Properties),
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
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	getReq := &s3tables.GetNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
	}
	var getResp s3tables.GetNamespaceResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetNamespace", getReq, &getResp, identityName)
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
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	deleteReq := &s3tables.DeleteNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
	}

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "DeleteNamespace", deleteReq, nil, identityName)
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
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	pageToken, pageSize, err := parsePagination(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", err.Error())
		return
	}

	listReq := &s3tables.ListTablesRequest{
		TableBucketARN:    bucketARN,
		Namespace:         namespace,
		ContinuationToken: pageToken,
		MaxTables:         pageSize,
	}
	var listResp s3tables.ListTablesResponse

	err = s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "ListTables", listReq, &listResp, identityName)
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
		NextPageToken: listResp.ContinuationToken,
		Identifiers:   identifiers,
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

	if err := validateCreateTableRequest(req); err != nil {
		if errors.Is(err, errStageCreateUnsupported) {
			writeError(w, http.StatusNotImplemented, "NotImplementedException", "stage-create is not supported; submit create without stage-create")
			return
		}
		writeError(w, http.StatusBadRequest, "BadRequestException", err.Error())
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	// Generate UUID for the new table
	tableUUID := uuid.New()
	tablePath := path.Join(encodeNamespace(namespace), req.Name)
	location := strings.TrimSuffix(req.Location, "/")
	if location == "" {
		if req.Properties != nil {
			if warehouse := strings.TrimSuffix(req.Properties["warehouse"], "/"); warehouse != "" {
				location = fmt.Sprintf("%s/%s", warehouse, tablePath)
			}
		}
		if location == "" {
			if warehouse := strings.TrimSuffix(os.Getenv("ICEBERG_WAREHOUSE"), "/"); warehouse != "" {
				location = fmt.Sprintf("%s/%s", warehouse, tablePath)
			}
		}
		if location == "" {
			location = fmt.Sprintf("s3://%s/%s", bucketName, tablePath)
		}
	} else {
		parsedBucket, parsedPath, err := parseS3Location(location)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid table location: "+err.Error())
			return
		}
		if parsedPath == "" {
			location = fmt.Sprintf("s3://%s/%s", parsedBucket, tablePath)
		}
	}

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

	tableName := req.Name
	metadataFileName := "v1.metadata.json" // Initial version is always 1
	metadataLocation := fmt.Sprintf("%s/metadata/%s", location, metadataFileName)
	if !req.StageCreate {
		// Save metadata file to filer for immediate table creation.
		metadataBucket, metadataPath, err := parseS3Location(location)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Invalid table location: "+err.Error())
			return
		}
		if err := s.saveMetadataFile(r.Context(), metadataBucket, metadataPath, metadataFileName, metadataBytes); err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to save metadata file: "+err.Error())
			return
		}
	}

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
		return s.tablesManager.Execute(r.Context(), mgrClient, "CreateTable", createReq, &createResp, identityName)
	})

	if err != nil {
		if tableErr, ok := err.(*s3tables.S3TablesError); ok && tableErr.Type == s3tables.ErrCodeTableAlreadyExists {
			getReq := &s3tables.GetTableRequest{
				TableBucketARN: bucketARN,
				Namespace:      namespace,
				Name:           tableName,
			}
			var getResp s3tables.GetTableResponse
			getErr := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
				mgrClient := s3tables.NewManagerClient(client)
				return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", getReq, &getResp, identityName)
			})
			if getErr != nil {
				writeError(w, http.StatusConflict, "AlreadyExistsException", err.Error())
				return
			}
			result := buildLoadTableResult(getResp, bucketName, namespace, tableName)
			writeJSON(w, http.StatusOK, result)
			return
		}
		if strings.Contains(err.Error(), "already exists") {
			getReq := &s3tables.GetTableRequest{
				TableBucketARN: bucketARN,
				Namespace:      namespace,
				Name:           tableName,
			}
			var getResp s3tables.GetTableResponse
			getErr := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
				mgrClient := s3tables.NewManagerClient(client)
				return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", getReq, &getResp, identityName)
			})
			if getErr != nil {
				writeError(w, http.StatusConflict, "AlreadyExistsException", err.Error())
				return
			}
			result := buildLoadTableResult(getResp, bucketName, namespace, tableName)
			writeJSON(w, http.StatusOK, result)
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
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	getReq := &s3tables.GetTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
	}
	var getResp s3tables.GetTableResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", getReq, &getResp, identityName)
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

	result := buildLoadTableResult(getResp, bucketName, namespace, tableName)
	writeJSON(w, http.StatusOK, result)
}

func buildLoadTableResult(getResp s3tables.GetTableResponse, bucketName string, namespace []string, tableName string) LoadTableResult {
	location := tableLocationFromMetadataLocation(getResp.MetadataLocation)
	if location == "" {
		location = fmt.Sprintf("s3://%s/%s/%s", bucketName, encodeNamespace(namespace), tableName)
	}
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

	return LoadTableResult{
		MetadataLocation: getResp.MetadataLocation,
		Metadata:         metadata,
		Config:           make(iceberg.Properties),
	}
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
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	getReq := &s3tables.GetTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
	}
	var getResp s3tables.GetTableResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", getReq, &getResp, identityName)
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
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	deleteReq := &s3tables.DeleteTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
	}

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "DeleteTable", deleteReq, nil, identityName)
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
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	// Parse commit request and keep statistics updates separate because iceberg-go v0.4.0
	// does not decode set/remove-statistics update actions yet.
	var raw struct {
		Identifier   *TableIdentifier  `json:"identifier,omitempty"`
		Requirements json.RawMessage   `json:"requirements"`
		Updates      []json.RawMessage `json:"updates"`
	}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid request body: "+err.Error())
		return
	}

	var req CommitTableRequest
	req.Identifier = raw.Identifier
	var statisticsUpdates []statisticsUpdate
	if len(raw.Requirements) > 0 {
		if err := json.Unmarshal(raw.Requirements, &req.Requirements); err != nil {
			writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid requirements: "+err.Error())
			return
		}
	}
	if len(raw.Updates) > 0 {
		var err error
		req.Updates, statisticsUpdates, err = parseCommitUpdates(raw.Updates)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid updates: "+err.Error())
			return
		}
	}

	maxCommitAttempts := 3
	generatedLegacyUUID := uuid.New()
	for attempt := 1; attempt <= maxCommitAttempts; attempt++ {
		getReq := &s3tables.GetTableRequest{
			TableBucketARN: bucketARN,
			Namespace:      namespace,
			Name:           tableName,
		}
		var getResp s3tables.GetTableResponse

		err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			mgrClient := s3tables.NewManagerClient(client)
			return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", getReq, &getResp, identityName)
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

		location := tableLocationFromMetadataLocation(getResp.MetadataLocation)
		if location == "" {
			location = fmt.Sprintf("s3://%s/%s/%s", bucketName, encodeNamespace(namespace), tableName)
		}
		tableUUID := uuid.Nil
		if getResp.Metadata != nil && getResp.Metadata.Iceberg != nil && getResp.Metadata.Iceberg.TableUUID != "" {
			if parsed, parseErr := uuid.Parse(getResp.Metadata.Iceberg.TableUUID); parseErr == nil {
				tableUUID = parsed
			}
		}
		if tableUUID == uuid.Nil {
			tableUUID = generatedLegacyUUID
		}

		var currentMetadata table.Metadata
		if getResp.Metadata != nil && len(getResp.Metadata.FullMetadata) > 0 {
			currentMetadata, err = table.ParseMetadataBytes(getResp.Metadata.FullMetadata)
			if err != nil {
				glog.Errorf("Iceberg: Failed to parse current metadata for %s: %v", tableName, err)
				writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to parse current metadata")
				return
			}
		} else {
			currentMetadata = newTableMetadata(tableUUID, location, nil, nil, nil, nil)
		}
		if currentMetadata == nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to build current metadata")
			return
		}

		for _, requirement := range req.Requirements {
			if err := requirement.Validate(currentMetadata); err != nil {
				writeError(w, http.StatusConflict, "CommitFailedException", "Requirement failed: "+err.Error())
				return
			}
		}

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

		newMetadata, err := builder.Build()
		if err != nil {
			writeError(w, http.StatusBadRequest, "BadRequestException", "Failed to build new metadata: "+err.Error())
			return
		}

		metadataVersion := getResp.MetadataVersion + 1
		metadataFileName := fmt.Sprintf("v%d.metadata.json", metadataVersion)
		newMetadataLocation := fmt.Sprintf("%s/metadata/%s", strings.TrimSuffix(location, "/"), metadataFileName)

		metadataBytes, err := json.Marshal(newMetadata)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to serialize metadata: "+err.Error())
			return
		}
		// iceberg-go does not currently support set/remove-statistics updates in MetadataBuilder.
		// Patch the encoded metadata JSON and parse it back to keep the response object consistent.
		metadataBytes, err = applyStatisticsUpdates(metadataBytes, statisticsUpdates)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BadRequestException", "Failed to apply statistics updates: "+err.Error())
			return
		}
		newMetadata, err = table.ParseMetadataBytes(metadataBytes)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to parse committed metadata: "+err.Error())
			return
		}

		metadataBucket, metadataPath, err := parseS3Location(location)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Invalid table location: "+err.Error())
			return
		}
		if err := s.saveMetadataFile(r.Context(), metadataBucket, metadataPath, metadataFileName, metadataBytes); err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to save metadata file: "+err.Error())
			return
		}

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
			return s.tablesManager.Execute(r.Context(), mgrClient, "UpdateTable", updateReq, nil, identityName)
		})
		if err == nil {
			result := CommitTableResponse{
				MetadataLocation: newMetadataLocation,
				Metadata:         newMetadata,
			}
			writeJSON(w, http.StatusOK, result)
			return
		}

		if isS3TablesConflict(err) {
			if cleanupErr := s.deleteMetadataFile(r.Context(), metadataBucket, metadataPath, metadataFileName); cleanupErr != nil {
				glog.V(1).Infof("Iceberg: failed to cleanup metadata file %s on conflict: %v", newMetadataLocation, cleanupErr)
			}
			if attempt < maxCommitAttempts {
				glog.V(1).Infof("Iceberg: CommitTable conflict for %s (attempt %d/%d), retrying", tableName, attempt, maxCommitAttempts)
				jitter := time.Duration(rand.Int64N(int64(25 * time.Millisecond)))
				time.Sleep(time.Duration(50*attempt)*time.Millisecond + jitter)
				continue
			}
			writeError(w, http.StatusConflict, "CommitFailedException", "Version token mismatch")
			return
		}

		if cleanupErr := s.deleteMetadataFile(r.Context(), metadataBucket, metadataPath, metadataFileName); cleanupErr != nil {
			glog.V(1).Infof("Iceberg: failed to cleanup metadata file %s after update failure: %v", newMetadataLocation, cleanupErr)
		}
		glog.Errorf("Iceberg: CommitTable UpdateTable error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to commit table update: "+err.Error())
		return
	}
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
