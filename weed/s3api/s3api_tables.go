package s3api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/gorilla/mux"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// S3TablesApiServer wraps the S3 Tables handler with S3ApiServer's filer access
type S3TablesApiServer struct {
	s3a     *S3ApiServer
	handler *s3tables.S3TablesHandler
}

// NewS3TablesApiServer creates a new S3 Tables API server
func NewS3TablesApiServer(s3a *S3ApiServer) *S3TablesApiServer {
	return &S3TablesApiServer{
		s3a:     s3a,
		handler: s3tables.NewS3TablesHandler(),
	}
}

// SetRegion sets the AWS region for ARN generation
func (st *S3TablesApiServer) SetRegion(region string) {
	st.handler.SetRegion(region)
}

// SetAccountID sets the AWS account ID for ARN generation
func (st *S3TablesApiServer) SetAccountID(accountID string) {
	st.handler.SetAccountID(accountID)
}

// SetDefaultAllow sets whether to allow access by default
func (st *S3TablesApiServer) SetDefaultAllow(allow bool) {
	st.handler.SetDefaultAllow(allow)
}

// S3TablesHandler handles S3 Tables API requests
func (st *S3TablesApiServer) S3TablesHandler(w http.ResponseWriter, r *http.Request) {
	st.handler.HandleRequest(w, r, st)
}

// WithFilerClient implements the s3tables.FilerClient interface
func (st *S3TablesApiServer) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return st.s3a.WithFilerClient(streamingMode, fn)
}

// registerS3TablesRoutes registers S3 Tables API routes
func (s3a *S3ApiServer) registerS3TablesRoutes(router *mux.Router) {
	// Create S3 Tables handler
	s3TablesApi := NewS3TablesApiServer(s3a)
	if s3a.iam != nil && s3a.iam.iamIntegration != nil {
		s3TablesApi.SetDefaultAllow(s3a.iam.iamIntegration.DefaultAllow())
	}

	// Regex for S3 Tables Bucket ARN
	const tableBucketARNRegex = "arn:aws:s3tables:[^/:]*:[^/:]*:bucket/[^/]+"

	// REST-style S3 Tables API routes (used by AWS CLI)
	targetMatcher := func(r *http.Request, rm *mux.RouteMatch) bool {
		return strings.HasPrefix(r.Header.Get("X-Amz-Target"), "S3Tables.")
	}
	router.Methods(http.MethodPost).Path("/").MatcherFunc(targetMatcher).
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.S3TablesHandler), "S3Tables-Target"))
	router.Methods(http.MethodPut).Path("/buckets").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("CreateTableBucket", buildCreateTableBucketRequest)), "S3Tables-CreateTableBucket"))
	router.Methods(http.MethodGet).Path("/buckets").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("ListTableBuckets", buildListTableBucketsRequest)), "S3Tables-ListTableBuckets"))
	router.Methods(http.MethodGet).Path("/buckets/{tableBucketARN:" + tableBucketARNRegex + "}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("GetTableBucket", buildTableBucketArnRequest)), "S3Tables-GetTableBucket"))
	router.Methods(http.MethodDelete).Path("/buckets/{tableBucketARN:" + tableBucketARNRegex + "}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("DeleteTableBucket", buildDeleteTableBucketRequest)), "S3Tables-DeleteTableBucket"))
	router.Methods(http.MethodPut).Path("/buckets/{tableBucketARN:" + tableBucketARNRegex + "}/policy").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("PutTableBucketPolicy", buildPutTableBucketPolicyRequest)), "S3Tables-PutTableBucketPolicy"))
	router.Methods(http.MethodGet).Path("/buckets/{tableBucketARN:" + tableBucketARNRegex + "}/policy").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("GetTableBucketPolicy", buildGetTableBucketPolicyRequest)), "S3Tables-GetTableBucketPolicy"))
	router.Methods(http.MethodDelete).Path("/buckets/{tableBucketARN:" + tableBucketARNRegex + "}/policy").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("DeleteTableBucketPolicy", buildDeleteTableBucketPolicyRequest)), "S3Tables-DeleteTableBucketPolicy"))

	router.Methods(http.MethodPut).Path("/namespaces/{tableBucketARN:" + tableBucketARNRegex + "}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("CreateNamespace", buildCreateNamespaceRequest)), "S3Tables-CreateNamespace"))
	router.Methods(http.MethodGet).Path("/namespaces/{tableBucketARN:" + tableBucketARNRegex + "}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("ListNamespaces", buildListNamespacesRequest)), "S3Tables-ListNamespaces"))
	router.Methods(http.MethodGet).Path("/namespaces/{tableBucketARN:" + tableBucketARNRegex + "}/{namespace}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("GetNamespace", buildGetNamespaceRequest)), "S3Tables-GetNamespace"))
	router.Methods(http.MethodDelete).Path("/namespaces/{tableBucketARN:" + tableBucketARNRegex + "}/{namespace}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("DeleteNamespace", buildDeleteNamespaceRequest)), "S3Tables-DeleteNamespace"))

	router.Methods(http.MethodPut).Path("/tables/{tableBucketARN:" + tableBucketARNRegex + "}/{namespace}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("CreateTable", buildCreateTableRequest)), "S3Tables-CreateTable"))
	router.Methods(http.MethodGet).Path("/tables/{tableBucketARN:" + tableBucketARNRegex + "}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("ListTables", buildListTablesRequest)), "S3Tables-ListTables"))
	router.Methods(http.MethodDelete).Path("/tables/{tableBucketARN:" + tableBucketARNRegex + "}/{namespace}/{name}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("DeleteTable", buildDeleteTableRequest)), "S3Tables-DeleteTable"))

	router.Methods(http.MethodPut).Path("/tables/{tableBucketARN:" + tableBucketARNRegex + "}/{namespace}/{name}/policy").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("PutTablePolicy", buildPutTablePolicyRequest)), "S3Tables-PutTablePolicy"))
	router.Methods(http.MethodGet).Path("/tables/{tableBucketARN:" + tableBucketARNRegex + "}/{namespace}/{name}/policy").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("GetTablePolicy", buildGetTablePolicyRequest)), "S3Tables-GetTablePolicy"))
	router.Methods(http.MethodDelete).Path("/tables/{tableBucketARN:" + tableBucketARNRegex + "}/{namespace}/{name}/policy").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("DeleteTablePolicy", buildDeleteTablePolicyRequest)), "S3Tables-DeleteTablePolicy"))

	router.Methods(http.MethodPost).Path("/tag/{resourceArn:arn:aws:s3tables:.*}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("TagResource", buildTagResourceRequest)), "S3Tables-TagResource"))
	router.Methods(http.MethodGet).Path("/tag/{resourceArn:arn:aws:s3tables:.*}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("ListTagsForResource", buildListTagsForResourceRequest)), "S3Tables-ListTagsForResource"))
	router.Methods(http.MethodDelete).Path("/tag/{resourceArn:arn:aws:s3tables:.*}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("UntagResource", buildUntagResourceRequest)), "S3Tables-UntagResource"))

	router.Methods(http.MethodGet).Path("/get-table").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("GetTable", buildGetTableRequest)), "S3Tables-GetTable"))

	glog.V(1).Infof("S3 Tables API enabled")
}

type s3tablesRequestBuilder func(r *http.Request) (interface{}, error)

func (st *S3TablesApiServer) handleRestOperation(operation string, builder s3tablesRequestBuilder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		payload, err := builder(r)
		if err != nil {
			writeS3TablesError(w, http.StatusBadRequest, s3tables.ErrCodeInvalidRequest, err.Error())
			return
		}
		if err := setS3TablesRequestBody(r, payload); err != nil {
			writeS3TablesError(w, http.StatusInternalServerError, s3tables.ErrCodeInternalError, err.Error())
			return
		}
		r.Header.Set("X-Amz-Target", "S3Tables."+operation)
		st.S3TablesHandler(w, r)
	}
}

func setS3TablesRequestBody(r *http.Request, payload interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	r.Header.Set("Content-Type", "application/x-amz-json-1.1")
	return nil
}

func readS3TablesJSONBody(r *http.Request, v interface{}) error {
	if r.Body == nil {
		return nil
	}
	defer r.Body.Close()
	const maxRequestBodySize = 10 * 1024 * 1024
	if r.ContentLength > maxRequestBodySize {
		return fmt.Errorf("request body too large: exceeds maximum size of %d bytes", maxRequestBodySize)
	}
	limitedReader := io.LimitReader(r.Body, maxRequestBodySize+1)
	body, err := io.ReadAll(limitedReader)
	if err != nil {
		return err
	}
	if len(body) > maxRequestBodySize {
		return fmt.Errorf("request body too large: exceeds maximum size of %d bytes", maxRequestBodySize)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	return json.Unmarshal(body, v)
}

func writeS3TablesError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(status)
	errorResponse := map[string]interface{}{
		"__type":  code,
		"message": message,
	}
	if err := json.NewEncoder(w).Encode(errorResponse); err != nil {
		glog.Errorf("failed to encode S3Tables error response (status=%d, code=%s, message=%q): %v", status, code, message, err)
	}
}

func getDecodedPathParam(r *http.Request, name string) (string, error) {
	value := mux.Vars(r)[name]
	if value == "" {
		return "", nil
	}
	decoded, err := url.PathUnescape(value)
	if err != nil {
		return "", err
	}
	if decoded == ".." || strings.Contains(decoded, "../") || strings.Contains(decoded, `..\`) || strings.Contains(decoded, "\x00") {
		return "", fmt.Errorf("invalid path parameter %s", name)
	}
	return decoded, nil
}

func buildTableBucketRequestWithARN(r *http.Request, constructor func(string) interface{}) (interface{}, error) {
	arn, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	if arn == "" {
		return nil, fmt.Errorf("tableBucketARN is required")
	}
	if _, err := s3tables.ParseBucketNameFromARN(arn); err != nil {
		return nil, err
	}
	return constructor(arn), nil
}

func parseOptionalIntParam(r *http.Request, name string) (int, error) {
	value := r.URL.Query().Get(name)
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func parseOptionalNamespace(r *http.Request, name string) ([]string, error) {
	value := r.URL.Query().Get(name)
	if value == "" {
		return nil, nil
	}
	parts, err := s3tables.ParseNamespace(value)
	if err != nil {
		return nil, fmt.Errorf("invalid %s: %w", name, err)
	}
	return parts, nil
}

func parseRequiredNamespacePathParam(r *http.Request, name string) ([]string, error) {
	value, err := getDecodedPathParam(r, name)
	if err != nil {
		return nil, err
	}
	if value == "" {
		return nil, fmt.Errorf("%s is required", name)
	}
	return s3tables.ParseNamespace(value)
}

// parseTagKeys handles tag key parsing from query parameters.
// If a single value contains commas, it is split into multiple keys (e.g., "key1,key2,key3").
// Otherwise, multiple query values are returned as-is.
func parseTagKeys(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func buildCreateTableBucketRequest(r *http.Request) (interface{}, error) {
	var req s3tables.CreateTableBucketRequest
	if err := readS3TablesJSONBody(r, &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func buildListTableBucketsRequest(r *http.Request) (interface{}, error) {
	maxBuckets, err := parseOptionalIntParam(r, "maxBuckets")
	if err != nil {
		return nil, err
	}
	return &s3tables.ListTableBucketsRequest{
		Prefix:            r.URL.Query().Get("prefix"),
		ContinuationToken: r.URL.Query().Get("continuationToken"),
		MaxBuckets:        maxBuckets,
	}, nil
}

func buildTableBucketArnRequest(r *http.Request) (interface{}, error) {
	return buildTableBucketRequestWithARN(r, func(arn string) interface{} {
		return &s3tables.GetTableBucketRequest{TableBucketARN: arn}
	})
}

func buildDeleteTableBucketRequest(r *http.Request) (interface{}, error) {
	return buildTableBucketRequestWithARN(r, func(arn string) interface{} {
		return &s3tables.DeleteTableBucketRequest{TableBucketARN: arn}
	})
}

func buildPutTableBucketPolicyRequest(r *http.Request) (interface{}, error) {
	var req s3tables.PutTableBucketPolicyRequest
	if err := readS3TablesJSONBody(r, &req); err != nil {
		return nil, err
	}
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	req.TableBucketARN = tableBucketARN
	return &req, nil
}

func buildGetTableBucketPolicyRequest(r *http.Request) (interface{}, error) {
	return buildTableBucketRequestWithARN(r, func(arn string) interface{} {
		return &s3tables.GetTableBucketPolicyRequest{TableBucketARN: arn}
	})
}

func buildDeleteTableBucketPolicyRequest(r *http.Request) (interface{}, error) {
	return buildTableBucketRequestWithARN(r, func(arn string) interface{} {
		return &s3tables.DeleteTableBucketPolicyRequest{TableBucketARN: arn}
	})
}

func buildCreateNamespaceRequest(r *http.Request) (interface{}, error) {
	var req s3tables.CreateNamespaceRequest
	if err := readS3TablesJSONBody(r, &req); err != nil {
		return nil, err
	}
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	req.TableBucketARN = tableBucketARN
	return &req, nil
}

func buildListNamespacesRequest(r *http.Request) (interface{}, error) {
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	maxNamespaces, err := parseOptionalIntParam(r, "maxNamespaces")
	if err != nil {
		return nil, err
	}
	return &s3tables.ListNamespacesRequest{
		TableBucketARN:    tableBucketARN,
		Prefix:            r.URL.Query().Get("prefix"),
		ContinuationToken: r.URL.Query().Get("continuationToken"),
		MaxNamespaces:     maxNamespaces,
	}, nil
}

func buildGetNamespaceRequest(r *http.Request) (interface{}, error) {
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	namespace, err := parseRequiredNamespacePathParam(r, "namespace")
	if err != nil {
		return nil, err
	}
	return &s3tables.GetNamespaceRequest{
		TableBucketARN: tableBucketARN,
		Namespace:      namespace,
	}, nil
}

func buildDeleteNamespaceRequest(r *http.Request) (interface{}, error) {
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	namespace, err := parseRequiredNamespacePathParam(r, "namespace")
	if err != nil {
		return nil, err
	}
	return &s3tables.DeleteNamespaceRequest{
		TableBucketARN: tableBucketARN,
		Namespace:      namespace,
	}, nil
}

func buildCreateTableRequest(r *http.Request) (interface{}, error) {
	var req s3tables.CreateTableRequest
	if err := readS3TablesJSONBody(r, &req); err != nil {
		return nil, err
	}
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	namespace, err := parseRequiredNamespacePathParam(r, "namespace")
	if err != nil {
		return nil, err
	}
	req.TableBucketARN = tableBucketARN
	req.Namespace = namespace
	return &req, nil
}

func buildListTablesRequest(r *http.Request) (interface{}, error) {
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	namespace, err := parseOptionalNamespace(r, "namespace")
	if err != nil {
		return nil, err
	}
	maxTables, err := parseOptionalIntParam(r, "maxTables")
	if err != nil {
		return nil, err
	}
	return &s3tables.ListTablesRequest{
		TableBucketARN:    tableBucketARN,
		Namespace:         namespace,
		Prefix:            r.URL.Query().Get("prefix"),
		ContinuationToken: r.URL.Query().Get("continuationToken"),
		MaxTables:         maxTables,
	}, nil
}

func buildGetTableRequest(r *http.Request) (interface{}, error) {
	query := r.URL.Query()
	tableARN := query.Get("tableArn")
	req := &s3tables.GetTableRequest{
		TableARN: tableARN,
	}
	if tableARN == "" {
		req.TableBucketARN = query.Get("tableBucketARN")
		namespace, err := parseOptionalNamespace(r, "namespace")
		if err != nil {
			return nil, err
		}
		req.Namespace = namespace
		req.Name = query.Get("name")
		if req.TableBucketARN == "" || len(req.Namespace) == 0 || req.Name == "" {
			return nil, fmt.Errorf("either tableArn or (tableBucketARN, namespace, name) must be provided")
		}
	}
	return req, nil
}

func buildDeleteTableRequest(r *http.Request) (interface{}, error) {
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	namespace, err := parseRequiredNamespacePathParam(r, "namespace")
	if err != nil {
		return nil, err
	}
	name, err := getDecodedPathParam(r, "name")
	if err != nil {
		return nil, err
	}
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if _, err := s3tables.ValidateTableName(name); err != nil {
		return nil, err
	}
	return &s3tables.DeleteTableRequest{
		TableBucketARN: tableBucketARN,
		Namespace:      namespace,
		Name:           name,
		VersionToken:   r.URL.Query().Get("versionToken"),
	}, nil
}

func buildPutTablePolicyRequest(r *http.Request) (interface{}, error) {
	var req s3tables.PutTablePolicyRequest
	if err := readS3TablesJSONBody(r, &req); err != nil {
		return nil, err
	}
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	namespace, err := parseRequiredNamespacePathParam(r, "namespace")
	if err != nil {
		return nil, err
	}
	name, err := getDecodedPathParam(r, "name")
	if err != nil {
		return nil, err
	}
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if _, err := s3tables.ValidateTableName(name); err != nil {
		return nil, err
	}
	req.TableBucketARN = tableBucketARN
	req.Namespace = namespace
	req.Name = name
	return &req, nil
}

func buildGetTablePolicyRequest(r *http.Request) (interface{}, error) {
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	namespace, err := parseRequiredNamespacePathParam(r, "namespace")
	if err != nil {
		return nil, err
	}
	name, err := getDecodedPathParam(r, "name")
	if err != nil {
		return nil, err
	}
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if _, err := s3tables.ValidateTableName(name); err != nil {
		return nil, err
	}
	return &s3tables.GetTablePolicyRequest{
		TableBucketARN: tableBucketARN,
		Namespace:      namespace,
		Name:           name,
	}, nil
}

func buildDeleteTablePolicyRequest(r *http.Request) (interface{}, error) {
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	namespace, err := parseRequiredNamespacePathParam(r, "namespace")
	if err != nil {
		return nil, err
	}
	name, err := getDecodedPathParam(r, "name")
	if err != nil {
		return nil, err
	}
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if _, err := s3tables.ValidateTableName(name); err != nil {
		return nil, err
	}
	return &s3tables.DeleteTablePolicyRequest{
		TableBucketARN: tableBucketARN,
		Namespace:      namespace,
		Name:           name,
	}, nil
}

func buildTagResourceRequest(r *http.Request) (interface{}, error) {
	var req s3tables.TagResourceRequest
	if err := readS3TablesJSONBody(r, &req); err != nil {
		return nil, err
	}
	resourceARN, err := getDecodedPathParam(r, "resourceArn")
	if err != nil {
		return nil, err
	}
	if resourceARN == "" {
		return nil, fmt.Errorf("resourceArn is required")
	}
	req.ResourceARN = resourceARN
	return &req, nil
}

func buildListTagsForResourceRequest(r *http.Request) (interface{}, error) {
	resourceARN, err := getDecodedPathParam(r, "resourceArn")
	if err != nil {
		return nil, err
	}
	if resourceARN == "" {
		return nil, fmt.Errorf("resourceArn is required")
	}
	return &s3tables.ListTagsForResourceRequest{
		ResourceARN: resourceARN,
	}, nil
}

func buildUntagResourceRequest(r *http.Request) (interface{}, error) {
	resourceARN, err := getDecodedPathParam(r, "resourceArn")
	if err != nil {
		return nil, err
	}
	if resourceARN == "" {
		return nil, fmt.Errorf("resourceArn is required")
	}
	tagKeys := parseTagKeys(r.URL.Query()["tagKeys"])
	if len(tagKeys) == 0 {
		return nil, fmt.Errorf("tagKeys is required for %s", resourceARN)
	}
	return &s3tables.UntagResourceRequest{
		ResourceARN: resourceARN,
		TagKeys:     tagKeys,
	}, nil
}

// authenticateS3Tables wraps the handler with IAM authentication using AuthSignatureOnly
// This authenticates the request but delegates authorization to the S3 Tables handler
// which performs granular permission checks based on the specific operation.
func (s3a *S3ApiServer) authenticateS3Tables(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		glog.V(2).Infof("S3Tables: authenticateS3Tables called, iam.isEnabled()=%t", s3a.iam.isEnabled())
		if !s3a.iam.isEnabled() {
			f(w, r)
			return
		}

		// Use AuthSignatureOnly to authenticate the request without authorizing specific actions
		identity, errCode := s3a.iam.AuthSignatureOnly(r)
		if errCode != s3err.ErrNone {
			glog.Errorf("S3Tables: AuthSignatureOnly failed: %v", errCode)
			s3err.WriteErrorResponse(w, r, errCode)
			return
		}

		// Store the authenticated identity in request context
		if identity != nil && identity.Name != "" {
			glog.V(2).Infof("S3Tables: authenticated identity Name=%s Account.Id=%s", identity.Name, identity.Account.Id)
			ctx := s3_constants.SetIdentityNameInContext(r.Context(), identity.Name)
			ctx = s3_constants.SetIdentityInContext(ctx, identity)
			r = r.WithContext(ctx)
		} else {
			glog.V(2).Infof("S3Tables: authenticated identity is nil or empty name")
		}

		f(w, r)
	}
}
