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

	// REST-style S3 Tables API routes (used by AWS CLI)
	router.Methods(http.MethodPut).Path("/buckets").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("CreateTableBucket", buildCreateTableBucketRequest)), "S3Tables-CreateTableBucket"))
	router.Methods(http.MethodGet).Path("/buckets").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("ListTableBuckets", buildListTableBucketsRequest)), "S3Tables-ListTableBuckets"))
	router.Methods(http.MethodGet).Path("/buckets/{tableBucketARN:[^/]+/[^/]+}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("GetTableBucket", buildTableBucketArnRequest)), "S3Tables-GetTableBucket"))
	router.Methods(http.MethodDelete).Path("/buckets/{tableBucketARN:[^/]+/[^/]+}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("DeleteTableBucket", buildDeleteTableBucketRequest)), "S3Tables-DeleteTableBucket"))
	router.Methods(http.MethodPut).Path("/buckets/{tableBucketARN:[^/]+/[^/]+}/policy").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("PutTableBucketPolicy", buildPutTableBucketPolicyRequest)), "S3Tables-PutTableBucketPolicy"))
	router.Methods(http.MethodGet).Path("/buckets/{tableBucketARN:[^/]+/[^/]+}/policy").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("GetTableBucketPolicy", buildGetTableBucketPolicyRequest)), "S3Tables-GetTableBucketPolicy"))
	router.Methods(http.MethodDelete).Path("/buckets/{tableBucketARN:[^/]+/[^/]+}/policy").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("DeleteTableBucketPolicy", buildDeleteTableBucketPolicyRequest)), "S3Tables-DeleteTableBucketPolicy"))

	router.Methods(http.MethodPut).Path("/namespaces/{tableBucketARN:[^/]+/[^/]+}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("CreateNamespace", buildCreateNamespaceRequest)), "S3Tables-CreateNamespace"))
	router.Methods(http.MethodGet).Path("/namespaces/{tableBucketARN:[^/]+/[^/]+}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("ListNamespaces", buildListNamespacesRequest)), "S3Tables-ListNamespaces"))
	router.Methods(http.MethodGet).Path("/namespaces/{tableBucketARN:[^/]+/[^/]+}/{namespace}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("GetNamespace", buildGetNamespaceRequest)), "S3Tables-GetNamespace"))
	router.Methods(http.MethodDelete).Path("/namespaces/{tableBucketARN:[^/]+/[^/]+}/{namespace}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("DeleteNamespace", buildDeleteNamespaceRequest)), "S3Tables-DeleteNamespace"))

	router.Methods(http.MethodPut).Path("/tables/{tableBucketARN:[^/]+/[^/]+}/{namespace}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("CreateTable", buildCreateTableRequest)), "S3Tables-CreateTable"))
	router.Methods(http.MethodGet).Path("/tables/{tableBucketARN:[^/]+/[^/]+}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("ListTables", buildListTablesRequest)), "S3Tables-ListTables"))
	router.Methods(http.MethodDelete).Path("/tables/{tableBucketARN:[^/]+/[^/]+}/{namespace}/{name}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("DeleteTable", buildDeleteTableRequest)), "S3Tables-DeleteTable"))
	router.Methods(http.MethodGet).Path("/get-table").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("GetTable", buildGetTableRequest)), "S3Tables-GetTable"))

	router.Methods(http.MethodPut).Path("/tables/{tableBucketARN:[^/]+/[^/]+}/{namespace}/{name}/policy").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("PutTablePolicy", buildPutTablePolicyRequest)), "S3Tables-PutTablePolicy"))
	router.Methods(http.MethodGet).Path("/tables/{tableBucketARN:[^/]+/[^/]+}/{namespace}/{name}/policy").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("GetTablePolicy", buildGetTablePolicyRequest)), "S3Tables-GetTablePolicy"))
	router.Methods(http.MethodDelete).Path("/tables/{tableBucketARN:[^/]+/[^/]+}/{namespace}/{name}/policy").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("DeleteTablePolicy", buildDeleteTablePolicyRequest)), "S3Tables-DeleteTablePolicy"))

	router.Methods(http.MethodPost).Path("/tag/{resourceArn:.*}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("TagResource", buildTagResourceRequest)), "S3Tables-TagResource"))
	router.Methods(http.MethodGet).Path("/tag/{resourceArn:.*}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("ListTagsForResource", buildListTagsForResourceRequest)), "S3Tables-ListTagsForResource"))
	router.Methods(http.MethodDelete).Path("/tag/{resourceArn:.*}").
		HandlerFunc(track(s3a.authenticateS3Tables(s3TablesApi.handleRestOperation("UntagResource", buildUntagResourceRequest)), "S3Tables-UntagResource"))

	glog.V(1).Infof("S3 Tables API enabled")
}

// isS3TablesAction checks if the action is an S3 Tables operation using O(1) map lookup

type s3tablesRequestBuilder func(r *http.Request) (interface{}, error)

func (st *S3TablesApiServer) handleRestOperation(operation string, builder s3tablesRequestBuilder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		payload, err := builder(r)
		if err != nil {
			writeS3TablesError(w, http.StatusBadRequest, s3tables.ErrCodeInvalidRequest, err.Error())
			return
		}
		if err := setS3TablesRequestBody(r, payload); err != nil {
			writeS3TablesError(w, http.StatusInternalServerError, s3tables.ErrCodeInternalError, "failed to prepare request")
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
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
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
	_ = json.NewEncoder(w).Encode(errorResponse)
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
	return decoded, nil
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
	return parsed, nil
}

func parseOptionalNamespace(r *http.Request, name string) []string {
	value := r.URL.Query().Get(name)
	if value == "" {
		return nil
	}
	return []string{value}
}

func parseTagKeys(values []string) []string {
	if len(values) == 1 {
		if split := strings.Split(values[0], ","); len(split) > 1 {
			return split
		}
	}
	return values
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
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	return &s3tables.GetTableBucketRequest{
		TableBucketARN: tableBucketARN,
	}, nil
}

func buildDeleteTableBucketRequest(r *http.Request) (interface{}, error) {
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	return &s3tables.DeleteTableBucketRequest{
		TableBucketARN: tableBucketARN,
	}, nil
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
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	return &s3tables.GetTableBucketPolicyRequest{
		TableBucketARN: tableBucketARN,
	}, nil
}

func buildDeleteTableBucketPolicyRequest(r *http.Request) (interface{}, error) {
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	return &s3tables.DeleteTableBucketPolicyRequest{
		TableBucketARN: tableBucketARN,
	}, nil
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
	namespace, err := getDecodedPathParam(r, "namespace")
	if err != nil {
		return nil, err
	}
	return &s3tables.GetNamespaceRequest{
		TableBucketARN: tableBucketARN,
		Namespace:      []string{namespace},
	}, nil
}

func buildDeleteNamespaceRequest(r *http.Request) (interface{}, error) {
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	namespace, err := getDecodedPathParam(r, "namespace")
	if err != nil {
		return nil, err
	}
	return &s3tables.DeleteNamespaceRequest{
		TableBucketARN: tableBucketARN,
		Namespace:      []string{namespace},
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
	namespace, err := getDecodedPathParam(r, "namespace")
	if err != nil {
		return nil, err
	}
	req.TableBucketARN = tableBucketARN
	if namespace != "" {
		req.Namespace = []string{namespace}
	}
	return &req, nil
}

func buildListTablesRequest(r *http.Request) (interface{}, error) {
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	maxTables, err := parseOptionalIntParam(r, "maxTables")
	if err != nil {
		return nil, err
	}
	return &s3tables.ListTablesRequest{
		TableBucketARN:    tableBucketARN,
		Namespace:         parseOptionalNamespace(r, "namespace"),
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
		req.Namespace = parseOptionalNamespace(r, "namespace")
		req.Name = query.Get("name")
	}
	return req, nil
}

func buildDeleteTableRequest(r *http.Request) (interface{}, error) {
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	namespace, err := getDecodedPathParam(r, "namespace")
	if err != nil {
		return nil, err
	}
	name, err := getDecodedPathParam(r, "name")
	if err != nil {
		return nil, err
	}
	return &s3tables.DeleteTableRequest{
		TableBucketARN: tableBucketARN,
		Namespace:      []string{namespace},
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
	namespace, err := getDecodedPathParam(r, "namespace")
	if err != nil {
		return nil, err
	}
	name, err := getDecodedPathParam(r, "name")
	if err != nil {
		return nil, err
	}
	req.TableBucketARN = tableBucketARN
	req.Namespace = []string{namespace}
	req.Name = name
	return &req, nil
}

func buildGetTablePolicyRequest(r *http.Request) (interface{}, error) {
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	namespace, err := getDecodedPathParam(r, "namespace")
	if err != nil {
		return nil, err
	}
	name, err := getDecodedPathParam(r, "name")
	if err != nil {
		return nil, err
	}
	return &s3tables.GetTablePolicyRequest{
		TableBucketARN: tableBucketARN,
		Namespace:      []string{namespace},
		Name:           name,
	}, nil
}

func buildDeleteTablePolicyRequest(r *http.Request) (interface{}, error) {
	tableBucketARN, err := getDecodedPathParam(r, "tableBucketARN")
	if err != nil {
		return nil, err
	}
	namespace, err := getDecodedPathParam(r, "namespace")
	if err != nil {
		return nil, err
	}
	name, err := getDecodedPathParam(r, "name")
	if err != nil {
		return nil, err
	}
	return &s3tables.DeleteTablePolicyRequest{
		TableBucketARN: tableBucketARN,
		Namespace:      []string{namespace},
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
	req.ResourceARN = resourceARN
	return &req, nil
}

func buildListTagsForResourceRequest(r *http.Request) (interface{}, error) {
	resourceARN, err := getDecodedPathParam(r, "resourceArn")
	if err != nil {
		return nil, err
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
	tagKeys := parseTagKeys(r.URL.Query()["tagKeys"])
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
		if !s3a.iam.isEnabled() {
			f(w, r)
			return
		}

		// Use AuthSignatureOnly to authenticate the request without authorizing specific actions
		identity, errCode := s3a.iam.AuthSignatureOnly(r)
		if errCode != s3err.ErrNone {
			s3err.WriteErrorResponse(w, r, errCode)
			return
		}

		// Store the authenticated identity in request context
		if identity != nil && identity.Name != "" {
			ctx := s3_constants.SetIdentityNameInContext(r.Context(), identity.Name)
			ctx = s3_constants.SetIdentityInContext(ctx, identity)
			r = r.WithContext(ctx)
		}

		f(w, r)
	}
}
