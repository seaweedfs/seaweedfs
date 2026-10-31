package s3vectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

var handlers = map[string]func(*Server, http.ResponseWriter, *http.Request){
	"CreateVectorBucket":       (*Server).handleCreateVectorBucket,
	"GetVectorBucket":          (*Server).handleGetVectorBucket,
	"ListVectorBuckets":        (*Server).handleListVectorBuckets,
	"DeleteVectorBucket":       (*Server).handleDeleteVectorBucket,
	"PutVectorBucketPolicy":    (*Server).handlePutVectorBucketPolicy,
	"GetVectorBucketPolicy":    (*Server).handleGetVectorBucketPolicy,
	"DeleteVectorBucketPolicy": (*Server).handleDeleteVectorBucketPolicy,
	"CreateIndex":              (*Server).handleCreateIndex,
	"GetIndex":                 (*Server).handleGetIndex,
	"ListIndexes":              (*Server).handleListIndexes,
	"DeleteIndex":              (*Server).handleDeleteIndex,
	"PutVectors":               (*Server).handlePutVectors,
	"GetVectors":               (*Server).handleGetVectors,
	"ListVectors":              (*Server).handleListVectors,
	"DeleteVectors":            (*Server).handleDeleteVectors,
	"QueryVectors":             (*Server).handleQueryVectors,
}

// lance table ids put indexes under a "vector" namespace inside their bucket.
const indexNamespace = "vector"

// Vector bucket records are s3tables table buckets; the S3 Vectors ARN form is
// arn:aws:s3vectors:{region}:{account}:bucket/{name}[/index/{name}].
func (s *Server) vectorBucketArn(bucket string) string {
	return fmt.Sprintf("arn:aws:s3vectors:%s:%s:bucket/%s", s.region, s.accountID, bucket)
}

func (s *Server) indexArn(bucket, index string) string {
	return fmt.Sprintf("%s/index/%s", s.vectorBucketArn(bucket), index)
}

// s3tablesArn renders the same bucket in the table-bucket ARN form the S3
// Tables catalog keys records by.
func (s *Server) s3tablesArn(bucket string) string {
	return fmt.Sprintf("arn:aws:s3tables:%s:%s:bucket/%s", s.region, s.accountID, bucket)
}

func (s *Server) arnParts(v *vectorBucketARNs, name, arn string) error {
	if arn == "" {
		v.bucket = name
		return nil
	}
	if !strings.HasPrefix(arn, "arn:") {
		v.bucket = arn
		return nil
	}
	parsed, err := awsarn.Parse(arn)
	if err != nil {
		return fmt.Errorf("malformed ARN %q", arn)
	}
	if !strings.HasPrefix(arn, s.vectorBucketArn("")) {
		return fmt.Errorf("ARN %q does not belong to this vector namespace", arn)
	}
	parts := strings.Split(parsed.Resource, "/")
	if (len(parts) != 2 && len(parts) != 4) || parts[0] != "bucket" || parts[1] == "" {
		return fmt.Errorf("malformed ARN %q", arn)
	}
	if len(parts) == 4 && (parts[2] != "index" || parts[3] == "") {
		return fmt.Errorf("malformed ARN %q", arn)
	}
	v.bucket, v.index = parts[1], ""
	if len(parts) == 4 {
		v.index = parts[3]
	}
	return nil
}

// bucketARNs resolves bucket selectors and rejects index ARNs.
func (s *Server) bucketARNs(name, arn string) (vectorBucketARNs, error) {
	var v vectorBucketARNs
	if err := s.arnParts(&v, name, arn); err != nil {
		return v, err
	}
	if v.index != "" {
		return v, fmt.Errorf("vectorBucketArn %q names an index, not a bucket", arn)
	}
	if name != "" && name != v.bucket {
		return v, fmt.Errorf("vectorBucketName and vectorBucketArn name different buckets")
	}
	return v, nil
}

// resolveIndexARNs resolves names, a bucket ARN, or an index ARN.
func (s *Server) resolveIndexARNs(bucketName, bucketArn, indexName, indexArn string) (vectorBucketARNs, error) {
	var v vectorBucketARNs
	if strings.HasPrefix(indexArn, "arn:") {
		if err := s.arnParts(&v, "", indexArn); err != nil {
			return v, err
		}
		if v.index == "" {
			return v, fmt.Errorf("indexArn %q does not name an index", indexArn)
		}
	} else if indexArn != "" {
		return v, fmt.Errorf("indexArn must be an ARN, got %q", indexArn)
	}
	if bucketArn != "" {
		if !strings.HasPrefix(bucketArn, "arn:") {
			return v, fmt.Errorf("vectorBucketArn must be an ARN, got %q", bucketArn)
		}
		bucket, err := s.bucketARNs("", bucketArn)
		if err != nil {
			return v, err
		}
		if v.bucket != "" && v.bucket != bucket.bucket {
			return v, fmt.Errorf("vectorBucketArn and indexArn name different buckets")
		}
		v.bucket = bucket.bucket
	}
	if v.bucket == "" {
		v.bucket = bucketName
	} else if bucketName != "" && bucketName != v.bucket {
		return v, fmt.Errorf("vectorBucketName and ARN bucket name different buckets")
	}
	if v.index == "" {
		v.index = indexName
	} else if indexName != "" && indexName != v.index {
		return v, fmt.Errorf("indexName and indexArn name different indexes")
	}
	if v.bucket == "" || v.index == "" {
		return v, fmt.Errorf("vectorBucketName and indexName (or indexArn) are required")
	}
	return v, nil
}

// indexRecord fetches the index's catalog record on the trusted path; the
// operation's own vector permission — checked in authorizeVector before any
// handler reaches this — is what gates access.
func (s *Server) indexRecord(ctx context.Context, bucket, index string) (map[string]interface{}, error) {
	var resp map[string]interface{}
	err := s.internalCatalog(ctx, "GetTable", &s3tables.GetTableRequest{
		TableBucketARN: s.s3tablesArn(bucket),
		Namespace:      []string{indexNamespace},
		Name:           index,
	}, &resp)
	return resp, err
}

func (s *Server) catalog(ctx context.Context, operation string, req, resp interface{}, r *http.Request) error {
	return s.tables.Execute(ctx, s.filerClient, operation, req, resp, identityOf(r))
}

// internalCatalog runs catalog bookkeeping under the trusted identity. It is
// for reads and writes that accompany — never replace — the operation's own
// permission check: bucket format lookups, the implicit index namespace, and
// rollback of a record this request just created.
func (s *Server) internalCatalog(ctx context.Context, operation string, req, resp interface{}) error {
	return s.internal.Execute(ctx, s.filerClient, operation, req, resp, "")
}

func catalogErrCode(err error) string {
	var apiErr *s3tables.S3TablesError
	if errors.As(err, &apiErr) {
		return apiErr.Type
	}
	return ""
}

func catalogErr(w http.ResponseWriter, err error) {
	var apiErr *s3tables.S3TablesError
	if !errors.As(err, &apiErr) {
		writeError(w, http.StatusInternalServerError, "InternalFailure", "internal error")
		return
	}
	switch {
	case apiErr.Type == s3tables.ErrCodeAccessDenied:
		writeError(w, http.StatusForbidden, "AccessDeniedException", "access denied")
	case strings.HasPrefix(apiErr.Type, "NoSuch"):
		writeError(w, http.StatusNotFound, "NotFoundException", apiErr.Message)
	case strings.Contains(apiErr.Type, "AlreadyExists") || strings.Contains(apiErr.Type, "NotEmpty") ||
		apiErr.Type == s3tables.ErrCodeConflict:
		writeError(w, http.StatusConflict, "ConflictException", apiErr.Message)
	case apiErr.Type == s3tables.ErrCodeInternalError:
		writeError(w, http.StatusInternalServerError, "InternalFailure", apiErr.Message)
	default:
		writeError(w, http.StatusBadRequest, "ValidationException", apiErr.Message)
	}
}

// vectorErr maps a VectorService gRPC status onto the API's error shape.
// Engine messages reach clients only for codes that describe the request
// itself; transport and internal details stay in the server log.
func vectorErr(w http.ResponseWriter, err error) {
	msg := status.Convert(err).Message()
	switch status.Code(err) {
	case codes.NotFound:
		writeError(w, http.StatusNotFound, "NotFoundException", msg)
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		writeError(w, http.StatusBadRequest, "ValidationException", msg)
	case codes.PermissionDenied, codes.Unauthenticated:
		writeError(w, http.StatusForbidden, "AccessDeniedException", "access denied")
	case codes.Unavailable:
		writeError(w, http.StatusServiceUnavailable, "ServiceUnavailableException", "vector service unavailable")
	default:
		glog.V(1).Infof("S3 Vectors: engine error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalFailure", "internal error")
	}
}

// ensureIndexNamespace declares the implicit index namespace when absent. The
// calls are internal: a caller's CreateTable grant is what authorizes the
// index, so it must not also need namespace reads or writes.
func (s *Server) ensureIndexNamespace(ctx context.Context, bucket string) error {
	var resp map[string]interface{}
	err := s.internalCatalog(ctx, "GetNamespace", &s3tables.GetNamespaceRequest{
		TableBucketARN: s.s3tablesArn(bucket),
		Namespace:      []string{indexNamespace},
	}, &resp)
	if err == nil {
		return nil
	}
	if code := catalogErrCode(err); code != s3tables.ErrCodeNoSuchNamespace && code != s3tables.ErrCodeNoSuchBucket {
		return err
	}
	err = s.internalCatalog(ctx, "CreateNamespace", &s3tables.CreateNamespaceRequest{
		TableBucketARN: s.s3tablesArn(bucket),
		Namespace:      []string{indexNamespace},
	}, &resp)
	// A concurrent CreateIndex may have won the create.
	if err != nil && catalogErrCode(err) != s3tables.ErrCodeNamespaceAlreadyExists {
		return err
	}
	return nil
}

func (s *Server) handleCreateVectorBucket(w http.ResponseWriter, r *http.Request) {
	var req CreateVectorBucketRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if req.VectorBucketName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "vectorBucketName is required")
		return
	}
	if err := s3tables.ValidateEncryptionConfiguration(req.EncryptionConfiguration, "sseType"); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	// The catalog keeps the s3tables field name; GetVectorBucket renames it back.
	enc := req.EncryptionConfiguration
	if len(enc) != 0 {
		enc = map[string]interface{}{"sseAlgorithm": enc["sseType"]}
	}
	var resp map[string]interface{}
	err := s.catalog(r.Context(), "CreateTableBucket",
		&s3tables.CreateTableBucketRequest{
			Name:                    req.VectorBucketName,
			Format:                  s3tables.FormatLance,
			EncryptionConfiguration: enc,
		}, &resp, r)
	if err != nil {
		catalogErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &CreateVectorBucketResponse{VectorBucketArn: s.vectorBucketArn(req.VectorBucketName)})
}

func (s *Server) handleGetVectorBucket(w http.ResponseWriter, r *http.Request) {
	var req GetVectorBucketRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := s.bucketARNs(req.VectorBucketName, req.VectorBucketArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	record, err := s.authorizeVector(r.Context(), r, "s3vectors:GetVectorBucket", arns.bucket, "")
	if err != nil {
		catalogErr(w, err)
		return
	}
	bucket := &VectorBucket{
		VectorBucketArn:  s.vectorBucketArn(arns.bucket),
		VectorBucketName: arns.bucket,
		CreationTime:     record.CreatedAt,
	}
	if cfg, ok := record.EncryptionConfiguration["sseAlgorithm"]; ok {
		bucket.EncryptionConfiguration = map[string]interface{}{"sseType": cfg}
	} else if len(record.EncryptionConfiguration) > 0 {
		bucket.EncryptionConfiguration = record.EncryptionConfiguration
	}
	writeJSON(w, http.StatusOK, &GetVectorBucketResponse{VectorBucket: bucket})
}

func (s *Server) handleListVectorBuckets(w http.ResponseWriter, r *http.Request) {
	var req ListVectorBucketsRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	maxResults := 0
	if req.MaxResults != nil {
		if *req.MaxResults < 1 || *req.MaxResults > vectorsMaxListPage {
			writeError(w, http.StatusBadRequest, "ValidationException", fmt.Sprintf("maxResults must be between 1 and %d", vectorsMaxListPage))
			return
		}
		maxResults = *req.MaxResults
	}
	var resp struct {
		TableBuckets []struct {
			Arn       string `json:"arn"`
			Name      string `json:"name"`
			CreatedAt string `json:"createdAt"`
			Format    string `json:"format"`
		} `json:"tableBuckets"`
		ContinuationToken string `json:"continuationToken"`
	}
	// The listing runs internal and filters on the vector policy itself: a
	// shared-slot check would hide buckets the caller was granted under
	// s3vectors actions, while showing ones they were not.
	err := s.internalCatalog(r.Context(), "ListTableBuckets",
		&s3tables.ListTableBucketsRequest{
			MaxBuckets: maxResults, ContinuationToken: req.NextToken, Prefix: req.Prefix,
		}, &resp)
	if err != nil {
		catalogErr(w, err)
		return
	}
	principal := s.tables.PrincipalFor(identityOf(r))
	pctx := s.tables.PolicyContext(r)
	// The catalog token continues the filtered page: ICEBERG buckets drop out
	// of the listing, Lance and format-less legacy buckets stay.
	out := &ListVectorBucketsResponse{NextToken: resp.ContinuationToken, VectorBuckets: []*VectorBucket{}}
	for _, b := range resp.TableBuckets {
		if b.Format != "" && b.Format != s3tables.FormatLance {
			continue
		}
		owner, policy, err := s.vectorPolicyView(r.Context(), b.Name)
		if err != nil {
			// A bucket that vanished mid-list drops out of the page rather
			// than failing the whole listing.
			continue
		}
		arn := s.vectorBucketArn(b.Name)
		if !s3tables.CheckPermissionWithContext("s3vectors:ListVectorBuckets", principal, owner, policy, arn, pctx) &&
			!s3tables.CheckPermissionWithContext("s3vectors:GetVectorBucket", principal, owner, policy, arn, pctx) {
			continue
		}
		created, _ := time.Parse(time.RFC3339, b.CreatedAt)
		out.VectorBuckets = append(out.VectorBuckets, &VectorBucket{
			VectorBucketArn:  s.vectorBucketArn(b.Name),
			VectorBucketName: b.Name,
			CreationTime:     created,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleDeleteVectorBucket(w http.ResponseWriter, r *http.Request) {
	var req DeleteVectorBucketRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := s.bucketARNs(req.VectorBucketName, req.VectorBucketArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if _, err := s.authorizeVector(r.Context(), r, "s3vectors:DeleteVectorBucket", arns.bucket, ""); err != nil {
		catalogErr(w, err)
		return
	}
	var resp map[string]interface{}
	err = s.internalCatalog(r.Context(), "DeleteTableBucket",
		&s3tables.DeleteTableBucketRequest{TableBucketARN: s.s3tablesArn(arns.bucket)}, &resp)
	if err != nil && catalogErrCode(err) == s3tables.ErrCodeBucketNotEmpty {
		// The implicit index namespace counts as bucket content even when it
		// holds no indexes; drop it under the delete's own authorization and
		// retry once. A namespace still holding indexes keeps the conflict.
		var nsResp map[string]interface{}
		nsErr := s.internalCatalog(context.WithoutCancel(r.Context()), "DeleteNamespace",
			&s3tables.DeleteNamespaceRequest{
				TableBucketARN: s.s3tablesArn(arns.bucket),
				Namespace:      []string{indexNamespace},
			}, &nsResp)
		if nsErr == nil {
			err = s.internalCatalog(r.Context(), "DeleteTableBucket",
				&s3tables.DeleteTableBucketRequest{TableBucketARN: s.s3tablesArn(arns.bucket)}, &resp)
		} else if code := catalogErrCode(nsErr); code != s3tables.ErrCodeNoSuchNamespace {
			glog.V(1).Infof("S3 Vectors: drop internal namespace in %s: %v", arns.bucket, nsErr)
		}
	}
	if err != nil {
		catalogErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &DeleteVectorBucketResponse{})
}

func distanceMetric(metric string) (vector_pb.DistanceMetric, bool) {
	switch strings.ToLower(metric) {
	case "", "euclidean":
		return vector_pb.DistanceMetric_L2, true
	case "cosine":
		return vector_pb.DistanceMetric_COSINE, true
	case "dotproduct":
		return vector_pb.DistanceMetric_DOT_PRODUCT, true
	}
	return vector_pb.DistanceMetric_L2, false
}

func tableID(bucket, index string) *vector_pb.Table {
	return &vector_pb.Table{Id: []string{bucket, indexNamespace, index}}
}

// vectorPolicyKey names the table-bucket attribute holding the bucket's
// s3vectors policy. The document stays in s3vectors form: the shared
// evaluator only honors catalog actions, so vector operations evaluate it
// themselves — against the s3vectors action and ARN the request names. A
// separate key keeps a vector grant from ever authorizing a catalog call.
// The constant lives in s3tables so the catalog asserts it in the update
// preconditions of its own whole-entry writes.
const vectorPolicyKey = s3tables.ExtendedKeyVectorPolicy

// errNoVectorPolicy reports a missing policy document; handlers map it to a
// 404 the same way the catalog maps a missing resource policy.
var errNoVectorPolicy = errors.New("vector bucket policy not found")

// validateVectorPolicy rejects a document the evaluator cannot honor: the
// shared Statement shape has no negated fields, so a rule relying on one would
// silently lose its exclusion; conditions are unsupported since the vector
// evaluation supplies no condition keys; and a rule naming another service is
// a dead rule the writer believes is enforced. The document must decode into
// the same typed form the evaluator uses.
func validateVectorPolicy(raw string) error {
	var typed s3tables.PolicyDocument
	if err := json.Unmarshal([]byte(raw), &typed); err != nil {
		return fmt.Errorf("invalid policy document: %w", err)
	}
	var doc map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return fmt.Errorf("invalid policy document: %w", err)
	}
	for _, stmt := range policyStatements(doc) {
		for key := range stmt {
			for _, field := range []string{"NotAction", "NotResource", "NotPrincipal", "Condition"} {
				if strings.EqualFold(key, field) {
					return fmt.Errorf("invalid policy document: %s is not supported", field)
				}
			}
		}
	}
	for _, stmt := range typed.Statement {
		for _, action := range statementActions(stmt.Action) {
			if service, _, found := strings.Cut(action, ":"); found && service != "s3vectors" {
				return fmt.Errorf("invalid policy document: action %q is not enforceable in a vector bucket policy", action)
			}
		}
		for _, resource := range statementActions(stmt.Resource) {
			if service := arnService(resource); service != "" && service != "s3vectors" {
				return fmt.Errorf("invalid policy document: resource %q is not enforceable in a vector bucket policy", resource)
			}
		}
	}
	return nil
}

// arnService returns the service field of an ARN in any partition.
func arnService(resource string) string {
	parts := strings.SplitN(resource, ":", 4)
	if len(parts) == 4 && parts[0] == "arn" {
		return parts[2]
	}
	return ""
}

func policyStatements(doc map[string]interface{}) []map[string]interface{} {
	var stmts []map[string]interface{}
	switch s := doc["Statement"].(type) {
	case []interface{}:
		for _, item := range s {
			if stmt, ok := item.(map[string]interface{}); ok {
				stmts = append(stmts, stmt)
			}
		}
	case map[string]interface{}:
		stmts = append(stmts, s)
	}
	return stmts
}

// policyHasVectorAction reports whether any statement names an s3vectors
// action — how a raw vector document in the shared policy slot is recognized.
// It reads the typed document so detection matches what the evaluator sees.
func policyHasVectorAction(raw string) bool {
	var doc s3tables.PolicyDocument
	if json.Unmarshal([]byte(raw), &doc) != nil {
		return false
	}
	for _, stmt := range doc.Statement {
		for _, action := range statementActions(stmt.Action) {
			if strings.HasPrefix(action, "s3vectors:") {
				return true
			}
		}
	}
	return false
}

func statementActions(spec interface{}) []string {
	switch a := spec.(type) {
	case string:
		return []string{a}
	case []interface{}:
		var actions []string
		for _, item := range a {
			if str, ok := item.(string); ok {
				actions = append(actions, str)
			}
		}
		return actions
	}
	return nil
}

func splitEntryPath(p string) (dir, name string) {
	i := strings.LastIndex(p, "/")
	return p[:i], p[i+1:]
}

// vectorPolicyFromExt resolves the effective s3vectors policy from an entry's
// attributes: the dedicated key wins, else a shared-slot document that still
// names vector actions.
func vectorPolicyFromExt(ext map[string][]byte) string {
	if doc := ext[vectorPolicyKey]; len(doc) > 0 {
		return string(doc)
	}
	if doc := string(ext[s3tables.ExtendedKeyPolicy]); policyHasVectorAction(doc) {
		return doc
	}
	return ""
}

// stripVectorActions removes s3vectors statements from a shared-slot
// document, keeping its catalog rules: a statement mixing both kinds keeps
// its catalog actions, and a now-empty statement drops out. It reports the
// document to write back — empty when nothing remains — and whether anything
// changed.
func stripVectorActions(raw string) (string, bool) {
	var doc map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return "", false
	}
	changed := false
	var kept []map[string]interface{}
	for _, stmt := range policyStatements(doc) {
		dropped := false
		for key, value := range stmt {
			if !strings.EqualFold(key, "Action") {
				continue
			}
			var actions []string
			keyDropped := false
			for _, a := range statementActions(value) {
				if strings.HasPrefix(a, "s3vectors:") {
					keyDropped = true
					continue
				}
				actions = append(actions, a)
			}
			if !keyDropped {
				continue
			}
			dropped = true
			if len(actions) > 0 {
				stmt[key] = actions
			} else {
				delete(stmt, key)
			}
		}
		if !dropped {
			kept = append(kept, stmt)
			continue
		}
		changed = true
		for key := range stmt {
			if strings.EqualFold(key, "Action") {
				kept = append(kept, stmt)
				break
			}
		}
	}
	if !changed {
		return "", false
	}
	if len(kept) == 0 {
		return "", true
	}
	doc["Statement"] = kept
	out, err := json.Marshal(doc)
	if err != nil {
		return "", false
	}
	return string(out), true
}

// vectorBucket fetches the bucket's catalog record under the trusted
// identity for the authorization lookup; nothing reaches the caller until
// the permission check below it passes.
func (s *Server) vectorBucket(ctx context.Context, bucket string) (*s3tables.GetTableBucketResponse, error) {
	var resp s3tables.GetTableBucketResponse
	if err := s.internalCatalog(ctx, "GetTableBucket",
		&s3tables.GetTableBucketRequest{TableBucketARN: s.s3tablesArn(bucket)}, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// vectorPolicy reads the bucket's stored s3vectors policy. A bucket written
// before the dedicated key carried the raw document in the shared policy
// slot; one that still names vector actions keeps working until the next
// vector policy write migrates it out.
func (s *Server) vectorPolicy(ctx context.Context, bucket string) (string, error) {
	dir, name := splitEntryPath(s3tables.GetTableBucketPath(bucket))
	var policy string
	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if err != nil {
			return err
		}
		policy = vectorPolicyFromExt(resp.Entry.Extended)
		return nil
	})
	return policy, err
}

// vectorPolicyView resolves a bucket's owner and stored s3vectors policy in
// one filer lookup — the per-bucket check the vector listing runs.
func (s *Server) vectorPolicyView(ctx context.Context, bucket string) (owner, policy string, err error) {
	dir, name := splitEntryPath(s3tables.GetTableBucketPath(bucket))
	err = s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if err != nil {
			return err
		}
		var meta struct {
			OwnerAccountID string `json:"ownerAccountId"`
		}
		if raw := resp.Entry.Extended[s3tables.ExtendedKeyMetadata]; len(raw) > 0 {
			_ = json.Unmarshal(raw, &meta)
		}
		owner = meta.OwnerAccountID
		policy = vectorPolicyFromExt(resp.Entry.Extended)
		return nil
	})
	return owner, policy, err
}

// mutateVectorPolicy applies a change to the bucket's vector-policy
// attribute under the entry's extended-attribute compare-and-swap, retrying
// a racing writer the way the catalog's own policy updates do.
func (s *Server) mutateVectorPolicy(ctx context.Context, bucket string, mutate func(ext map[string][]byte) error) error {
	dir, name := splitEntryPath(s3tables.GetTableBucketPath(bucket))
	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		for attempt := 0; attempt < 3; attempt++ {
			resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
				Directory: dir,
				Name:      name,
			})
			if err != nil {
				return err
			}
			entry := resp.Entry
			// Assert the vector policy key stays absent too, so a concurrent
			// policy write trips the compare-and-swap instead of being
			// silently reverted by the whole-entry update.
			expected := s3tables.SnapshotExtended(entry.Extended, vectorPolicyKey)
			if entry.Extended == nil {
				entry.Extended = make(map[string][]byte)
			}
			if err := mutate(entry.Extended); err != nil {
				return err
			}
			if _, err := client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
				Directory:        dir,
				Entry:            entry,
				ExpectedExtended: expected,
			}); err == nil {
				return nil
			} else if status.Code(err) != codes.FailedPrecondition {
				return err
			}
		}
		return &s3tables.S3TablesError{Type: s3tables.ErrCodeConflict,
			Message: "vector bucket policy changed concurrently, retry the request"}
	})
}

// isVectorBucket reports whether the catalog record describes a vector
// bucket: an explicit ICEBERG bucket is not one, while a bucket declared
// before formats existed reads as Lance.
func isVectorBucket(record *s3tables.GetTableBucketResponse) bool {
	return record.Format == "" || record.Format == s3tables.FormatLance
}

// errVectorDenied marks a failed vector permission check inside a policy
// mutation; catalogErr renders it as AccessDenied.
var errVectorDenied = &s3tables.S3TablesError{Type: s3tables.ErrCodeAccessDenied, Message: "access denied"}

// checkVectorPolicyDoc evaluates the caller's vector action against the
// document the entry currently holds. Called inside a policy mutation's
// compare-and-swap, so a grant revoked between reads never authorizes the
// pending write.
func (s *Server) checkVectorPolicyDoc(r *http.Request, action, bucket, owner string, ext map[string][]byte) bool {
	principal := s.tables.PrincipalFor(identityOf(r))
	return s3tables.CheckPermissionWithContext(action, principal, owner,
		vectorPolicyFromExt(ext), s.vectorBucketArn(bucket), s.tables.PolicyContext(r))
}

// authorizeVector grants the request's own vector operation — not a catalog
// stand-in — against the stored s3vectors policy and the ARN it actually
// targets, so an index-scoped rule reaches the index it names and a read
// grant never carries a write. Catalog bookkeeping below rides on the
// trusted internal path, the same pattern the implicit namespace uses.
// The returned record saves the handler a second bucket lookup.
func (s *Server) authorizeVector(ctx context.Context, r *http.Request, action, bucket, index string) (*s3tables.GetTableBucketResponse, error) {
	record, err := s.vectorBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	policy, err := s.vectorPolicy(ctx, bucket)
	if err != nil {
		return nil, err
	}
	resourceARN := s.vectorBucketArn(bucket)
	if index != "" {
		resourceARN = s.indexArn(bucket, index)
	}
	principal := s.tables.PrincipalFor(identityOf(r))
	if !s3tables.CheckPermissionWithContext(action, principal, record.OwnerAccountID, policy, resourceARN, s.tables.PolicyContext(r)) {
		return nil, &s3tables.S3TablesError{Type: s3tables.ErrCodeAccessDenied, Message: "access denied"}
	}
	if !isVectorBucket(record) {
		return nil, &s3tables.S3TablesError{Type: s3tables.ErrCodeNoSuchBucket,
			Message: fmt.Sprintf("vector bucket %s not found", bucket)}
	}
	return record, nil
}

func (s *Server) handlePutVectorBucketPolicy(w http.ResponseWriter, r *http.Request) {
	var req PutVectorBucketPolicyRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := s.bucketARNs(req.VectorBucketName, req.VectorBucketArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if err := validateVectorPolicy(req.Policy); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	record, err := s.vectorBucket(r.Context(), arns.bucket)
	if err != nil {
		catalogErr(w, err)
		return
	}
	if !isVectorBucket(record) {
		writeError(w, http.StatusNotFound, "NotFoundException",
			fmt.Sprintf("vector bucket %s not found", arns.bucket))
		return
	}
	err = s.mutateVectorPolicy(r.Context(), arns.bucket, func(ext map[string][]byte) error {
		if !s.checkVectorPolicyDoc(r, "s3vectors:PutVectorBucketPolicy", arns.bucket, record.OwnerAccountID, ext) {
			return errVectorDenied
		}
		// A raw vector document still in the shared slot migrates out with
		// this write; catalog rules in it stay untouched.
		if stripped, changed := stripVectorActions(string(ext[s3tables.ExtendedKeyPolicy])); changed {
			if stripped == "" {
				delete(ext, s3tables.ExtendedKeyPolicy)
			} else {
				ext[s3tables.ExtendedKeyPolicy] = []byte(stripped)
			}
		}
		ext[vectorPolicyKey] = []byte(req.Policy)
		return nil
	})
	if err != nil {
		catalogErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &PutVectorBucketPolicyResponse{})
}

func (s *Server) handleGetVectorBucketPolicy(w http.ResponseWriter, r *http.Request) {
	var req GetVectorBucketPolicyRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := s.bucketARNs(req.VectorBucketName, req.VectorBucketArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if _, err := s.authorizeVector(r.Context(), r, "s3vectors:GetVectorBucketPolicy", arns.bucket, ""); err != nil {
		catalogErr(w, err)
		return
	}
	policy, err := s.vectorPolicy(r.Context(), arns.bucket)
	if err != nil {
		catalogErr(w, err)
		return
	}
	if policy == "" {
		writeError(w, http.StatusNotFound, "NotFoundException", errNoVectorPolicy.Error())
		return
	}
	writeJSON(w, http.StatusOK, &GetVectorBucketPolicyResponse{Policy: policy})
}

func (s *Server) handleDeleteVectorBucketPolicy(w http.ResponseWriter, r *http.Request) {
	var req DeleteVectorBucketPolicyRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := s.bucketARNs(req.VectorBucketName, req.VectorBucketArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	record, err := s.vectorBucket(r.Context(), arns.bucket)
	if err != nil {
		catalogErr(w, err)
		return
	}
	if !isVectorBucket(record) {
		writeError(w, http.StatusNotFound, "NotFoundException",
			fmt.Sprintf("vector bucket %s not found", arns.bucket))
		return
	}
	err = s.mutateVectorPolicy(r.Context(), arns.bucket, func(ext map[string][]byte) error {
		if !s.checkVectorPolicyDoc(r, "s3vectors:DeleteVectorBucketPolicy", arns.bucket, record.OwnerAccountID, ext) {
			return errVectorDenied
		}
		removed := false
		if _, ok := ext[vectorPolicyKey]; ok {
			delete(ext, vectorPolicyKey)
			removed = true
		}
		// A legacy document in the shared slot loses its vector rules with
		// the delete; catalog rules in it stay untouched.
		if stripped, changed := stripVectorActions(string(ext[s3tables.ExtendedKeyPolicy])); changed {
			removed = true
			if stripped == "" {
				delete(ext, s3tables.ExtendedKeyPolicy)
			} else {
				ext[s3tables.ExtendedKeyPolicy] = []byte(stripped)
			}
		}
		if !removed {
			return errNoVectorPolicy
		}
		return nil
	})
	if errors.Is(err, errNoVectorPolicy) {
		writeError(w, http.StatusNotFound, "NotFoundException", errNoVectorPolicy.Error())
		return
	}
	if err != nil {
		catalogErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &DeleteVectorBucketPolicyResponse{})
}

func (s *Server) handleCreateIndex(w http.ResponseWriter, r *http.Request) {
	var req CreateIndexRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if strings.ToLower(req.DataType) != "float32" {
		writeError(w, http.StatusBadRequest, "ValidationException", "only float32 vectors are supported")
		return
	}
	metric, ok := distanceMetric(req.DistanceMetric)
	if !ok {
		writeError(w, http.StatusBadRequest, "ValidationException", "unknown distanceMetric "+req.DistanceMetric)
		return
	}
	arns, err := s.resolveIndexARNs(req.VectorBucketName, req.VectorBucketArn, req.IndexName, req.IndexArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if req.Dimension <= 0 || req.Dimension > 4096 {
		writeError(w, http.StatusBadRequest, "ValidationException", "dimension must be 1..=4096")
		return
	}
	// The create grant is exercised before any write: a denied caller must not
	// leave even the implicit namespace behind.
	if _, err := s.authorizeVector(r.Context(), r, "s3vectors:CreateIndex", arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	// The namespace is implicit on AWS and internal bookkeeping here.
	if err := s.ensureIndexNamespace(r.Context(), arns.bucket); err != nil {
		catalogErr(w, err)
		return
	}
	// Only a caller who passed the create check may learn an index exists.
	var existing map[string]interface{}
	getErr := s.internalCatalog(r.Context(), "GetTable", &s3tables.GetTableRequest{
		TableBucketARN: s.s3tablesArn(arns.bucket),
		Namespace:      []string{indexNamespace},
		Name:           arns.index,
	}, &existing)
	switch {
	case getErr == nil:
		writeError(w, http.StatusConflict, "ConflictException",
			fmt.Sprintf("index %s already exists", arns.index))
		return
	case catalogErrCode(getErr) != s3tables.ErrCodeNoSuchTable:
		catalogErr(w, getErr)
		return
	}
	metadata := map[string]string{
		"dimension":      fmt.Sprint(req.Dimension),
		"distanceMetric": strings.ToLower(req.DistanceMetric),
		// The nonce marks the record this request created, so a rollback after
		// an engine failure can never erase a pre-existing index.
		"createNonce": uuid.NewString(),
	}
	var nonFilterable []string
	if req.MetadataConfiguration != nil {
		nonFilterable = req.MetadataConfiguration.NonFilterableMetadataKeys
		if keys, err := json.Marshal(nonFilterable); err == nil {
			metadata["nonFilterableMetadataKeys"] = string(keys)
		}
	}
	metadataJSON, _ := json.Marshal(metadata)
	err = s.internalCatalog(r.Context(), "CreateTable", &s3tables.CreateTableRequest{
		TableBucketARN: s.s3tablesArn(arns.bucket),
		Namespace:      []string{indexNamespace},
		Name:           arns.index,
		Format:         s3tables.FormatLance,
		Metadata:       &s3tables.TableMetadata{FullMetadata: metadataJSON},
	}, nil)
	if err != nil {
		catalogErr(w, err)
		return
	}
	_, err = s.vectors.CreateIndex(r.Context(), &vector_pb.CreateIndexRequest{
		Table:                     tableID(arns.bucket, arns.index),
		Dimension:                 uint32(req.Dimension),
		DistanceMetric:            metric,
		NonFilterableMetadataKeys: nonFilterable,
	})
	if err != nil {
		// Roll the index record back so a half-created index does not linger.
		// The delete runs past a client disconnect and only removes the record
		// this request created: the nonce must match, and the versionToken
		// compare-and-swap keeps a racing create from being erased.
		ctx := context.WithoutCancel(r.Context())
		var rec map[string]interface{}
		if gerr := s.internalCatalog(ctx, "GetTable", &s3tables.GetTableRequest{
			TableBucketARN: s.s3tablesArn(arns.bucket),
			Namespace:      []string{indexNamespace},
			Name:           arns.index,
		}, &rec); gerr == nil && recordNonce(rec) == metadata["createNonce"] {
			var delResp map[string]interface{}
			if derr := s.internalCatalog(ctx, "DeleteTable", &s3tables.DeleteTableRequest{
				TableBucketARN: s.s3tablesArn(arns.bucket),
				Namespace:      []string{indexNamespace},
				Name:           arns.index,
				VersionToken:   recordVersionToken(rec),
			}, &delResp); derr != nil {
				glog.Errorf("S3 Vectors: rollback of catalog table %s/%s failed: %v", arns.bucket, arns.index, derr)
			}
		} else if gerr != nil {
			glog.Errorf("S3 Vectors: rollback lookup for %s/%s failed: %v", arns.bucket, arns.index, gerr)
		}
		vectorErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &CreateIndexResponse{IndexArn: s.indexArn(arns.bucket, arns.index)})
}

// recordNonce and recordVersionToken read the catalog record's internal
// bookkeeping for the create-index rollback.
func recordNonce(record map[string]interface{}) string {
	if meta, ok := record["metadata"].(map[string]interface{}); ok {
		if fm, ok := meta["fullMetadata"].(map[string]interface{}); ok {
			return fmt.Sprint(fm["createNonce"])
		}
	}
	return ""
}

func recordVersionToken(record map[string]interface{}) string {
	token, _ := record["versionToken"].(string)
	return token
}

func indexFromRecord(s *Server, bucket, name string, m map[string]interface{}) *Index {
	idx := &Index{
		IndexArn:         s.indexArn(bucket, name),
		IndexName:        name,
		VectorBucketName: bucket,
		DataType:         "float32",
	}
	if created, ok := m["createdAt"].(string); ok {
		idx.CreationTime, _ = time.Parse(time.RFC3339, created)
	}
	if meta, ok := m["metadata"].(map[string]interface{}); ok {
		if fm, ok := meta["fullMetadata"].(map[string]interface{}); ok {
			var dim float64
			fmt.Sscan(fmt.Sprint(fm["dimension"]), &dim)
			idx.Dimension = int32(dim)
			idx.DistanceMetric = strings.ToLower(fmt.Sprint(fm["distanceMetric"]))
			switch keys := fm["nonFilterableMetadataKeys"].(type) {
			case string:
				var list []string
				if err := json.Unmarshal([]byte(keys), &list); err == nil && list != nil {
					idx.MetadataConfiguration = &MetadataConfiguration{NonFilterableMetadataKeys: list}
				}
			case []interface{}:
				var list []string
				for _, k := range keys {
					list = append(list, fmt.Sprint(k))
				}
				idx.MetadataConfiguration = &MetadataConfiguration{NonFilterableMetadataKeys: list}
			}
		}
	}
	if idx.DistanceMetric == "" || idx.DistanceMetric == "<nil>" {
		idx.DistanceMetric = "euclidean"
	}
	return idx
}

func (s *Server) handleGetIndex(w http.ResponseWriter, r *http.Request) {
	var req GetIndexRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := s.resolveIndexARNs(req.VectorBucketName, req.VectorBucketArn, req.IndexName, req.IndexArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if _, err := s.authorizeVector(r.Context(), r, "s3vectors:GetIndex", arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	var resp map[string]interface{}
	err = s.internalCatalog(r.Context(), "GetTable", &s3tables.GetTableRequest{
		TableBucketARN: s.s3tablesArn(arns.bucket),
		Namespace:      []string{indexNamespace},
		Name:           arns.index,
	}, &resp)
	if err != nil {
		catalogErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &GetIndexResponse{Index: indexFromRecord(s, arns.bucket, arns.index, resp)})
}

func (s *Server) handleListIndexes(w http.ResponseWriter, r *http.Request) {
	var req ListIndexesRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := s.bucketARNs(req.VectorBucketName, req.VectorBucketArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	maxResults := 0
	if req.MaxResults != nil {
		if *req.MaxResults < 1 || *req.MaxResults > vectorsMaxListPage {
			writeError(w, http.StatusBadRequest, "ValidationException", fmt.Sprintf("maxResults must be between 1 and %d", vectorsMaxListPage))
			return
		}
		maxResults = *req.MaxResults
	}
	if _, err := s.authorizeVector(r.Context(), r, "s3vectors:ListIndexes", arns.bucket, ""); err != nil {
		catalogErr(w, err)
		return
	}
	var resp struct {
		TableSummaries []struct {
			Name      string `json:"name"`
			Format    string `json:"format"`
			CreatedAt string `json:"createdAt"`
		} `json:"tables"`
		ContinuationToken string `json:"continuationToken"`
	}
	err = s.internalCatalog(r.Context(), "ListTables", &s3tables.ListTablesRequest{
		TableBucketARN:    s.s3tablesArn(arns.bucket),
		Namespace:         []string{indexNamespace},
		MaxTables:         maxResults,
		ContinuationToken: req.NextToken,
		Prefix:            req.Prefix,
	}, &resp)
	if err != nil {
		catalogErr(w, err)
		return
	}
	// Summaries carry only what the catalog listing knows; a summary that
	// asserted a dimension or metric without a record lookup would lie.
	out := &ListIndexesResponse{NextToken: resp.ContinuationToken, Indexes: []*IndexSummary{}}
	for _, t := range resp.TableSummaries {
		// A non-Lance table in the index namespace is not an index.
		if t.Format != "" && t.Format != s3tables.FormatLance {
			continue
		}
		created, _ := time.Parse(time.RFC3339, t.CreatedAt)
		out.Indexes = append(out.Indexes, &IndexSummary{
			IndexName:        t.Name,
			IndexArn:         s.indexArn(arns.bucket, t.Name),
			VectorBucketName: arns.bucket,
			CreationTime:     created,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleDeleteIndex(w http.ResponseWriter, r *http.Request) {
	var req DeleteIndexRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := s.resolveIndexARNs(req.VectorBucketName, req.VectorBucketArn, req.IndexName, req.IndexArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if _, err := s.authorizeVector(r.Context(), r, "s3vectors:DeleteIndex", arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	// A named table is an index only while it carries the Lance format —
	// an Iceberg or format-less table under the same namespace is not an
	// index and must not be deleted by this operation.
	var record map[string]interface{}
	if err := s.internalCatalog(r.Context(), "GetTable", &s3tables.GetTableRequest{
		TableBucketARN: s.s3tablesArn(arns.bucket),
		Namespace:      []string{indexNamespace},
		Name:           arns.index,
	}, &record); err != nil {
		catalogErr(w, err)
		return
	}
	if format, _ := record["format"].(string); format != s3tables.FormatLance {
		writeError(w, http.StatusBadRequest, "ValidationException",
			fmt.Sprintf("index %q is not a vector index", arns.index))
		return
	}
	// Deleting the catalog record is the delete: the catalog purges the
	// dataset under the index's location along with the entry.
	var resp map[string]interface{}
	err = s.internalCatalog(r.Context(), "DeleteTable", &s3tables.DeleteTableRequest{
		TableBucketARN: s.s3tablesArn(arns.bucket),
		Namespace:      []string{indexNamespace},
		Name:           arns.index,
	}, &resp)
	if err != nil {
		catalogErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &DeleteIndexResponse{})
}

func metadataStruct(m map[string]interface{}) (*structpb.Struct, error) {
	if m == nil {
		return nil, nil
	}
	return structpb.NewStruct(m)
}

func structToMap(s *structpb.Struct) map[string]interface{} {
	if s == nil {
		return nil
	}
	return s.AsMap()
}

func (s *Server) handlePutVectors(w http.ResponseWriter, r *http.Request) {
	var req PutVectorsRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := s.resolveIndexARNs(req.VectorBucketName, req.VectorBucketArn, req.IndexName, req.IndexArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if len(req.Vectors) == 0 || len(req.Vectors) > vectorsMaxPutBatch {
		writeError(w, http.StatusBadRequest, "ValidationException",
			fmt.Sprintf("vectors must contain between 1 and %d items", vectorsMaxPutBatch))
		return
	}
	// A read grant on the index does not carry a write: mutations require the
	// vector write grant, the same check credential vending applies.
	if _, err := s.authorizeVector(r.Context(), r, "s3vectors:PutVectors", arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	record, err := s.indexRecord(r.Context(), arns.bucket, arns.index)
	if err != nil {
		catalogErr(w, err)
		return
	}
	dimension := indexFromRecord(s, arns.bucket, arns.index, record).Dimension
	vectors := make([]*vector_pb.Vector, 0, len(req.Vectors))
	for _, v := range req.Vectors {
		pbVector, err := toPutVector(v, dimension)
		if err != nil {
			writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
		vectors = append(vectors, pbVector)
	}
	_, err = s.vectors.PutVectors(r.Context(), &vector_pb.PutVectorsRequest{
		Table:   tableID(arns.bucket, arns.index),
		Vectors: vectors,
	})
	if err != nil {
		vectorErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &PutVectorsResponse{})
}

func toPutVector(v *PutInputVector, dimension int32) (*vector_pb.Vector, error) {
	if v == nil {
		return nil, fmt.Errorf("vectors contains a null element")
	}
	if v.Key == "" {
		return nil, fmt.Errorf("vector key must not be empty")
	}
	metadata, err := metadataStruct(v.Metadata)
	if err != nil {
		return nil, err
	}
	var data []float32
	if v.Data != nil {
		data = v.Data.Float32
	}
	if int32(len(data)) != dimension {
		return nil, fmt.Errorf("vector %q has %d dimensions, the index declares %d", v.Key, len(data), dimension)
	}
	return &vector_pb.Vector{Key: v.Key, Data: data, Metadata: metadata}, nil
}

func toOutputVectors(vectors []*vector_pb.Vector, returnData, returnMetadata bool) []*OutputVector {
	out := make([]*OutputVector, 0, len(vectors))
	for _, v := range vectors {
		o := &OutputVector{Key: v.Key}
		if returnData {
			o.Data = &VectorData{Float32: v.Data}
		}
		if returnMetadata {
			o.Metadata = structToMap(v.Metadata)
		}
		out = append(out, o)
	}
	return out
}

// vectorKeysBounds enforces the AWS 1..max key list window; GetVectors is
// capped lower than a PutVectors or DeleteVectors batch.
func vectorKeysBounds(keys []string, max int) error {
	if len(keys) == 0 || len(keys) > max {
		return fmt.Errorf("keys must be between 1 and %d", max)
	}
	return nil
}

func (s *Server) handleGetVectors(w http.ResponseWriter, r *http.Request) {
	var req GetVectorsRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if err := vectorKeysBounds(req.Keys, vectorsMaxGetKeys); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := s.resolveIndexARNs(req.VectorBucketName, req.VectorBucketArn, req.IndexName, req.IndexArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if _, err := s.authorizeVector(r.Context(), r, "s3vectors:GetVectors", arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	if _, err := s.indexRecord(r.Context(), arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	resp, err := s.vectors.GetVectors(r.Context(), &vector_pb.GetVectorsRequest{
		Table:          tableID(arns.bucket, arns.index),
		Keys:           req.Keys,
		ReturnData:     req.ReturnData,
		ReturnMetadata: req.ReturnMetadata,
	})
	if err != nil {
		vectorErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &GetVectorsResponse{
		Vectors: toOutputVectors(resp.Vectors, req.ReturnData, req.ReturnMetadata),
	})
}

func (s *Server) handleListVectors(w http.ResponseWriter, r *http.Request) {
	var req ListVectorsRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if err := listVectorsBounds(&req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := s.resolveIndexARNs(req.VectorBucketName, req.VectorBucketArn, req.IndexName, req.IndexArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if _, err := s.authorizeVector(r.Context(), r, "s3vectors:ListVectors", arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	if _, err := s.indexRecord(r.Context(), arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	maxResults := 0
	if req.MaxResults != nil {
		maxResults = *req.MaxResults
	}
	// AWS exposes a static split: segmentIndex/segmentCount divides the full
	// listing. The engine service does not know AWS segments, so page through
	// the index here and keep only the caller's share; a call scans about
	// segmentCount × maxResults rows. The engine's next_token is a row offset,
	// so the caller's token is the offset to resume scanning from.
	if req.SegmentCount != nil {
		var offset uint64
		if req.NextToken != "" {
			var err error
			if offset, err = strconv.ParseUint(req.NextToken, 10, 64); err != nil {
				writeError(w, http.StatusBadRequest, "ValidationException", "invalid nextToken")
				return
			}
		}
		out, nextToken, err := s.listSegmentVectors(r.Context(), tableID(arns.bucket, arns.index),
			offset, maxResults, uint64(*req.SegmentCount), uint64(*req.SegmentIndex), req.ReturnData, req.ReturnMetadata)
		if err != nil {
			vectorErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, &ListVectorsResponse{
			NextToken: nextToken,
			Vectors:   toOutputVectors(out, req.ReturnData, req.ReturnMetadata),
		})
		return
	}
	resp, err := s.vectors.ListVectors(r.Context(), &vector_pb.ListVectorsRequest{
		Table:          tableID(arns.bucket, arns.index),
		MaxResults:     uint32(maxResults),
		NextToken:      req.NextToken,
		ReturnData:     req.ReturnData,
		ReturnMetadata: req.ReturnMetadata,
	})
	if err != nil {
		vectorErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &ListVectorsResponse{
		NextToken: resp.NextToken,
		Vectors:   toOutputVectors(resp.Vectors, req.ReturnData, req.ReturnMetadata),
	})
}

// The page constants mirror the engine's page sizing so a segmented listing
// pages like a plain one does; vectorsMaxPage is also the AWS ListVectors
// maxResults cap, and vectorsMaxTopK the AWS QueryVectors limit.
const (
	vectorsDefaultPage = 500
	vectorsMaxGetKeys  = 100
	vectorsMaxPutBatch = 500
	vectorsMaxPage     = 1000
	vectorsMaxListPage = 500
	vectorsMaxTopK     = 10000
	vectorsMaxSegments = 16
)

// listVectorsBounds enforces the AWS ListVectors windows: maxResults 1..1000,
// segmentCount 1..16, and segmentIndex below it, the two given together.
func listVectorsBounds(req *ListVectorsRequest) error {
	if req.MaxResults != nil && (*req.MaxResults < 1 || *req.MaxResults > vectorsMaxPage) {
		return fmt.Errorf("maxResults must be between 1 and %d", vectorsMaxPage)
	}
	if (req.SegmentCount == nil) != (req.SegmentIndex == nil) {
		return fmt.Errorf("segmentCount and segmentIndex must be specified together")
	}
	if req.SegmentCount == nil {
		return nil
	}
	if *req.SegmentCount < 1 || *req.SegmentCount > vectorsMaxSegments {
		return fmt.Errorf("segmentCount must be between 1 and %d", vectorsMaxSegments)
	}
	if *req.SegmentIndex < 0 || *req.SegmentIndex >= *req.SegmentCount {
		return fmt.Errorf("segmentIndex must be in [0, segmentCount)")
	}
	return nil
}

// listSegmentVectors scans the index from offset, keeping the rows that fall
// in the caller's segment until limit are collected or the index runs out.
// The engine's next_token is a row offset, so the returned token is the offset
// scanning resumes from.
func (s *Server) listSegmentVectors(ctx context.Context, table *vector_pb.Table, offset uint64, maxResults int, segmentCount, segmentIndex uint64, returnData, returnMetadata bool) ([]*vector_pb.Vector, string, error) {
	limit := maxResults
	if limit <= 0 {
		limit = vectorsDefaultPage
	} else if limit > vectorsMaxPage {
		limit = vectorsMaxPage
	}
	var out []*vector_pb.Vector
	nextToken := ""
	token := strconv.FormatUint(offset, 10)
	for {
		page, err := s.vectors.ListVectors(ctx, &vector_pb.ListVectorsRequest{
			Table:          table,
			MaxResults:     vectorsMaxPage,
			NextToken:      token,
			ReturnData:     returnData,
			ReturnMetadata: returnMetadata,
		})
		if err != nil {
			return nil, "", err
		}
		base := offset
		offset += uint64(len(page.Vectors))
		full := false
		for j, v := range page.Vectors {
			if (base+uint64(j))%segmentCount == segmentIndex {
				out = append(out, v)
				if len(out) == limit {
					if j+1 < len(page.Vectors) || page.NextToken != "" {
						nextToken = strconv.FormatUint(base+uint64(j)+1, 10)
					}
					full = true
					break
				}
			}
		}
		if full || len(page.Vectors) == 0 || page.NextToken == "" {
			break
		}
		token = page.NextToken
	}
	return out, nextToken, nil
}

func (s *Server) handleDeleteVectors(w http.ResponseWriter, r *http.Request) {
	var req DeleteVectorsRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if err := vectorKeysBounds(req.Keys, vectorsMaxPutBatch); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := s.resolveIndexARNs(req.VectorBucketName, req.VectorBucketArn, req.IndexName, req.IndexArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if _, err := s.authorizeVector(r.Context(), r, "s3vectors:DeleteVectors", arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	if _, err := s.indexRecord(r.Context(), arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	_, err = s.vectors.DeleteVectors(r.Context(), &vector_pb.DeleteVectorsRequest{
		Table: tableID(arns.bucket, arns.index),
		Keys:  req.Keys,
	})
	if err != nil {
		vectorErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &DeleteVectorsResponse{})
}

// translateFilter renders an AWS S3 Vectors filter document as a Lance SQL
// predicate over the metadata JSON column. Only the documented operators are
// translated; anything else fails loudly instead of silently overmatching.
func translateFilter(filter map[string]interface{}) (string, error) {
	if len(filter) == 0 {
		return "", nil
	}
	return filterExpr(filter)
}

// nonFilterableField returns the first blocked metadata key a filter
// references; the bool reports whether any matched.
func nonFilterableField(cfg *MetadataConfiguration, filter map[string]interface{}) (string, bool) {
	if cfg == nil {
		return "", false
	}
	fields := map[string]bool{}
	filterFields(filter, fields)
	for _, k := range cfg.NonFilterableMetadataKeys {
		if fields[k] {
			return k, true
		}
	}
	return "", false
}

// filterFields collects the field names a filter expression references —
// $-operators recurse; every other key is a field.
func filterFields(filter map[string]interface{}, out map[string]bool) {
	for key, cond := range filter {
		if strings.HasPrefix(key, "$") {
			if sub, ok := cond.([]interface{}); ok {
				for _, item := range sub {
					if m, ok := item.(map[string]interface{}); ok {
						filterFields(m, out)
					}
				}
			}
			continue
		}
		out[key] = true
	}
}

func filterExpr(filter map[string]interface{}) (string, error) {
	parts := make([]string, 0, len(filter))
	for key, cond := range filter {
		if strings.HasPrefix(key, "$") {
			sub, ok := cond.([]interface{})
			if !ok {
				return "", fmt.Errorf("%s expects a list", key)
			}
			if len(sub) == 0 {
				return "", fmt.Errorf("%s expects a non-empty list", key)
			}
			exprs := make([]string, 0, len(sub))
			for _, item := range sub {
				m, ok := item.(map[string]interface{})
				if !ok {
					return "", fmt.Errorf("%s expects objects", key)
				}
				e, err := filterExpr(m)
				if err != nil {
					return "", err
				}
				if e == "" {
					return "", fmt.Errorf("%s expects non-empty objects", key)
				}
				exprs = append(exprs, e)
			}
			joiner := " AND "
			if key == "$or" {
				joiner = " OR "
			}
			if key != "$and" && key != "$or" {
				return "", fmt.Errorf("unsupported operator %s", key)
			}
			parts = append(parts, "("+strings.Join(exprs, joiner)+")")
			continue
		}
		clause, err := fieldClause(key, cond)
		if err != nil {
			return "", err
		}
		parts = append(parts, clause)
	}
	return strings.Join(parts, " AND "), nil
}

// fieldAccess renders the operand's comparison column over the serialized
// json_extract text. Strings and booleans compare the serialized form
// directly, which is exact-typed and never errors — booleans serialize as
// exactly 'true'/'false', and a cast would also accept the '1'/'0' a stored
// number produces. Numbers go through TRY_CAST, which is NULL on any other
// stored type. The json_get_* extractors are not used — they convert
// numeric-looking strings and booleans, and error on mismatched types.
func fieldAccess(field string, operand interface{}) (string, error) {
	path := sqlEscape(existsPath(field))
	switch operand.(type) {
	case float64:
		return fmt.Sprintf("TRY_CAST(json_extract(`metadata`, '%s') AS DOUBLE)", path), nil
	case bool, string:
		return fmt.Sprintf("json_extract(`metadata`, '%s')", path), nil
	default:
		return "", fmt.Errorf("unsupported filter operand %v", operand)
	}
}

// fieldMatch renders "field <rhs>".
func fieldMatch(field string, operand interface{}, rhs string) (string, error) {
	col, err := fieldAccess(field, operand)
	if err != nil {
		return "", err
	}
	return col + " " + rhs, nil
}

// existsPath renders the field as a JSONPath for the json_exists and
// json_extract parsers: bracket notation keeps dots, spaces, and other
// specials inside the key literal.
func existsPath(field string) string {
	field = strings.ReplaceAll(field, `\`, `\\`)
	field = strings.ReplaceAll(field, `"`, `\"`)
	return `$["` + field + `"]`
}

func sqlEscape(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func fieldClause(field string, cond interface{}) (string, error) {
	switch c := cond.(type) {
	case map[string]interface{}:
		parts := []string{}
		for op, operand := range c {
			switch op {
			case "$exists":
				if b, ok := operand.(bool); ok {
					expr := fmt.Sprintf("json_exists(`metadata`, '%s')", sqlEscape(existsPath(field)))
					if !b {
						expr = "NOT " + expr
					}
					parts = append(parts, expr)
				} else {
					return "", fmt.Errorf("$exists expects a boolean")
				}
			case "$eq", "$ne":
				lit, err := sqlLiteral(operand)
				if err != nil {
					return "", err
				}
				match, err := fieldMatch(field, operand, "= "+lit)
				if err != nil {
					return "", err
				}
				if op == "$ne" {
					match = "NOT COALESCE(" + match + ", false)"
				}
				parts = append(parts, match)
			case "$in", "$nin":
				list, ok := operand.([]interface{})
				if !ok {
					return "", fmt.Errorf("%s expects a list", op)
				}
				if len(list) == 0 {
					if op == "$in" {
						parts = append(parts, "false")
					} else {
						parts = append(parts, "true")
					}
					continue
				}
				// Members may mix types; each extractor only sees its own. A
				// member whose typed extractor returns NULL must not let NOT
				// invert NULL into NULL under $nin, so every group coalesces.
				var groups []string
				for _, item := range list {
					lit, err := sqlLiteral(item)
					if err != nil {
						return "", err
					}
					g, err := fieldMatch(field, item, "in ("+lit+")")
					if err != nil {
						return "", err
					}
					groups = append(groups, fmt.Sprintf("COALESCE(%s, false)", g))
				}
				expr := "(" + strings.Join(groups, " OR ") + ")"
				if op == "$nin" {
					expr = "NOT " + expr
				}
				parts = append(parts, expr)
			case "$lt", "$lte", "$gt", "$gte":
				if _, ok := operand.(float64); !ok {
					return "", fmt.Errorf("%s expects a number", op)
				}
				lit, err := sqlLiteral(operand)
				if err != nil {
					return "", err
				}
				sym := map[string]string{"$lt": "<", "$lte": "<=", "$gt": ">", "$gte": ">="}[op]
				col, err := fieldAccess(field, operand)
				if err != nil {
					return "", err
				}
				parts = append(parts, fmt.Sprintf("%s %s %s", col, sym, lit))
			default:
				return "", fmt.Errorf("unsupported operator %s", op)
			}
		}
		if len(parts) == 0 {
			return "", fmt.Errorf("%s expects at least one operator", field)
		}
		return strings.Join(parts, " AND "), nil
	default:
		lit, err := sqlLiteral(cond)
		if err != nil {
			return "", err
		}
		return fieldMatch(field, cond, "= "+lit)
	}
}

// jsonbString renders s as the JSON text json_extract returns for a stored
// string: quoted, with the same escapes the jsonb serializer emits.
func jsonbString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case 0:
			b.WriteString(`\0`)
		default:
			if r < 0x20 || (r >= 0x7f && r <= 0x9f) {
				fmt.Fprintf(&b, `\u{%x}`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

func sqlLiteral(v interface{}) (string, error) {
	switch t := v.(type) {
	case string:
		return "'" + sqlEscape(jsonbString(t)) + "'", nil
	case bool:
		if t {
			return "'true'", nil
		}
		return "'false'", nil
	case float64:
		return fmt.Sprintf("%v", t), nil
	}
	return "", fmt.Errorf("unsupported filter operand %v", v)
}

func (s *Server) handleQueryVectors(w http.ResponseWriter, r *http.Request) {
	var req QueryVectorsRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, err := s.resolveIndexARNs(req.VectorBucketName, req.VectorBucketArn, req.IndexName, req.IndexArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	var queryVector []float32
	if req.QueryVector != nil {
		queryVector = req.QueryVector.Float32
	}
	if len(queryVector) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "queryVector is required")
		return
	}
	if req.TopK <= 0 || req.TopK > vectorsMaxTopK {
		writeError(w, http.StatusBadRequest, "ValidationException",
			fmt.Sprintf("topK must be between 1 and %d", vectorsMaxTopK))
		return
	}
	if _, err := s.authorizeVector(r.Context(), r, "s3vectors:QueryVectors", arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	record, err := s.indexRecord(r.Context(), arns.bucket, arns.index)
	if err != nil {
		catalogErr(w, err)
		return
	}
	idx := indexFromRecord(s, arns.bucket, arns.index, record)
	if int32(len(queryVector)) != idx.Dimension {
		writeError(w, http.StatusBadRequest, "ValidationException",
			fmt.Sprintf("queryVector has %d dimensions, the index declares %d", len(queryVector), idx.Dimension))
		return
	}
	if key, blocked := nonFilterableField(idx.MetadataConfiguration, req.Filter); blocked {
		writeError(w, http.StatusBadRequest, "ValidationException",
			fmt.Sprintf("metadata key %q is not filterable", key))
		return
	}
	metric := idx.DistanceMetric
	filter, err := translateFilter(req.Filter)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	resp, err := s.vectors.QueryVectors(r.Context(), &vector_pb.QueryVectorsRequest{
		Table:          tableID(arns.bucket, arns.index),
		QueryVector:    queryVector,
		TopK:           uint32(req.TopK),
		Filter:         filter,
		ReturnMetadata: req.ReturnMetadata,
		ReturnDistance: req.ReturnDistance,
	})
	if err != nil {
		vectorErr(w, err)
		return
	}
	out := &QueryVectorsResponse{DistanceMetric: metric, Vectors: []*QueryOutputVector{}}
	for _, v := range resp.Vectors {
		o := &QueryOutputVector{Key: v.Key}
		if req.ReturnDistance {
			o.Distance = &v.Distance
		}
		if req.ReturnMetadata {
			o.Metadata = structToMap(v.Metadata)
		}
		out.Vectors = append(out.Vectors, o)
	}
	writeJSON(w, http.StatusOK, out)
}
