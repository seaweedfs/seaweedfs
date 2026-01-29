package s3api

import (
	"net/http"
	"strings"

	"github.com/gorilla/mux"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// s3TablesActionsMap contains all valid S3 Tables operations for O(1) lookup
var s3TablesActionsMap = map[string]struct{}{
	"CreateTableBucket":       {},
	"GetTableBucket":          {},
	"ListTableBuckets":        {},
	"DeleteTableBucket":       {},
	"PutTableBucketPolicy":    {},
	"GetTableBucketPolicy":    {},
	"DeleteTableBucketPolicy": {},
	"CreateNamespace":         {},
	"GetNamespace":            {},
	"ListNamespaces":          {},
	"DeleteNamespace":         {},
	"CreateTable":             {},
	"GetTable":                {},
	"ListTables":              {},
	"DeleteTable":             {},
	"PutTablePolicy":          {},
	"GetTablePolicy":          {},
	"DeleteTablePolicy":       {},
	"TagResource":             {},
	"ListTagsForResource":     {},
	"UntagResource":           {},
}

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

	// S3 Tables API uses POST with x-amz-target header
	// The AWS CLI sends requests with:
	// - Content-Type: application/x-amz-json-1.1
	// - X-Amz-Target: S3Tables.<OperationName>

	// Matcher function to identify S3 Tables requests
	s3TablesMatcher := func(r *http.Request, rm *mux.RouteMatch) bool {
		// Check for X-Amz-Target header with S3Tables prefix
		target := r.Header.Get("X-Amz-Target")
		if target != "" && strings.HasPrefix(target, "S3Tables.") {
			return true
		}

		// Also check for specific S3 Tables actions in query string (CLI fallback)
		action := r.URL.Query().Get("Action")
		if isS3TablesAction(action) {
			return true
		}

		return false
	}

	// Register the S3 Tables handler wrapped with IAM authentication
	router.Methods(http.MethodPost).Path("/").MatcherFunc(s3TablesMatcher).
		HandlerFunc(track(s3a.authenticateS3Tables(func(w http.ResponseWriter, r *http.Request) {
			s3TablesApi.S3TablesHandler(w, r)
		}), "S3Tables"))

	glog.V(1).Infof("S3 Tables API enabled")
}

// isS3TablesAction checks if the action is an S3 Tables operation using O(1) map lookup
func isS3TablesAction(action string) bool {
	_, ok := s3TablesActionsMap[action]
	return ok
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
