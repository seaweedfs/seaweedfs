package s3api

import (
	"net/http"
	"strings"

	"github.com/gorilla/mux"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	. "github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// S3TablesApiServer wraps the S3 Tables handler with S3ApiServer's filer access
type S3TablesApiServer struct {
	s3a     *S3ApiServer
	handler *s3tables.S3TablesHandler
}

// NewS3TablesApiServer creates a new S3 Tables API server
func NewS3TablesApiServer(s3a *S3ApiServer) *S3TablesApiServer {
	filerAddr := ""
	if len(s3a.option.Filers) > 0 {
		filerAddr = string(s3a.option.Filers[0])
	}

	return &S3TablesApiServer{
		s3a:     s3a,
		handler: s3tables.NewS3TablesHandler(filerAddr),
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

	// Register the S3 Tables handler
	router.Methods(http.MethodPost).Path("/").MatcherFunc(s3TablesMatcher).
		HandlerFunc(track(s3a.iam.Auth(func(w http.ResponseWriter, r *http.Request) {
			s3TablesApi.S3TablesHandler(w, r)
		}, ACTION_ADMIN), "S3Tables"))

	glog.V(1).Infof("S3 Tables API enabled")
}

// isS3TablesAction checks if the action is an S3 Tables operation
func isS3TablesAction(action string) bool {
	s3TablesActions := []string{
		"CreateTableBucket",
		"GetTableBucket",
		"ListTableBuckets",
		"DeleteTableBucket",
		"PutTableBucketPolicy",
		"GetTableBucketPolicy",
		"DeleteTableBucketPolicy",
		"CreateNamespace",
		"GetNamespace",
		"ListNamespaces",
		"DeleteNamespace",
		"CreateTable",
		"GetTable",
		"ListTables",
		"DeleteTable",
		"PutTablePolicy",
		"GetTablePolicy",
		"DeleteTablePolicy",
		"TagResource",
		"ListTagsForResource",
		"UntagResource",
	}

	for _, a := range s3TablesActions {
		if a == action {
			return true
		}
	}
	return false
}
