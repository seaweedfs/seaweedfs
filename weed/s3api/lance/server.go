package lance

import (
	"context"
	"net/http"
	"time"

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
	DefaultAllow() bool
}

// VendedCredentials are short-lived S3 credentials scoped to one table.
type VendedCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Expiration      time.Time
}

// CredentialVendor mints credentials limited to a single table's prefix for a
// caller the catalog has already authenticated and authorized. A nil result with
// no error means the deployment has vending switched off.
type CredentialVendor interface {
	VendTableCredentials(ctx context.Context, principal, bucket, prefix string) (*VendedCredentials, error)
}

// Server implements the Lance Namespace REST spec.
type Server struct {
	filerClient       FilerClient
	tablesManager     *s3tables.Manager
	authenticator     S3Authenticator
	credentialVendor  CredentialVendor
	s3Endpoint        string
	s3Region          string
	managedVersioning bool
}

// NewServer creates a Lance namespace server over the given filer.
func NewServer(filerClient FilerClient, authenticator S3Authenticator) *Server {
	manager := s3tables.NewManager()
	// Mirror the S3 port: fall open by default only when the gateway itself is
	// open, so an authenticated caller still passes the normal permission check.
	if authenticator != nil {
		manager.SetDefaultAllow(authenticator.DefaultAllow())
	}
	return &Server{
		filerClient:   filerClient,
		tablesManager: manager,
		authenticator: authenticator,
	}
}

// SetCredentialVendor enables storage_options credential vending for clients
// that ask for it with vend_credentials.
func (s *Server) SetCredentialVendor(vendor CredentialVendor) {
	s.credentialVendor = vendor
}

// SetS3Endpoint configures the S3 endpoint advertised in storage_options so a
// client can reach the dataset without separately discovering the S3 address.
func (s *Server) SetS3Endpoint(endpoint string) {
	s.s3Endpoint = endpoint
}

// SetManagedVersioning makes the namespace the external manifest store for its
// tables: commits reserve a version here before the manifest is finalised, which
// is a real put-if-not-exists. It is off by default because turning it on moves
// where a table's version history lives, so a reader that does not go through
// this namespace no longer sees the whole picture.
func (s *Server) SetManagedVersioning(enabled bool) {
	s.managedVersioning = enabled
}

// SetS3Region configures the region advertised in storage_options.
func (s *Server) SetS3Region(region string) {
	s.s3Region = region
}

// RegisterRoutes registers the Lance Namespace REST routes.
//
// The spec puts the identifier in the path rather than the body so a reverse
// proxy can route and authorize without deserializing the request.
func (s *Server) RegisterRoutes(router *mux.Router) {
	router.Use(loggingMiddleware)

	router.HandleFunc("/v1/namespace/{id}/create", s.Auth(s.handleCreateNamespace)).Methods(http.MethodPost)
	router.HandleFunc("/v1/namespace/{id}/list", s.Auth(s.handleListNamespaces)).Methods(http.MethodGet)
	router.HandleFunc("/v1/namespace/{id}/describe", s.Auth(s.handleDescribeNamespace)).Methods(http.MethodPost)
	router.HandleFunc("/v1/namespace/{id}/drop", s.Auth(s.handleDropNamespace)).Methods(http.MethodPost)
	router.HandleFunc("/v1/namespace/{id}/exists", s.Auth(s.handleNamespaceExists)).Methods(http.MethodPost)
	router.HandleFunc("/v1/namespace/{id}/table/list", s.Auth(s.handleListTables)).Methods(http.MethodGet)

	router.HandleFunc("/v1/table", s.Auth(s.handleListAllTables)).Methods(http.MethodGet)
	router.HandleFunc("/v1/table/{id}/declare", s.Auth(s.handleDeclareTable)).Methods(http.MethodPost)
	router.HandleFunc("/v1/table/{id}/describe", s.Auth(s.handleDescribeTable)).Methods(http.MethodPost)
	router.HandleFunc("/v1/table/{id}/exists", s.Auth(s.handleTableExists)).Methods(http.MethodPost)
	router.HandleFunc("/v1/table/{id}/register", s.Auth(s.handleRegisterTable)).Methods(http.MethodPost)
	router.HandleFunc("/v1/table/{id}/deregister", s.Auth(s.handleDeregisterTable)).Methods(http.MethodPost)
	router.HandleFunc("/v1/table/{id}/drop", s.Auth(s.handleDropTable)).Methods(http.MethodPost)
	router.HandleFunc("/v1/table/{id}/rename", s.Auth(s.handleRenameTable)).Methods(http.MethodPost)

	// The data plane needs Lance format support that does not exist in Go. Say
	// so with the spec's own code instead of returning a bare 404.
	for _, action := range []string{"create", "insert", "merge_insert", "update", "delete",
		"query", "count_rows", "explain_plan", "analyze_plan", "restore",
		"add_columns", "alter_columns", "drop_columns", "backfill_column",
		"create_index", "create_scalar_index", "stats", "schema_metadata/update"} {
		router.HandleFunc("/v1/table/{id}/"+action, s.Auth(s.handleUnsupported)).Methods(http.MethodPost)
	}
	router.HandleFunc("/v1/table/{id}/version/create", s.Auth(s.handleCreateTableVersion)).Methods(http.MethodPost)
	router.HandleFunc("/v1/table/{id}/version/list", s.Auth(s.handleListTableVersions)).Methods(http.MethodPost)
	router.HandleFunc("/v1/table/{id}/version/describe", s.Auth(s.handleDescribeTableVersion)).Methods(http.MethodPost)
	router.HandleFunc("/v1/table/{id}/version/delete", s.Auth(s.handleBatchDeleteTableVersions)).Methods(http.MethodPost)

	for _, action := range []string{
		"index/list", "tags/list", "tags/version", "tags/create", "tags/delete", "tags/update",
		"branches/list", "branches/create", "branches/delete"} {
		router.HandleFunc("/v1/table/{id}/"+action, s.Auth(s.handleUnsupported)).Methods(http.MethodPost)
	}

	router.PathPrefix("/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		glog.V(2).Infof("lance: no route for %s %s", r.Method, r.RequestURI)
		writeError(w, r, http.StatusNotFound, codeUnsupported, "no such operation")
	})

	glog.V(2).Infof("Registered Lance Namespace routes")
}

func (s *Server) handleUnsupported(w http.ResponseWriter, r *http.Request) {
	writeError(w, r, http.StatusNotImplemented, codeUnsupported,
		"this namespace records table metadata only; run data operations through a Lance client against the table location")
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		glog.V(2).Infof("lance request: %s %s from %s", r.Method, r.RequestURI, r.RemoteAddr)
		next.ServeHTTP(w, r)
	})
}

// Auth authenticates the caller and puts the identity in the request context.
// The Lance spec maps identity onto the same headers the S3 authenticator
// already understands, so SigV4 and bearer tokens both keep working.
func (s *Server) Auth(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.authenticator == nil {
			writeError(w, r, http.StatusUnauthorized, codeUnauthenticated, "authentication required")
			return
		}

		identityName, identity, errCode := s.authenticator.AuthenticateRequest(r)
		if errCode != s3err.ErrNone {
			if !s.authenticator.DefaultAllow() {
				apiErr := s3err.GetAPIError(errCode)
				code := codeInternal
				switch apiErr.HTTPStatusCode {
				case http.StatusForbidden:
					code = codePermissionDenied
				case http.StatusUnauthorized:
					code = codeUnauthenticated
				case http.StatusBadRequest:
					code = codeInvalidInput
				}
				writeError(w, r, apiErr.HTTPStatusCode, code, apiErr.Description)
				return
			}
			glog.V(2).Infof("lance: authentication failed (%v) but the gateway is open, proceeding", errCode)
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
