package iceberg

import (
	"net/http"

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
			// If authentication failed but DefaultAllow is enabled, proceed without identity
			if s.authenticator.DefaultAllow() {
				glog.V(2).Infof("Iceberg: AuthenticateRequest failed (%v), but DefaultAllow is true, proceeding", errCode)
			} else {
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
