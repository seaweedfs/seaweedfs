// Package s3vectors serves the AWS S3 Vectors API on top of the shared
// VectorService gRPC boundary and the S3 Tables catalog.
package s3vectors

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
)

// FilerClient provides access to the filer for storage operations.
type FilerClient interface {
	WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error
}

// S3Authenticator authenticates a request, returning the caller identity.
type S3Authenticator interface {
	AuthenticateRequest(r *http.Request) (string, interface{}, s3err.ErrorCode)
	DefaultAllow() bool
}

// Server implements the AWS S3 Vectors API.
type Server struct {
	filerClient   FilerClient
	tables        *s3tables.Manager
	internal      *s3tables.Manager
	vectors       vector_pb.VectorServiceClient
	authenticator S3Authenticator
	region        string
	accountID     string
}

// NewServer creates an S3 Vectors server over the given filer and catalog.
func NewServer(filerClient FilerClient, authenticator S3Authenticator, vectors vector_pb.VectorServiceClient) *Server {
	manager := s3tables.NewManager()
	if authenticator != nil {
		manager.SetDefaultAllow(authenticator.DefaultAllow())
	}
	// Bookkeeping calls that ride along an operation the catalog already
	// authorizes — format lookups, the implicit index namespace — must not
	// demand read grants the API does not require.
	internal := s3tables.NewManager()
	internal.SetTrusted(true)
	return &Server{
		filerClient:   filerClient,
		tables:        manager,
		internal:      internal,
		vectors:       vectors,
		authenticator: authenticator,
	}
}

// SetRegion configures the region embedded in the ARNs this server mints.
func (s *Server) SetRegion(region string) { s.region = region }

// SetAccountID configures the account id embedded in the ARNs this server mints.
func (s *Server) SetAccountID(id string) { s.accountID = id }

// SetEncryptionChecker tells CreateVectorBucket whether the gateway can
// apply the AES256 a bucket declares.
func (s *Server) SetEncryptionChecker(fn func() bool) { s.internal.SetEncryptionChecker(fn) }

// RegisterRoutes mounts POST / dispatching on the X-Amz-Target header and
// POST /{Operation} for clients that route per-path. Metrics wrap Auth like the
// S3 track middleware so rejected requests still count toward the operation.
func (s *Server) RegisterRoutes(router *mux.Router) {
	router.HandleFunc("/", s.instrument(s.Auth(s.dispatch))).Methods(http.MethodPost)
	router.HandleFunc("/{op}", s.instrument(s.Auth(s.dispatch))).Methods(http.MethodPost)
}

// operationFromRequest resolves the operation the way dispatch does: the last
// path segment, or the X-Amz-Target suffix when the path is bare.
func operationFromRequest(r *http.Request) string {
	op := strings.Trim(r.URL.Path, "/")
	if i := strings.LastIndex(op, "/"); i >= 0 {
		op = op[i+1:]
	}
	if op == "" {
		target := r.Header.Get("X-Amz-Target")
		op = target[strings.LastIndex(target, ".")+1:]
	}
	return op
}

// instrument records in-flight, latency, and status for a known operation;
// unknown operations stay out of the metric labels.
func (s *Server) instrument(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		op := operationFromRequest(r)
		if _, ok := handlers[op]; !ok {
			next(w, r)
			return
		}
		label := "s3vectors." + op
		inFlight := stats_collect.S3InFlightRequestsGauge.WithLabelValues(label)
		inFlight.Inc()
		defer inFlight.Dec()
		recorder := stats_collect.NewStatusResponseWriter(w)
		start := time.Now()
		next(recorder, r)
		elapsed := time.Since(start)
		stats_collect.S3RequestHistogram.WithLabelValues(label, "").Observe(elapsed.Seconds())
		stats_collect.S3RequestCounter.WithLabelValues(label, strconv.Itoa(recorder.Status), "").Inc()
		stats_collect.RecordBucketActiveTime("")
	}
}

func (s *Server) dispatch(w http.ResponseWriter, r *http.Request) {
	// Clients carry the operation in the X-Amz-Target header or in the path —
	// POST /QueryVectors and POST / with a target mean the same call here.
	op := operationFromRequest(r)
	handler, ok := handlers[op]
	if !ok {
		writeError(w, http.StatusBadRequest, "ValidationException", "unknown operation "+op)
		return
	}
	handler(s, w, r)
}

// Auth authenticates the caller and puts the identity in the request context.
func (s *Server) Auth(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		identity, identityObj, errCode := s.authenticator.AuthenticateRequest(r)
		if errCode != s3err.ErrNone {
			writeError(w, http.StatusForbidden, "AccessDenied", "authentication failed")
			return
		}
		r = r.WithContext(s3_constants.SetIdentityInContext(
			s3_constants.SetIdentityNameInContext(r.Context(), identity), identityObj))
		handler(w, r)
	}
}
