// Package s3vectors serves the AWS S3 Vectors API on top of the shared
// VectorService gRPC boundary and the S3 Tables catalog.
package s3vectors

import (
	"context"
	"encoding/json"
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
	"golang.org/x/sync/semaphore"
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
	filerClient      FilerClient
	tables           *s3tables.Manager
	internal         *s3tables.Manager
	vectors          vector_pb.VectorServiceClient
	authenticator    S3Authenticator
	region           string
	accountID        string
	limit            func(http.HandlerFunc, string) http.HandlerFunc
	requestSlots     *semaphore.Weighted
	requestBytes     *semaphore.Weighted
	requestByteLimit int64
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
	server := &Server{
		filerClient:   filerClient,
		tables:        manager,
		internal:      internal,
		vectors:       vectors,
		authenticator: authenticator,
	}
	server.SetRequestLimits(0, 0)
	return server
}

// SetRegion configures the region embedded in the ARNs this server mints.
func (s *Server) SetRegion(region string) { s.region = region }

// SetAccountID configures the account id embedded in the ARNs this server mints.
func (s *Server) SetAccountID(id string) { s.accountID = id }

// SetEncryptionChecker tells CreateVectorBucket whether the gateway can
// apply the AES256 a bucket declares.
func (s *Server) SetEncryptionChecker(fn func() bool) { s.internal.SetEncryptionChecker(fn) }

// RegisterRoutes mounts POST / dispatching on the X-Amz-Target header,
// POST /{Operation} for clients that route per-path, and the tagging calls on
// /tags/{resourceArn}. Metrics wrap Auth like the S3 track middleware so
// rejected requests still count toward the operation.
func (s *Server) RegisterRoutes(router *mux.Router) {
	handler := s.instrument(s.Auth(s.dispatch))
	router.HandleFunc("/", handler).Methods(http.MethodPost)
	router.HandleFunc(tagsPathPrefix+"{resourceArn:.+}", handler).Methods(http.MethodGet, http.MethodPost, http.MethodDelete)
	router.HandleFunc("/{op}", handler).Methods(http.MethodPost)
}

// operationFromRequest resolves the operation the way dispatch does: the
// method on a /tags/ path, else the last path segment, or the X-Amz-Target
// suffix when the path is bare.
func operationFromRequest(r *http.Request) string {
	if strings.HasPrefix(r.URL.Path, tagsPathPrefix) {
		return tagOperation(r.Method)
	}
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
		r = s3err.WithAccessLogOperation(r, label)
		info := &requestInfo{}
		r = s3_constants.EnsureIdentityHolder(s3err.EnsureAuditTracking(r.WithContext(context.WithValue(r.Context(), requestInfoKey{}, info))))
		recorder := &responseWriter{StatusRecorder: stats_collect.NewStatusResponseWriter(w), info: info}
		start := time.Now()
		next(recorder, r)
		elapsed := time.Since(start)
		bucket := ""
		if info.authorized && recorder.Status != http.StatusForbidden {
			bucket = info.bucket
		}
		log := accessLog(r, recorder.Status, label, info)
		if !s3err.AuditAlreadyLogged(r) {
			s3err.PostAccessLog(*log)
			s3err.MarkAuditLogged(r)
		}
		stats_collect.S3RequestHistogram.WithLabelValues(label, bucket).Observe(elapsed.Seconds())
		stats_collect.S3RequestCounter.WithLabelValues(label, strconv.Itoa(recorder.Status), bucket).Inc()
		if bucket != "" {
			stats_collect.RecordBucketActiveTime(bucket)
			stats_collect.S3BucketTrafficReceivedBytesCounter.WithLabelValues(bucket).Add(float64(info.received))
			stats_collect.S3BucketTrafficSentBytesCounter.WithLabelValues(bucket).Add(float64(info.sent))
		}
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
	if s.limit != nil {
		s.limitRequest(func(w http.ResponseWriter, r *http.Request) { handler(s, w, r) }, w, r)
		return
	}
	handler(s, w, r)
}

// Auth authenticates the caller and puts the identity in the request context.
func (s *Server) Auth(handler http.HandlerFunc) http.HandlerFunc {
	if s.requestSlots == nil {
		s.SetRequestLimits(0, 0)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > maxRequestBodyBytes {
			writeError(w, http.StatusBadRequest, "ValidationException", errRequestBodyTooLarge.Error())
			return
		}
		if err := s.requestSlots.Acquire(r.Context(), 1); err != nil {
			writeError(w, http.StatusRequestTimeout, "RequestTimeout", err.Error())
			return
		}
		defer s.requestSlots.Release(1)
		var size int64
		if r.Body != nil && r.Body != http.NoBody {
			size = r.ContentLength
			if size <= 0 {
				size = maxRequestBodyBytes
			}
		}
		size = min(size, s.requestByteLimit)
		if err := s.requestBytes.Acquire(r.Context(), size); err != nil {
			writeError(w, http.StatusRequestTimeout, "RequestTimeout", err.Error())
			return
		}
		defer s.requestBytes.Release(size)
		r = r.WithContext(s3_constants.SetRequestBodyLimitInContext(r.Context(), maxRequestBodyBytes))
		identity, identityObj, errCode := s.authenticator.AuthenticateRequest(r)
		switch errCode {
		case s3err.ErrNone:
		case s3err.ErrEntityTooLarge:
			writeError(w, http.StatusBadRequest, "ValidationException", errRequestBodyTooLarge.Error())
			return
		default:
			writeError(w, http.StatusForbidden, "AccessDenied", "authentication failed")
			return
		}
		r = r.WithContext(s3_constants.SetIdentityInContext(
			s3_constants.SetIdentityNameInContext(r.Context(), identity), identityObj))
		handler(w, r)
	}
}

type requestInfoKey struct{}

type requestInfo struct {
	bucket, index  string
	authorized     bool
	received, sent int64
	errorCode      string
	body           []byte
}

func authorizeRequest(r *http.Request, bucket, index string) {
	if info, ok := r.Context().Value(requestInfoKey{}).(*requestInfo); ok {
		info.bucket, info.index, info.authorized = bucket, index, true
	}
}

type responseWriter struct {
	*stats_collect.StatusRecorder
	info *requestInfo
}

func (w *responseWriter) Write(body []byte) (int, error) {
	n, err := w.StatusRecorder.Write(body)
	w.info.sent += int64(n)
	if w.Status >= http.StatusBadRequest {
		var response Error
		if json.Unmarshal(body, &response) == nil {
			w.info.errorCode = response.Code
		}
	}
	return n, err
}

func setRequestTarget(r *http.Request, bucket, index string) {
	if info, ok := r.Context().Value(requestInfoKey{}).(*requestInfo); ok {
		info.bucket, info.index = bucket, index
	}
}

func accessLog(r *http.Request, status int, operation string, info *requestInfo) *s3err.AccessLog {
	log := s3err.GetAccessLog(r, status, s3err.ErrNone)
	log.Operation, log.Bucket, log.Key = operation, info.bucket, info.index
	log.ErrorCode = info.errorCode
	return log
}
