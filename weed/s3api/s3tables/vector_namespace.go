package s3tables

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

// VectorIndexNamespace holds a vector bucket's indexes. Only the S3 Vectors
// gateway, through a trusted Manager, may change it in a LANCE bucket.
const VectorIndexNamespace = "vector"

const vectorNamespaceReadOnly = "the vector namespace of a vector bucket is read-only; manage its indexes through S3 Vectors"

var errVectorNamespaceReadOnly = errors.New(vectorNamespaceReadOnly)

func isVectorIndexNamespace(namespace []string) bool {
	name, err := validateNamespace(namespace)
	return err == nil && name == VectorIndexNamespace
}

func (h *S3TablesHandler) checkVectorNamespaceWrite(ctx context.Context, filerClient FilerClient, bucketName string, namespace []string) error {
	if h.trusted || !isVectorIndexNamespace(namespace) {
		return nil
	}
	var metadata tableBucketMetadata
	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(ctx, client, GetTableBucketPath(bucketName), ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, &metadata)
	})
	if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrAttributeNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if metadata.Format == FormatLance {
		return &S3TablesError{Type: ErrCodeAccessDenied, Message: vectorNamespaceReadOnly}
	}
	return nil
}

// fenceVectorNamespace returns a fence for an untrusted write naming the
// vector namespace of a LANCE bucket. The request still runs, so the
// operation's own checks answer everything short of a write: an ungranted
// caller gets its usual denial, and a CreateTable that finds the index
// already there (the vector engine's declare) succeeds.
func (h *S3TablesHandler) fenceVectorNamespace(w http.ResponseWriter, r *http.Request, filerClient FilerClient, operation string) *vectorNamespaceFence {
	if h.trusted || strings.HasPrefix(operation, "Get") || strings.HasPrefix(operation, "List") {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, MaxRequestBodySize+1))
	r.Body = io.NopCloser(bytes.NewReader(body))

	var req struct {
		TableBucketARN  string   `json:"tableBucketARN"`
		Namespace       []string `json:"namespace"`
		SourceNamespace []string `json:"sourceNamespace"`
		DestNamespace   []string `json:"destNamespace"`
		ResourceARN     string   `json:"resourceArn"`
	}
	// Like the handler's own decode, this keeps going past a mistyped field.
	_ = json.Unmarshal(body, &req)
	type target struct {
		bucket    string
		namespace []string
	}
	bucketName, _ := parseBucketNameFromARN(req.TableBucketARN)
	targets := []target{{bucketName, req.Namespace}, {bucketName, req.SourceNamespace}, {bucketName, req.DestNamespace}}
	if bucket, namespace, _, err := parseTableFromARN(req.ResourceARN); err == nil {
		targets = append(targets, target{bucket, []string{namespace}})
	}
	for _, t := range targets {
		if t.bucket != "" && h.checkVectorNamespaceWrite(r.Context(), filerClient, t.bucket, t.namespace) != nil {
			return &vectorNamespaceFence{ResponseWriter: w, h: h, filer: filerClient}
		}
	}
	return nil
}

// vectorNamespaceFence passes reads, refuses filer writes, and answers the
// request as read-only once a write was refused. Handlers write only after
// their permission check, so an ungranted caller never sees that answer.
type vectorNamespaceFence struct {
	http.ResponseWriter
	h        *S3TablesHandler
	filer    FilerClient
	tripped  atomic.Bool
	decided  bool
	refusing bool
}

func (f *vectorNamespaceFence) WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return f.filer.WithFilerClient(streamingMode, func(client filer_pb.SeaweedFilerClient) error {
		return fn(readOnlyFilerClient{SeaweedFilerClient: client, tripped: &f.tripped})
	})
}

func (f *vectorNamespaceFence) WriteHeader(status int) {
	if !f.settle() {
		f.ResponseWriter.WriteHeader(status)
	}
}

func (f *vectorNamespaceFence) Write(b []byte) (int, error) {
	if f.settle() {
		return len(b), nil
	}
	return f.ResponseWriter.Write(b)
}

// settle decides, at the first response write, whether the handler's answer
// is replaced by the read-only refusal.
func (f *vectorNamespaceFence) settle() bool {
	if !f.decided {
		f.decided = true
		f.refusing = f.tripped.Load()
		if f.refusing {
			f.h.writeError(f.ResponseWriter, http.StatusForbidden, ErrCodeAccessDenied, vectorNamespaceReadOnly)
		}
	}
	return f.refusing
}

type readOnlyFilerClient struct {
	filer_pb.SeaweedFilerClient
	tripped *atomic.Bool
}

func (c readOnlyFilerClient) refuse() error {
	c.tripped.Store(true)
	return errVectorNamespaceReadOnly
}

func (c readOnlyFilerClient) CreateEntry(context.Context, *filer_pb.CreateEntryRequest, ...grpc.CallOption) (*filer_pb.CreateEntryResponse, error) {
	return nil, c.refuse()
}

func (c readOnlyFilerClient) UpdateEntry(context.Context, *filer_pb.UpdateEntryRequest, ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	return nil, c.refuse()
}

func (c readOnlyFilerClient) AppendToEntry(context.Context, *filer_pb.AppendToEntryRequest, ...grpc.CallOption) (*filer_pb.AppendToEntryResponse, error) {
	return nil, c.refuse()
}

func (c readOnlyFilerClient) DeleteEntry(context.Context, *filer_pb.DeleteEntryRequest, ...grpc.CallOption) (*filer_pb.DeleteEntryResponse, error) {
	return nil, c.refuse()
}

func (c readOnlyFilerClient) AtomicRenameEntry(context.Context, *filer_pb.AtomicRenameEntryRequest, ...grpc.CallOption) (*filer_pb.AtomicRenameEntryResponse, error) {
	return nil, c.refuse()
}

func (c readOnlyFilerClient) StreamRenameEntry(context.Context, *filer_pb.StreamRenameEntryRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.StreamRenameEntryResponse], error) {
	return nil, c.refuse()
}

func (c readOnlyFilerClient) StreamMutateEntry(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[filer_pb.StreamMutateEntryRequest, filer_pb.StreamMutateEntryResponse], error) {
	return nil, c.refuse()
}

func (c readOnlyFilerClient) ObjectTransaction(context.Context, *filer_pb.ObjectTransactionRequest, ...grpc.CallOption) (*filer_pb.ObjectTransactionResponse, error) {
	return nil, c.refuse()
}

func (c readOnlyFilerClient) ObjectTransactionBatch(context.Context, *filer_pb.ObjectTransactionBatchRequest, ...grpc.CallOption) (*filer_pb.ObjectTransactionBatchResponse, error) {
	return nil, c.refuse()
}

// CheckVectorNamespaceWrite refuses an untrusted write into the vector
// namespace of a LANCE bucket, for callers that write the filer directly.
func (m *Manager) CheckVectorNamespaceWrite(ctx context.Context, filerClient FilerClient, bucketName string, namespace []string) error {
	return m.handler.checkVectorNamespaceWrite(ctx, filerClient, bucketName, namespace)
}
