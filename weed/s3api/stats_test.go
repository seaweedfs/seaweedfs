package s3api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTrackAuditFallbackForDirectWriteHeader covers the regression behind
// issue #9463: handlers that bypass writeSuccessResponse helpers and call
// w.WriteHeader directly (GetObjectHandler stream path, HeadObjectHandler,
// GetBucketEncryption, GetObjectLockConfiguration, etc.) used to drop their
// audit entry. track() must mark the request as audited via a fallback
// PostLog after the handler returns.
func TestTrackAuditFallbackForDirectWriteHeader(t *testing.T) {
	var captured *http.Request
	handler := func(w http.ResponseWriter, r *http.Request) {
		captured = r
		w.WriteHeader(http.StatusOK)
	}
	wrapped := track(handler, "GET")
	wrapped(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/bucket/object", nil))

	if assert.NotNil(t, captured, "handler must have been invoked") {
		assert.True(t, s3err.AuditAlreadyLogged(captured),
			"track must emit fallback audit when handler writes response directly")
	}
}

// TestTrackAuditSkipsFallbackWhenHandlerEmits guards against double logging:
// handlers that already call PostLog (e.g. via writeSuccessResponseXML or
// WriteErrorResponse) flip the audit flag, and track must observe that and
// not re-emit.
func TestTrackAuditSkipsFallbackWhenHandlerEmits(t *testing.T) {
	var captured *http.Request
	handler := func(w http.ResponseWriter, r *http.Request) {
		captured = r
		s3err.PostLog(r, http.StatusOK, s3err.ErrNone)
		w.WriteHeader(http.StatusOK)
	}
	wrapped := track(handler, "PUT")
	wrapped(httptest.NewRecorder(), httptest.NewRequest(http.MethodPut, "/bucket/object", nil))

	if assert.NotNil(t, captured) {
		assert.True(t, s3err.AuditAlreadyLogged(captured),
			"flag must be set after handler PostLog")
	}
}

type slowReader struct {
	io.Reader
	delay time.Duration
}

func (s slowReader) Read(b []byte) (int, error) {
	time.Sleep(s.delay)
	return s.Reader.Read(b)
}

func putStageSamples(t *testing.T, operation, stage string) (uint64, float64) {
	t.Helper()
	var m dto.Metric
	require.NoError(t, stats_collect.S3PutStageHistogram.WithLabelValues(operation, stage).(prometheus.Histogram).Write(&m))
	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}

func stagedUploadOption(assignDelay time.Duration, uploadErr error) *operation.ChunkedUploadOption {
	return &operation.ChunkedUploadOption{
		ChunkSize:      4,
		SmallFileLimit: 0,
		AssignFunc: func(ctx context.Context, count int, expectedDataSize uint64) (*operation.VolumeAssignRequest, *operation.AssignResult, error) {
			time.Sleep(assignDelay)
			return nil, &operation.AssignResult{Fid: "3,01637037d6", Url: "127.0.0.1:1", Count: 1}, nil
		},
		UploadFunc: func(ctx context.Context, data []byte, option *operation.UploadOption) (*operation.UploadResult, error) {
			if uploadErr != nil {
				return nil, uploadErr
			}
			return &operation.UploadResult{Size: uint32(len(data))}, nil
		},
	}
}

func TestPutStagesRecordEachStage(t *testing.T) {
	r := withMultipartPart(httptest.NewRequest(http.MethodPut, "/bucket/object?partNumber=1&uploadId=u", nil), uploadFilerClient{})
	before := map[string]uint64{}
	sums := map[string]float64{}
	for _, stage := range []string{"body_read", "assign", "chunk_upload", "metadata"} {
		before[stage], sums[stage] = putStageSamples(t, "UploadPart", stage)
	}

	stages := newPutStages(r)
	result, err := stages.upload(context.Background(), slowReader{Reader: bytes.NewReader([]byte("12345678")), delay: 10 * time.Millisecond}, stagedUploadOption(20*time.Millisecond, nil))
	require.NoError(t, err)
	require.Len(t, result.FileChunks, 2)
	time.Sleep(5 * time.Millisecond)
	stages.record()

	for stage, atLeast := range map[string]time.Duration{"body_read": 20 * time.Millisecond, "assign": 40 * time.Millisecond, "chunk_upload": 0, "metadata": 5 * time.Millisecond} {
		count, sum := putStageSamples(t, "UploadPart", stage)
		assert.Equal(t, before[stage]+1, count, stage)
		assert.GreaterOrEqual(t, sum-sums[stage], atLeast.Seconds(), stage)
	}
}

func TestPutStagesSkipMetadataAfterFailedUpload(t *testing.T) {
	r := httptest.NewRequest(http.MethodPut, "/bucket/object", nil)
	bodyBefore, _ := putStageSamples(t, "PutObject", "body_read")
	metadataBefore, _ := putStageSamples(t, "PutObject", "metadata")

	stages := newPutStages(r)
	_, err := stages.upload(context.Background(), bytes.NewReader([]byte("1234")), stagedUploadOption(0, errors.New("volume down")))
	require.Error(t, err)
	stages.record()

	bodyAfter, _ := putStageSamples(t, "PutObject", "body_read")
	metadataAfter, _ := putStageSamples(t, "PutObject", "metadata")
	assert.Equal(t, bodyBefore+1, bodyAfter)
	assert.Equal(t, metadataBefore, metadataAfter)
}

func TestPutStagesRecordNothingBeforeUpload(t *testing.T) {
	before, _ := putStageSamples(t, "PutObject", "body_read")
	newPutStages(httptest.NewRequest(http.MethodPut, "/bucket/object", nil)).record()
	after, _ := putStageSamples(t, "PutObject", "body_read")
	assert.Equal(t, before, after)
}
