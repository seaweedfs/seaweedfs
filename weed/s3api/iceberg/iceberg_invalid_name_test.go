package iceberg

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

func TestNameValidationError(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		// Wrapped exactly as the s3tables manager surfaces it.
		{fmt.Errorf("all filers failed, last error: invalid namespace name: only 'a-z', '0-9', and '_' are allowed"), true},
		{fmt.Errorf("invalid table name: only 'a-z', '0-9', and '_' are allowed"), true},
		{fmt.Errorf("namespace name must start with a letter or digit"), true},
		{fmt.Errorf("namespace name cannot start with reserved prefix 'aws'"), true},
		{fmt.Errorf("table name must be between 1 and 255 characters"), true},
		{fmt.Errorf("namespace not found"), false},
		{fmt.Errorf("all filers failed, last error: rpc timeout"), false},
		// Unrelated faults that merely mention a name must stay 500.
		{fmt.Errorf("failed to resolve table name from index"), false},
		{fmt.Errorf("error fetching namespace name mapping"), false},
	}
	for _, c := range cases {
		if got := nameValidationError(c.err); got != c.want {
			t.Errorf("nameValidationError(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

func TestWriteManagerError(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		wantCode    int
		wantType    string
		wantMessage []string
	}{
		{name: "invalid name is a client error", err: fmt.Errorf("invalid namespace name: only 'a-z', '0-9', and '_' are allowed"), wantCode: http.StatusBadRequest, wantType: "BadRequestException"},
		{name: "rejected schema is a client error", err: fmt.Errorf("%w: for v2: variant is not supported until v3", iceberg.ErrInvalidSchema), wantCode: http.StatusBadRequest, wantType: "BadRequestException"},
		{name: "bad format version is a client error", err: fmt.Errorf("%w: 4", iceberg.ErrInvalidFormatVersion), wantCode: http.StatusBadRequest, wantType: "BadRequestException"},
		{
			name:     "missing table bucket is a client error",
			err:      fmt.Errorf("all filers failed, last error: %w", &s3tables.S3TablesError{Type: s3tables.ErrCodeNoSuchBucket, Message: "table bucket warehouse not found"}),
			wantCode: http.StatusNotFound,
			wantType: "NoSuchNamespaceException",
			// The guidance is the point of the mapping: the bucket the request
			// resolved to is one the client never named, so the response has to
			// say how to name a real one.
			wantMessage: []string{"table bucket warehouse not found", "warehouse=s3://<table-bucket>/", "/v1/<table-bucket>/"},
		},
		{name: "everything else is a server fault", err: fmt.Errorf("all filers failed, last error: connection refused"), wantCode: http.StatusInternalServerError, wantType: "InternalServerError"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			writeManagerError(rec, c.err)
			if rec.Code != c.wantCode {
				t.Fatalf("status = %d, want %d", rec.Code, c.wantCode)
			}
			var resp ErrorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if resp.Error.Type != c.wantType {
				t.Fatalf("error type = %q, want %q", resp.Error.Type, c.wantType)
			}
			if resp.Error.Code != c.wantCode {
				t.Fatalf("error code = %d, want %d", resp.Error.Code, c.wantCode)
			}
			for _, want := range c.wantMessage {
				if !strings.Contains(resp.Error.Message, want) {
					t.Errorf("message %q does not contain %q", resp.Error.Message, want)
				}
			}
		})
	}
}
