package iceberg

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
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
		name     string
		err      error
		wantCode int
		wantType string
	}{
		{"invalid name is a client error", fmt.Errorf("invalid namespace name: only 'a-z', '0-9', and '_' are allowed"), http.StatusBadRequest, "BadRequestException"},
		{"everything else is a server fault", fmt.Errorf("all filers failed, last error: connection refused"), http.StatusInternalServerError, "InternalServerError"},
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
		})
	}
}
