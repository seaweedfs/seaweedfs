package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

func newValidatePolicyRequest(t *testing.T, document map[string]interface{}) *http.Request {
	t.Helper()
	body, err := json.Marshal(map[string]interface{}{"document": document})
	if err != nil {
		t.Fatalf("failed to marshal request body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/object-store/policies/validate", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func TestValidatePolicy(t *testing.T) {
	handlers := &PolicyHandlers{adminServer: &dash.AdminServer{}}

	tests := []struct {
		name       string
		document   map[string]interface{}
		wantStatus int
	}{
		{
			name: "valid document with Resource",
			document: map[string]interface{}{
				"Version": "2012-10-17",
				"Statement": []map[string]interface{}{
					{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::my-bucket/*"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "valid document with NotResource only",
			document: map[string]interface{}{
				"Version": "2012-10-17",
				"Statement": []map[string]interface{}{
					{"Effect": "Allow", "Action": "s3:GetObject", "NotResource": "arn:aws:s3:::secret-bucket/*"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing version",
			document: map[string]interface{}{
				"Statement": []map[string]interface{}{
					{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::my-bucket/*"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "no statements",
			document: map[string]interface{}{
				"Version":   "2012-10-17",
				"Statement": []map[string]interface{}{},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid effect",
			document: map[string]interface{}{
				"Version": "2012-10-17",
				"Statement": []map[string]interface{}{
					{"Effect": "Maybe", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::my-bucket/*"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing action",
			document: map[string]interface{}{
				"Version": "2012-10-17",
				"Statement": []map[string]interface{}{
					{"Effect": "Allow", "Resource": "arn:aws:s3:::my-bucket/*"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "neither Resource nor NotResource",
			document: map[string]interface{}{
				"Version": "2012-10-17",
				"Statement": []map[string]interface{}{
					{"Effect": "Allow", "Action": "s3:GetObject"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := newValidatePolicyRequest(t, tt.document)
			w := httptest.NewRecorder()

			handlers.ValidatePolicy(w, req)

			if w.Code != tt.wantStatus {
				t.Fatalf("expected status %d, got %d (body: %s)", tt.wantStatus, w.Code, w.Body.String())
			}
		})
	}
}

// Sanity check that policy_engine.PolicyStatement (the type ValidatePolicy
// actually decodes into) round-trips NotResource-only statements the way the
// test above assumes.
func TestValidatePolicy_NotResourceOnlyDecodes(t *testing.T) {
	raw := []byte(`{"Effect":"Allow","Action":"s3:GetObject","NotResource":"arn:aws:s3:::secret/*"}`)
	var stmt policy_engine.PolicyStatement
	if err := json.Unmarshal(raw, &stmt); err != nil {
		t.Fatalf("failed to unmarshal statement: %v", err)
	}
	if len(stmt.Resource.Strings()) != 0 {
		t.Fatalf("expected no Resource, got %v", stmt.Resource.Strings())
	}
	if len(stmt.NotResource.Strings()) != 1 {
		t.Fatalf("expected exactly one NotResource entry, got %v", stmt.NotResource.Strings())
	}
}
