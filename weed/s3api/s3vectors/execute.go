package s3vectors

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// Error is an S3 Vectors error response, as Execute returns it.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Execute runs one operation as identity for trusted local tooling such as
// the admin console, like s3tables.Manager.Execute: nothing is signed or
// authenticated, and identity's grants decide what the operation may do.
// Operations that reach the vector engine need the server's VectorServiceClient.
func (s *Server) Execute(ctx context.Context, operation string, req, resp interface{}, identity string) error {
	handler, ok := handlers[operation]
	if !ok {
		return &Error{Code: "ValidationException", Message: "unknown operation " + operation}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, "/"+operation, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if identity != "" {
		r = r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), identity))
	}
	w := httptest.NewRecorder()
	handler(s, w, r)
	if w.Code >= http.StatusBadRequest {
		e := &Error{}
		if json.Unmarshal(w.Body.Bytes(), e) != nil || e.Code == "" {
			e = &Error{Code: http.StatusText(w.Code), Message: w.Body.String()}
		}
		return e
	}
	if resp == nil || w.Body.Len() == 0 {
		return nil
	}
	return json.Unmarshal(w.Body.Bytes(), resp)
}
