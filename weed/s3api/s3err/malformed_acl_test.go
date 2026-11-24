package s3err

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMalformedACLErrorMapping: ErrMalformedACL is S3's MalformedACLError, a
// 400 client error.
func TestMalformedACLErrorMapping(t *testing.T) {
	apiError := GetAPIError(ErrMalformedACL)
	assert.Equal(t, "MalformedACLError", apiError.Code)
	assert.Equal(t, http.StatusBadRequest, apiError.HTTPStatusCode)
	assert.NotEmpty(t, apiError.Description)
}

// TestMalformedACLErrorResponse: the error document a client receives carries
// the MalformedACLError code and the 400 status.
func TestMalformedACLErrorResponse(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "/bucket/object?acl", nil)
	rr := httptest.NewRecorder()
	WriteErrorResponse(rr, req, ErrMalformedACL)
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "<Code>MalformedACLError</Code>")
}

// TestMalformedACLIsAppended: the new code is added after every existing one,
// so the numeric value of each existing ErrorCode stays the same.
func TestMalformedACLIsAppended(t *testing.T) {
	assert.Equal(t, ErrNoSuchPublicAccessBlockConfiguration+1, ErrMalformedACL)
}
