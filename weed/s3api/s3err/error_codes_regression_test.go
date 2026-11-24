package s3err

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestExistingACLErrorMappingsUnchanged pins the codes the ACL paths already
// return, and the last code before MalformedACLError was appended, so adding
// the new code cannot change what clients receive for them. It uses only codes
// that exist on master and passes there too.
func TestExistingACLErrorMappingsUnchanged(t *testing.T) {
	cases := []struct {
		code   ErrorCode
		s3Code string
		status int
	}{
		{ErrInvalidRequest, "InvalidRequest", http.StatusBadRequest},
		{ErrMalformedXML, "MalformedXML", http.StatusBadRequest},
		{ErrAccessDenied, "AccessDenied", http.StatusForbidden},
		{ErrNotImplemented, "NotImplemented", http.StatusNotImplemented},
		{ErrAccessControlListNotSupported, "AccessControlListNotSupported", http.StatusBadRequest},
		{ErrNoSuchPublicAccessBlockConfiguration, "NoSuchPublicAccessBlockConfiguration", http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.s3Code, func(t *testing.T) {
			apiError := GetAPIError(tc.code)
			assert.Equal(t, tc.s3Code, apiError.Code)
			assert.Equal(t, tc.status, apiError.HTTPStatusCode)
		})
	}
}
