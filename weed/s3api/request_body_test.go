package s3api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadRequestBodyLeavesOversizedBodyDrainable(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Body = io.NopCloser(strings.NewReader("abcdef"))

	body, err := readRequestBody(req, 3)
	remaining, readErr := io.ReadAll(req.Body)

	assert.ErrorIs(t, err, errRequestBodyTooLarge)
	assert.Nil(t, body)
	assert.NoError(t, readErr)
	assert.Equal(t, "ef", string(remaining))
}
