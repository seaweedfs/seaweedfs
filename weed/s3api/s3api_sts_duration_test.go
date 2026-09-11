package s3api

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDurationSecondsRequest(t *testing.T, seconds string) *http.Request {
	t.Helper()
	form := url.Values{}
	if seconds != "" {
		form.Set("DurationSeconds", seconds)
	}
	req, err := http.NewRequest("POST", "/", strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	require.NoError(t, req.ParseForm())
	return req
}

func newSTSHandlersWithMaxSession(t *testing.T, maxSession time.Duration) *STSHandlers {
	t.Helper()
	s := sts.NewSTSService()
	require.NoError(t, s.Initialize(&sts.STSConfig{
		Issuer:           "test-issuer",
		SigningKey:       []byte("test-signing-key-at-least-32-bytes-long-for-security"),
		TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
		MaxSessionLength: sts.FlexibleDuration{Duration: maxSession},
	}))
	return NewSTSHandlers(s, nil)
}

func TestParseDurationSeconds_RespectsConfiguredMaxSessionLength(t *testing.T) {
	h := newSTSHandlersWithMaxSession(t, 168*time.Hour)

	ds, errCode, err := h.parseDurationSeconds(newDurationSecondsRequest(t, "604800"))
	require.NoError(t, err)
	assert.Equal(t, STSErrorCode(""), errCode)
	if assert.NotNil(t, ds) {
		assert.Equal(t, int64(604800), *ds)
	}
}

func TestParseDurationSeconds_RejectsAboveConfiguredMaxSessionLength(t *testing.T) {
	h := newSTSHandlersWithMaxSession(t, 24*time.Hour)

	ds, errCode, err := h.parseDurationSeconds(newDurationSecondsRequest(t, "90000"))
	assert.Error(t, err)
	assert.Equal(t, STSErrInvalidParameterValue, errCode)
	assert.Nil(t, ds)
}

func TestParseDurationSeconds_FallsBackToDefaultWhenUnset(t *testing.T) {
	h := &STSHandlers{stsService: nil}

	ds, errCode, err := h.parseDurationSeconds(newDurationSecondsRequest(t, "43200"))
	require.NoError(t, err)
	assert.Equal(t, STSErrorCode(""), errCode)
	if assert.NotNil(t, ds) {
		assert.Equal(t, int64(43200), *ds)
	}

	_, errCode, err = h.parseDurationSeconds(newDurationSecondsRequest(t, "43201"))
	assert.Error(t, err)
	assert.Equal(t, STSErrInvalidParameterValue, errCode)
}

func TestParseDurationSeconds_EnforcesMinimum(t *testing.T) {
	h := newSTSHandlersWithMaxSession(t, 168*time.Hour)

	_, errCode, err := h.parseDurationSeconds(newDurationSecondsRequest(t, "899"))
	assert.Error(t, err)
	assert.Equal(t, STSErrInvalidParameterValue, errCode)
}

func TestParseDurationSeconds_EmptyReturnsNil(t *testing.T) {
	h := newSTSHandlersWithMaxSession(t, 168*time.Hour)

	ds, errCode, err := h.parseDurationSeconds(newDurationSecondsRequest(t, ""))
	require.NoError(t, err)
	assert.Equal(t, STSErrorCode(""), errCode)
	assert.Nil(t, ds)
}

func TestParseDurationSeconds_SubMinimumMaxSessionLengthKeepsCapping(t *testing.T) {
	h := newSTSHandlersWithMaxSession(t, 5*time.Minute)

	ds, errCode, err := h.parseDurationSeconds(newDurationSecondsRequest(t, "900"))
	require.NoError(t, err)
	assert.Equal(t, STSErrorCode(""), errCode)
	if assert.NotNil(t, ds) {
		assert.Equal(t, int64(900), *ds)
	}

	_, errCode, err = h.parseDurationSeconds(newDurationSecondsRequest(t, "43201"))
	assert.Error(t, err)
	assert.Equal(t, STSErrInvalidParameterValue, errCode)
}
