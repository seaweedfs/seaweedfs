package s3api

import (
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

// TestExtractObjectLockMetadataFromRequest tests the function that extracts
// object lock headers from PUT requests and stores them in Extended attributes.
// This test would have caught the bug where object lock headers were ignored.
func TestExtractObjectLockMetadataFromRequest(t *testing.T) {
	s3a := &S3ApiServer{}

	t.Run("Extract COMPLIANCE mode and retention date", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		retainUntilDate := time.Now().Add(24 * time.Hour)
		req.Header.Set(s3_constants.AmzObjectLockMode, "COMPLIANCE")
		req.Header.Set(s3_constants.AmzObjectLockRetainUntilDate, retainUntilDate.Format(time.RFC3339))

		entry := &filer_pb.Entry{
			Extended: make(map[string][]byte),
		}

		s3a.extractObjectLockMetadataFromRequest(req, entry)

		// Verify mode was stored
		assert.Contains(t, entry.Extended, s3_constants.ExtObjectLockModeKey)
		assert.Equal(t, "COMPLIANCE", string(entry.Extended[s3_constants.ExtObjectLockModeKey]))

		// Verify retention date was stored
		assert.Contains(t, entry.Extended, s3_constants.ExtRetentionUntilDateKey)
		storedTimestamp, err := strconv.ParseInt(string(entry.Extended[s3_constants.ExtRetentionUntilDateKey]), 10, 64)
		assert.NoError(t, err)
		storedTime := time.Unix(storedTimestamp, 0)
		assert.WithinDuration(t, retainUntilDate, storedTime, 1*time.Second)
	})

	t.Run("Extract GOVERNANCE mode and retention date", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		retainUntilDate := time.Now().Add(12 * time.Hour)
		req.Header.Set(s3_constants.AmzObjectLockMode, "GOVERNANCE")
		req.Header.Set(s3_constants.AmzObjectLockRetainUntilDate, retainUntilDate.Format(time.RFC3339))

		entry := &filer_pb.Entry{
			Extended: make(map[string][]byte),
		}

		s3a.extractObjectLockMetadataFromRequest(req, entry)

		assert.Equal(t, "GOVERNANCE", string(entry.Extended[s3_constants.ExtObjectLockModeKey]))
		assert.Contains(t, entry.Extended, s3_constants.ExtRetentionUntilDateKey)
	})

	t.Run("Extract legal hold ON", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		req.Header.Set(s3_constants.AmzObjectLockLegalHold, "ON")

		entry := &filer_pb.Entry{
			Extended: make(map[string][]byte),
		}

		s3a.extractObjectLockMetadataFromRequest(req, entry)

		assert.Contains(t, entry.Extended, s3_constants.ExtLegalHoldKey)
		assert.Equal(t, "ON", string(entry.Extended[s3_constants.ExtLegalHoldKey]))
	})

	t.Run("Extract legal hold OFF", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		req.Header.Set(s3_constants.AmzObjectLockLegalHold, "OFF")

		entry := &filer_pb.Entry{
			Extended: make(map[string][]byte),
		}

		s3a.extractObjectLockMetadataFromRequest(req, entry)

		assert.Contains(t, entry.Extended, s3_constants.ExtLegalHoldKey)
		assert.Equal(t, "OFF", string(entry.Extended[s3_constants.ExtLegalHoldKey]))
	})

	t.Run("Handle all object lock headers together", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		retainUntilDate := time.Now().Add(24 * time.Hour)
		req.Header.Set(s3_constants.AmzObjectLockMode, "COMPLIANCE")
		req.Header.Set(s3_constants.AmzObjectLockRetainUntilDate, retainUntilDate.Format(time.RFC3339))
		req.Header.Set(s3_constants.AmzObjectLockLegalHold, "ON")

		entry := &filer_pb.Entry{
			Extended: make(map[string][]byte),
		}

		s3a.extractObjectLockMetadataFromRequest(req, entry)

		// All metadata should be stored
		assert.Equal(t, "COMPLIANCE", string(entry.Extended[s3_constants.ExtObjectLockModeKey]))
		assert.Contains(t, entry.Extended, s3_constants.ExtRetentionUntilDateKey)
		assert.Equal(t, "ON", string(entry.Extended[s3_constants.ExtLegalHoldKey]))
	})

	t.Run("Handle no object lock headers", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		// No object lock headers set

		entry := &filer_pb.Entry{
			Extended: make(map[string][]byte),
		}

		s3a.extractObjectLockMetadataFromRequest(req, entry)

		// No object lock metadata should be stored
		assert.NotContains(t, entry.Extended, s3_constants.ExtObjectLockModeKey)
		assert.NotContains(t, entry.Extended, s3_constants.ExtRetentionUntilDateKey)
		assert.NotContains(t, entry.Extended, s3_constants.ExtLegalHoldKey)
	})

	t.Run("Handle invalid retention date gracefully", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		req.Header.Set(s3_constants.AmzObjectLockMode, "GOVERNANCE")
		req.Header.Set(s3_constants.AmzObjectLockRetainUntilDate, "invalid-date")

		entry := &filer_pb.Entry{
			Extended: make(map[string][]byte),
		}

		s3a.extractObjectLockMetadataFromRequest(req, entry)

		// Mode should be stored but not invalid date
		assert.Equal(t, "GOVERNANCE", string(entry.Extended[s3_constants.ExtObjectLockModeKey]))
		assert.NotContains(t, entry.Extended, s3_constants.ExtRetentionUntilDateKey)
	})
}

// TestAddObjectLockHeadersToResponse tests the function that adds object lock
// metadata from Extended attributes to HTTP response headers.
// This test would have caught the bug where HEAD responses didn't include object lock metadata.
func TestAddObjectLockHeadersToResponse(t *testing.T) {
	s3a := &S3ApiServer{}

	t.Run("Add COMPLIANCE mode and retention date to response", func(t *testing.T) {
		w := httptest.NewRecorder()
		retainUntilTime := time.Now().Add(24 * time.Hour)

		entry := &filer_pb.Entry{
			Extended: map[string][]byte{
				s3_constants.ExtObjectLockModeKey:     []byte("COMPLIANCE"),
				s3_constants.ExtRetentionUntilDateKey: []byte(strconv.FormatInt(retainUntilTime.Unix(), 10)),
			},
		}

		s3a.addObjectLockHeadersToResponse(w, entry)

		// Verify headers were set
		assert.Equal(t, "COMPLIANCE", w.Header().Get(s3_constants.AmzObjectLockMode))
		assert.NotEmpty(t, w.Header().Get(s3_constants.AmzObjectLockRetainUntilDate))

		// Verify the date format is correct
		returnedDate := w.Header().Get(s3_constants.AmzObjectLockRetainUntilDate)
		parsedTime, err := time.Parse(time.RFC3339, returnedDate)
		assert.NoError(t, err)
		assert.WithinDuration(t, retainUntilTime, parsedTime, 1*time.Second)
	})

	t.Run("Add GOVERNANCE mode to response", func(t *testing.T) {
		w := httptest.NewRecorder()
		entry := &filer_pb.Entry{
			Extended: map[string][]byte{
				s3_constants.ExtObjectLockModeKey: []byte("GOVERNANCE"),
			},
		}

		s3a.addObjectLockHeadersToResponse(w, entry)

		assert.Equal(t, "GOVERNANCE", w.Header().Get(s3_constants.AmzObjectLockMode))
	})

	t.Run("Add legal hold ON to response", func(t *testing.T) {
		w := httptest.NewRecorder()
		entry := &filer_pb.Entry{
			Extended: map[string][]byte{
				s3_constants.ExtLegalHoldKey: []byte("ON"),
			},
		}

		s3a.addObjectLockHeadersToResponse(w, entry)

		assert.Equal(t, "ON", w.Header().Get(s3_constants.AmzObjectLockLegalHold))
	})

	t.Run("Add legal hold OFF to response", func(t *testing.T) {
		w := httptest.NewRecorder()
		entry := &filer_pb.Entry{
			Extended: map[string][]byte{
				s3_constants.ExtLegalHoldKey: []byte("OFF"),
			},
		}

		s3a.addObjectLockHeadersToResponse(w, entry)

		assert.Equal(t, "OFF", w.Header().Get(s3_constants.AmzObjectLockLegalHold))
	})

	t.Run("Add all object lock headers to response", func(t *testing.T) {
		w := httptest.NewRecorder()
		retainUntilTime := time.Now().Add(12 * time.Hour)

		entry := &filer_pb.Entry{
			Extended: map[string][]byte{
				s3_constants.ExtObjectLockModeKey:     []byte("GOVERNANCE"),
				s3_constants.ExtRetentionUntilDateKey: []byte(strconv.FormatInt(retainUntilTime.Unix(), 10)),
				s3_constants.ExtLegalHoldKey:          []byte("ON"),
			},
		}

		s3a.addObjectLockHeadersToResponse(w, entry)

		// All headers should be set
		assert.Equal(t, "GOVERNANCE", w.Header().Get(s3_constants.AmzObjectLockMode))
		assert.NotEmpty(t, w.Header().Get(s3_constants.AmzObjectLockRetainUntilDate))
		assert.Equal(t, "ON", w.Header().Get(s3_constants.AmzObjectLockLegalHold))
	})

	t.Run("Handle entry with no object lock metadata", func(t *testing.T) {
		w := httptest.NewRecorder()
		entry := &filer_pb.Entry{
			Extended: map[string][]byte{
				"other-metadata": []byte("some-value"),
			},
		}

		s3a.addObjectLockHeadersToResponse(w, entry)

		// No object lock headers should be set
		assert.Empty(t, w.Header().Get(s3_constants.AmzObjectLockMode))
		assert.Empty(t, w.Header().Get(s3_constants.AmzObjectLockRetainUntilDate))
		assert.Empty(t, w.Header().Get(s3_constants.AmzObjectLockLegalHold))
	})

	t.Run("Handle nil entry gracefully", func(t *testing.T) {
		w := httptest.NewRecorder()

		// Should not panic
		s3a.addObjectLockHeadersToResponse(w, nil)

		// No headers should be set
		assert.Empty(t, w.Header().Get(s3_constants.AmzObjectLockMode))
		assert.Empty(t, w.Header().Get(s3_constants.AmzObjectLockRetainUntilDate))
		assert.Empty(t, w.Header().Get(s3_constants.AmzObjectLockLegalHold))
	})

	t.Run("Handle entry with nil Extended map gracefully", func(t *testing.T) {
		w := httptest.NewRecorder()
		entry := &filer_pb.Entry{
			Extended: nil,
		}

		// Should not panic
		s3a.addObjectLockHeadersToResponse(w, entry)

		// No headers should be set
		assert.Empty(t, w.Header().Get(s3_constants.AmzObjectLockMode))
		assert.Empty(t, w.Header().Get(s3_constants.AmzObjectLockRetainUntilDate))
		assert.Empty(t, w.Header().Get(s3_constants.AmzObjectLockLegalHold))
	})

	t.Run("Handle invalid retention timestamp gracefully", func(t *testing.T) {
		w := httptest.NewRecorder()
		entry := &filer_pb.Entry{
			Extended: map[string][]byte{
				s3_constants.ExtObjectLockModeKey:     []byte("COMPLIANCE"),
				s3_constants.ExtRetentionUntilDateKey: []byte("invalid-timestamp"),
			},
		}

		s3a.addObjectLockHeadersToResponse(w, entry)

		// Mode should be set but not retention date due to invalid timestamp
		assert.Equal(t, "COMPLIANCE", w.Header().Get(s3_constants.AmzObjectLockMode))
		assert.Empty(t, w.Header().Get(s3_constants.AmzObjectLockRetainUntilDate))
	})
}

// TestObjectLockHeaderRoundTrip tests the complete round trip:
// extract from request → store in Extended attributes → add to response
func TestObjectLockHeaderRoundTrip(t *testing.T) {
	s3a := &S3ApiServer{}

	t.Run("Complete round trip for COMPLIANCE mode", func(t *testing.T) {
		// 1. Create request with object lock headers
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		retainUntilDate := time.Now().Add(24 * time.Hour)
		req.Header.Set(s3_constants.AmzObjectLockMode, "COMPLIANCE")
		req.Header.Set(s3_constants.AmzObjectLockRetainUntilDate, retainUntilDate.Format(time.RFC3339))
		req.Header.Set(s3_constants.AmzObjectLockLegalHold, "ON")

		// 2. Extract and store in Extended attributes
		entry := &filer_pb.Entry{
			Extended: make(map[string][]byte),
		}
		s3a.extractObjectLockMetadataFromRequest(req, entry)

		// 3. Add to response headers
		w := httptest.NewRecorder()
		s3a.addObjectLockHeadersToResponse(w, entry)

		// 4. Verify round trip preserved all data
		assert.Equal(t, "COMPLIANCE", w.Header().Get(s3_constants.AmzObjectLockMode))
		assert.Equal(t, "ON", w.Header().Get(s3_constants.AmzObjectLockLegalHold))

		returnedDate := w.Header().Get(s3_constants.AmzObjectLockRetainUntilDate)
		parsedTime, err := time.Parse(time.RFC3339, returnedDate)
		assert.NoError(t, err)
		assert.WithinDuration(t, retainUntilDate, parsedTime, 1*time.Second)
	})

	t.Run("Complete round trip for GOVERNANCE mode", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		retainUntilDate := time.Now().Add(12 * time.Hour)
		req.Header.Set(s3_constants.AmzObjectLockMode, "GOVERNANCE")
		req.Header.Set(s3_constants.AmzObjectLockRetainUntilDate, retainUntilDate.Format(time.RFC3339))

		entry := &filer_pb.Entry{Extended: make(map[string][]byte)}
		s3a.extractObjectLockMetadataFromRequest(req, entry)

		w := httptest.NewRecorder()
		s3a.addObjectLockHeadersToResponse(w, entry)

		assert.Equal(t, "GOVERNANCE", w.Header().Get(s3_constants.AmzObjectLockMode))
		assert.NotEmpty(t, w.Header().Get(s3_constants.AmzObjectLockRetainUntilDate))
	})
}
