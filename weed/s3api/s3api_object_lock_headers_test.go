package s3api

import (
	"fmt"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"errors"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
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

		err := s3a.extractObjectLockMetadataFromRequest(req, entry)
		assert.NoError(t, err)

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

		err := s3a.extractObjectLockMetadataFromRequest(req, entry)
		assert.NoError(t, err)

		assert.Equal(t, "GOVERNANCE", string(entry.Extended[s3_constants.ExtObjectLockModeKey]))
		assert.Contains(t, entry.Extended, s3_constants.ExtRetentionUntilDateKey)
	})

	t.Run("Extract legal hold ON", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		req.Header.Set(s3_constants.AmzObjectLockLegalHold, "ON")

		entry := &filer_pb.Entry{
			Extended: make(map[string][]byte),
		}

		err := s3a.extractObjectLockMetadataFromRequest(req, entry)
		assert.NoError(t, err)

		assert.Contains(t, entry.Extended, s3_constants.ExtLegalHoldKey)
		assert.Equal(t, "ON", string(entry.Extended[s3_constants.ExtLegalHoldKey]))
	})

	t.Run("Extract legal hold OFF", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		req.Header.Set(s3_constants.AmzObjectLockLegalHold, "OFF")

		entry := &filer_pb.Entry{
			Extended: make(map[string][]byte),
		}

		err := s3a.extractObjectLockMetadataFromRequest(req, entry)
		assert.NoError(t, err)

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

		err := s3a.extractObjectLockMetadataFromRequest(req, entry)
		assert.NoError(t, err)

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

		err := s3a.extractObjectLockMetadataFromRequest(req, entry)
		assert.NoError(t, err)

		// No object lock metadata should be stored
		assert.NotContains(t, entry.Extended, s3_constants.ExtObjectLockModeKey)
		assert.NotContains(t, entry.Extended, s3_constants.ExtRetentionUntilDateKey)
		assert.NotContains(t, entry.Extended, s3_constants.ExtLegalHoldKey)
	})

	t.Run("Handle invalid retention date - should return error", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		req.Header.Set(s3_constants.AmzObjectLockMode, "GOVERNANCE")
		req.Header.Set(s3_constants.AmzObjectLockRetainUntilDate, "invalid-date")

		entry := &filer_pb.Entry{
			Extended: make(map[string][]byte),
		}

		err := s3a.extractObjectLockMetadataFromRequest(req, entry)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidRetentionDateFormat))

		// Mode should be stored but not invalid date
		assert.Equal(t, "GOVERNANCE", string(entry.Extended[s3_constants.ExtObjectLockModeKey]))
		assert.NotContains(t, entry.Extended, s3_constants.ExtRetentionUntilDateKey)
	})

	t.Run("Handle invalid legal hold value - should return error", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		req.Header.Set(s3_constants.AmzObjectLockLegalHold, "INVALID")

		entry := &filer_pb.Entry{
			Extended: make(map[string][]byte),
		}

		err := s3a.extractObjectLockMetadataFromRequest(req, entry)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidLegalHoldStatus))

		// No legal hold metadata should be stored due to error
		assert.NotContains(t, entry.Extended, s3_constants.ExtLegalHoldKey)
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

		// No object lock headers should be set for entries without object lock metadata
		assert.Empty(t, w.Header().Get(s3_constants.AmzObjectLockMode))
		assert.Empty(t, w.Header().Get(s3_constants.AmzObjectLockRetainUntilDate))
		assert.Empty(t, w.Header().Get(s3_constants.AmzObjectLockLegalHold))
	})

	t.Run("Handle entry with object lock mode but no legal hold - should default to OFF", func(t *testing.T) {
		w := httptest.NewRecorder()
		entry := &filer_pb.Entry{
			Extended: map[string][]byte{
				s3_constants.ExtObjectLockModeKey: []byte("GOVERNANCE"),
			},
		}

		s3a.addObjectLockHeadersToResponse(w, entry)

		// Should set mode and default legal hold to OFF
		assert.Equal(t, "GOVERNANCE", w.Header().Get(s3_constants.AmzObjectLockMode))
		assert.Empty(t, w.Header().Get(s3_constants.AmzObjectLockRetainUntilDate))
		assert.Equal(t, "OFF", w.Header().Get(s3_constants.AmzObjectLockLegalHold))
	})

	t.Run("Handle entry with retention date but no legal hold - should default to OFF", func(t *testing.T) {
		w := httptest.NewRecorder()
		retainUntilTime := time.Now().Add(24 * time.Hour)
		entry := &filer_pb.Entry{
			Extended: map[string][]byte{
				s3_constants.ExtRetentionUntilDateKey: []byte(strconv.FormatInt(retainUntilTime.Unix(), 10)),
			},
		}

		s3a.addObjectLockHeadersToResponse(w, entry)

		// Should set retention date and default legal hold to OFF
		assert.Empty(t, w.Header().Get(s3_constants.AmzObjectLockMode))
		assert.NotEmpty(t, w.Header().Get(s3_constants.AmzObjectLockRetainUntilDate))
		assert.Equal(t, "OFF", w.Header().Get(s3_constants.AmzObjectLockLegalHold))
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
		err := s3a.extractObjectLockMetadataFromRequest(req, entry)
		assert.NoError(t, err)

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
		err := s3a.extractObjectLockMetadataFromRequest(req, entry)
		assert.NoError(t, err)

		w := httptest.NewRecorder()
		s3a.addObjectLockHeadersToResponse(w, entry)

		assert.Equal(t, "GOVERNANCE", w.Header().Get(s3_constants.AmzObjectLockMode))
		assert.NotEmpty(t, w.Header().Get(s3_constants.AmzObjectLockRetainUntilDate))
	})
}

// TestValidateObjectLockHeaders tests the validateObjectLockHeaders function
// to ensure proper validation of object lock headers in PUT requests
func TestValidateObjectLockHeaders(t *testing.T) {
	s3a := &S3ApiServer{}

	t.Run("Valid COMPLIANCE mode with retention date on versioned bucket", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		retainUntilDate := time.Now().Add(24 * time.Hour)
		req.Header.Set(s3_constants.AmzObjectLockMode, "COMPLIANCE")
		req.Header.Set(s3_constants.AmzObjectLockRetainUntilDate, retainUntilDate.Format(time.RFC3339))

		err := s3a.validateObjectLockHeaders(req, true) // versioned bucket
		assert.NoError(t, err)
	})

	t.Run("Valid GOVERNANCE mode with retention date on versioned bucket", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		retainUntilDate := time.Now().Add(12 * time.Hour)
		req.Header.Set(s3_constants.AmzObjectLockMode, "GOVERNANCE")
		req.Header.Set(s3_constants.AmzObjectLockRetainUntilDate, retainUntilDate.Format(time.RFC3339))

		err := s3a.validateObjectLockHeaders(req, true) // versioned bucket
		assert.NoError(t, err)
	})

	t.Run("Valid legal hold ON on versioned bucket", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		req.Header.Set(s3_constants.AmzObjectLockLegalHold, "ON")

		err := s3a.validateObjectLockHeaders(req, true) // versioned bucket
		assert.NoError(t, err)
	})

	t.Run("Valid legal hold OFF on versioned bucket", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		req.Header.Set(s3_constants.AmzObjectLockLegalHold, "OFF")

		err := s3a.validateObjectLockHeaders(req, true) // versioned bucket
		assert.NoError(t, err)
	})

	t.Run("Invalid object lock mode", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		req.Header.Set(s3_constants.AmzObjectLockMode, "INVALID_MODE")
		retainUntilDate := time.Now().Add(24 * time.Hour)
		req.Header.Set(s3_constants.AmzObjectLockRetainUntilDate, retainUntilDate.Format(time.RFC3339))

		err := s3a.validateObjectLockHeaders(req, true) // versioned bucket
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidObjectLockMode))
	})

	t.Run("Invalid legal hold status", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		req.Header.Set(s3_constants.AmzObjectLockLegalHold, "INVALID_STATUS")

		err := s3a.validateObjectLockHeaders(req, true) // versioned bucket
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidLegalHoldStatus))
	})

	t.Run("Object lock headers on non-versioned bucket", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		req.Header.Set(s3_constants.AmzObjectLockMode, "COMPLIANCE")
		retainUntilDate := time.Now().Add(24 * time.Hour)
		req.Header.Set(s3_constants.AmzObjectLockRetainUntilDate, retainUntilDate.Format(time.RFC3339))

		err := s3a.validateObjectLockHeaders(req, false) // non-versioned bucket
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrObjectLockVersioningRequired))
	})

	t.Run("Invalid retention date format", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		req.Header.Set(s3_constants.AmzObjectLockMode, "COMPLIANCE")
		req.Header.Set(s3_constants.AmzObjectLockRetainUntilDate, "invalid-date-format")

		err := s3a.validateObjectLockHeaders(req, true) // versioned bucket
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidRetentionDateFormat))
	})

	t.Run("Retention date in the past", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		req.Header.Set(s3_constants.AmzObjectLockMode, "COMPLIANCE")
		pastDate := time.Now().Add(-24 * time.Hour)
		req.Header.Set(s3_constants.AmzObjectLockRetainUntilDate, pastDate.Format(time.RFC3339))

		err := s3a.validateObjectLockHeaders(req, true) // versioned bucket
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrRetentionDateMustBeFuture))
	})

	t.Run("Mode without retention date", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		req.Header.Set(s3_constants.AmzObjectLockMode, "COMPLIANCE")

		err := s3a.validateObjectLockHeaders(req, true) // versioned bucket
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrObjectLockModeRequiresDate))
	})

	t.Run("Retention date without mode", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		retainUntilDate := time.Now().Add(24 * time.Hour)
		req.Header.Set(s3_constants.AmzObjectLockRetainUntilDate, retainUntilDate.Format(time.RFC3339))

		err := s3a.validateObjectLockHeaders(req, true) // versioned bucket
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrRetentionDateRequiresMode))
	})

	t.Run("Governance bypass header on non-versioned bucket", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		req.Header.Set("x-amz-bypass-governance-retention", "true")

		err := s3a.validateObjectLockHeaders(req, false) // non-versioned bucket
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrGovernanceBypassVersioningRequired))
	})

	t.Run("Governance bypass header on versioned bucket should pass", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		req.Header.Set("x-amz-bypass-governance-retention", "true")

		err := s3a.validateObjectLockHeaders(req, true) // versioned bucket
		assert.NoError(t, err)
	})

	t.Run("No object lock headers should pass", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		// No object lock headers set

		err := s3a.validateObjectLockHeaders(req, true) // versioned bucket
		assert.NoError(t, err)
	})

	t.Run("Mixed valid headers should pass", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/object", nil)
		retainUntilDate := time.Now().Add(48 * time.Hour)
		req.Header.Set(s3_constants.AmzObjectLockMode, "GOVERNANCE")
		req.Header.Set(s3_constants.AmzObjectLockRetainUntilDate, retainUntilDate.Format(time.RFC3339))
		req.Header.Set(s3_constants.AmzObjectLockLegalHold, "ON")

		err := s3a.validateObjectLockHeaders(req, true) // versioned bucket
		assert.NoError(t, err)
	})
}

// TestMapValidationErrorToS3Error tests the error mapping function
func TestMapValidationErrorToS3Error(t *testing.T) {
	tests := []struct {
		name         string
		inputError   error
		expectedCode s3err.ErrorCode
	}{
		{
			name:         "ErrObjectLockVersioningRequired",
			inputError:   ErrObjectLockVersioningRequired,
			expectedCode: s3err.ErrInvalidRequest,
		},
		{
			name:         "ErrInvalidObjectLockMode",
			inputError:   ErrInvalidObjectLockMode,
			expectedCode: s3err.ErrInvalidRequest,
		},
		{
			name:         "ErrInvalidLegalHoldStatus",
			inputError:   ErrInvalidLegalHoldStatus,
			expectedCode: s3err.ErrMalformedXML,
		},
		{
			name:         "ErrInvalidRetentionDateFormat",
			inputError:   ErrInvalidRetentionDateFormat,
			expectedCode: s3err.ErrMalformedDate,
		},
		{
			name:         "ErrRetentionDateMustBeFuture",
			inputError:   ErrRetentionDateMustBeFuture,
			expectedCode: s3err.ErrInvalidRequest,
		},
		{
			name:         "ErrObjectLockModeRequiresDate",
			inputError:   ErrObjectLockModeRequiresDate,
			expectedCode: s3err.ErrInvalidRequest,
		},
		{
			name:         "ErrRetentionDateRequiresMode",
			inputError:   ErrRetentionDateRequiresMode,
			expectedCode: s3err.ErrInvalidRequest,
		},
		{
			name:         "ErrGovernanceBypassVersioningRequired",
			inputError:   ErrGovernanceBypassVersioningRequired,
			expectedCode: s3err.ErrInvalidRequest,
		},
		{
			name:         "Unknown error defaults to ErrInvalidRequest",
			inputError:   fmt.Errorf("unknown error"),
			expectedCode: s3err.ErrInvalidRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapValidationErrorToS3Error(tt.inputError)
			assert.Equal(t, tt.expectedCode, result)
		})
	}
}

// TestObjectLockPermissionLogic documents the correct behavior for object lock permission checks
// in PUT operations for both versioned and non-versioned buckets
func TestObjectLockPermissionLogic(t *testing.T) {
	t.Run("Non-versioned bucket PUT operation logic", func(t *testing.T) {
		// In non-versioned buckets, PUT operations overwrite existing objects
		// Therefore, we MUST check if the existing object has object lock protections
		// that would prevent overwrite before allowing the PUT operation.
		//
		// This test documents the expected behavior:
		// 1. Check object lock headers validity (handled by validateObjectLockHeaders)
		// 2. Check if existing object has object lock protections (handled by checkObjectLockPermissions)
		// 3. If existing object is under retention/legal hold, deny the PUT unless governance bypass is valid

		t.Log("For non-versioned buckets:")
		t.Log("- PUT operations overwrite existing objects")
		t.Log("- Must check existing object lock protections before allowing overwrite")
		t.Log("- Governance bypass headers can be used to override GOVERNANCE mode retention")
		t.Log("- COMPLIANCE mode retention and legal holds cannot be bypassed")
	})

	t.Run("Versioned bucket PUT operation logic", func(t *testing.T) {
		// In versioned buckets, PUT operations create new versions without overwriting existing ones
		// Therefore, we do NOT need to check existing object permissions since we're not modifying them.
		// We only need to validate the object lock headers for the new version being created.
		//
		// This test documents the expected behavior:
		// 1. Check object lock headers validity (handled by validateObjectLockHeaders)
		// 2. Skip checking existing object permissions (since we're creating a new version)
		// 3. Apply object lock metadata to the new version being created

		t.Log("For versioned buckets:")
		t.Log("- PUT operations create new versions without overwriting existing objects")
		t.Log("- No need to check existing object lock protections")
		t.Log("- Only validate object lock headers for the new version being created")
		t.Log("- Each version has independent object lock settings")
	})

	t.Run("Governance bypass header validation", func(t *testing.T) {
		// Governance bypass headers should only be used in specific scenarios:
		// 1. Only valid on versioned buckets (consistent with object lock headers)
		// 2. For non-versioned buckets: Used to override existing object's GOVERNANCE retention
		// 3. For versioned buckets: Not typically needed since new versions don't conflict with existing ones

		t.Log("Governance bypass behavior:")
		t.Log("- Only valid on versioned buckets (header validation)")
		t.Log("- For non-versioned buckets: Allows overwriting objects under GOVERNANCE retention")
		t.Log("- For versioned buckets: Not typically needed for PUT operations")
		t.Log("- Must have s3:BypassGovernanceRetention permission")
	})
}
