package s3api

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestValidateRetention(t *testing.T) {
	futureTime := time.Now().Add(24 * time.Hour)
	pastTime := time.Now().Add(-24 * time.Hour)

	tests := []struct {
		name        string
		retention   *ObjectRetention
		expectError bool
		errorMsg    string
	}{
		{
			name: "Valid GOVERNANCE retention",
			retention: &ObjectRetention{
				Mode:            s3_constants.RetentionModeGovernance,
				RetainUntilDate: &futureTime,
			},
			expectError: false,
		},
		{
			name: "Valid COMPLIANCE retention",
			retention: &ObjectRetention{
				Mode:            s3_constants.RetentionModeCompliance,
				RetainUntilDate: &futureTime,
			},
			expectError: false,
		},
		{
			name: "Missing Mode",
			retention: &ObjectRetention{
				RetainUntilDate: &futureTime,
			},
			expectError: true,
			errorMsg:    "retention configuration must specify Mode",
		},
		{
			name: "Missing RetainUntilDate",
			retention: &ObjectRetention{
				Mode: s3_constants.RetentionModeGovernance,
			},
			expectError: true,
			errorMsg:    "retention configuration must specify RetainUntilDate",
		},
		{
			name: "Invalid Mode",
			retention: &ObjectRetention{
				Mode:            "INVALID_MODE",
				RetainUntilDate: &futureTime,
			},
			expectError: true,
			errorMsg:    "invalid retention mode: INVALID_MODE",
		},
		{
			name: "Past RetainUntilDate",
			retention: &ObjectRetention{
				Mode:            s3_constants.RetentionModeGovernance,
				RetainUntilDate: &pastTime,
			},
			expectError: true,
			errorMsg:    "retain until date must be in the future",
		},
		{
			name:        "Empty retention",
			retention:   &ObjectRetention{},
			expectError: true,
			errorMsg:    "retention configuration must specify Mode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRetention(tt.retention)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if err.Error() != tt.errorMsg {
					t.Errorf("Expected error message '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %v", err)
				}
			}
		})
	}
}

func TestValidateLegalHold(t *testing.T) {
	tests := []struct {
		name        string
		legalHold   *ObjectLegalHold
		expectError bool
		errorMsg    string
	}{
		{
			name: "Valid ON status",
			legalHold: &ObjectLegalHold{
				Status: s3_constants.LegalHoldOn,
			},
			expectError: false,
		},
		{
			name: "Valid OFF status",
			legalHold: &ObjectLegalHold{
				Status: s3_constants.LegalHoldOff,
			},
			expectError: false,
		},
		{
			name: "Invalid status",
			legalHold: &ObjectLegalHold{
				Status: "INVALID_STATUS",
			},
			expectError: true,
			errorMsg:    "invalid legal hold status: INVALID_STATUS",
		},
		{
			name: "Empty status",
			legalHold: &ObjectLegalHold{
				Status: "",
			},
			expectError: true,
			errorMsg:    "invalid legal hold status: ",
		},
		{
			name: "Lowercase on",
			legalHold: &ObjectLegalHold{
				Status: "on",
			},
			expectError: true,
			errorMsg:    "invalid legal hold status: on",
		},
		{
			name: "Lowercase off",
			legalHold: &ObjectLegalHold{
				Status: "off",
			},
			expectError: true,
			errorMsg:    "invalid legal hold status: off",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateLegalHold(tt.legalHold)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if err.Error() != tt.errorMsg {
					t.Errorf("Expected error message '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %v", err)
				}
			}
		})
	}
}
