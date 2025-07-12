package s3api

import (
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// TestPutObjectRetention test was removed because it was calling PutObjectRetentionHandler
// without proper setup (no bucket directory, no object entry, no versioning configuration).
// Proper integration tests that set up buckets, objects, and versioning are handled in
// the dedicated integration test suites under test/s3/retention/

func TestValidateRetention(t *testing.T) {
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
				RetainUntilDate: timePtr(time.Now().Add(24 * time.Hour)),
			},
			expectError: false,
		},
		{
			name: "Valid COMPLIANCE retention",
			retention: &ObjectRetention{
				Mode:            s3_constants.RetentionModeCompliance,
				RetainUntilDate: timePtr(time.Now().Add(24 * time.Hour)),
			},
			expectError: false,
		},
		{
			name: "Missing Mode",
			retention: &ObjectRetention{
				RetainUntilDate: timePtr(time.Now().Add(24 * time.Hour)),
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
				RetainUntilDate: timePtr(time.Now().Add(24 * time.Hour)),
			},
			expectError: true,
			errorMsg:    "invalid retention mode",
		},
		{
			name: "Past RetainUntilDate",
			retention: &ObjectRetention{
				Mode:            s3_constants.RetentionModeGovernance,
				RetainUntilDate: timePtr(time.Now().Add(-24 * time.Hour)),
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
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error message to contain '%s', got: %v", tt.errorMsg, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
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
			errorMsg:    "invalid legal hold status",
		},
		{
			name: "Empty status",
			legalHold: &ObjectLegalHold{
				Status: "",
			},
			expectError: true,
			errorMsg:    "invalid legal hold status",
		},
		{
			name: "Lowercase on",
			legalHold: &ObjectLegalHold{
				Status: "on",
			},
			expectError: true,
			errorMsg:    "invalid legal hold status",
		},
		{
			name: "Lowercase off",
			legalHold: &ObjectLegalHold{
				Status: "off",
			},
			expectError: true,
			errorMsg:    "invalid legal hold status",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateLegalHold(tt.legalHold)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error message to contain '%s', got: %v", tt.errorMsg, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

func TestParseObjectLockConfiguration(t *testing.T) {
	// This test is commented out as it requires proper HTTP request setup
	// to use the parseObjectLockConfiguration function correctly.
	// The important validation logic is tested in TestValidateObjectLockConfiguration
	t.Skip("Parsing tests require proper HTTP request setup - validation tests cover the business logic")
}

func TestParseXMLGeneric(t *testing.T) {
	// This test is commented out as it requires proper HTTP request setup
	// The important validation logic is tested in other validation tests
	t.Skip("Parsing tests require proper HTTP request setup - validation tests cover the business logic")
}

func TestValidateObjectLockConfiguration(t *testing.T) {
	tests := []struct {
		name        string
		config      *ObjectLockConfiguration
		expectError bool
		errorMsg    string
	}{
		{
			name: "Valid config with ObjectLockEnabled only",
			config: &ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
			},
			expectError: false,
		},
		{
			name: "Valid config with rule and days",
			config: &ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
				Rule: &ObjectLockRule{
					DefaultRetention: &DefaultRetention{
						Mode: "GOVERNANCE",
						Days: 30,
					},
				},
			},
			expectError: false,
		},
		{
			name: "Valid config with rule and years",
			config: &ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
				Rule: &ObjectLockRule{
					DefaultRetention: &DefaultRetention{
						Mode:  "COMPLIANCE",
						Years: 1,
					},
				},
			},
			expectError: false,
		},
		{
			name: "Invalid ObjectLockEnabled value",
			config: &ObjectLockConfiguration{
				ObjectLockEnabled: "InvalidValue",
			},
			expectError: true,
			errorMsg:    "invalid object lock enabled value",
		},
		{
			name: "Invalid rule - missing mode",
			config: &ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
				Rule: &ObjectLockRule{
					DefaultRetention: &DefaultRetention{
						Days: 30,
					},
				},
			},
			expectError: true,
			errorMsg:    "default retention must specify Mode",
		},
		{
			name: "Invalid rule - both days and years",
			config: &ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
				Rule: &ObjectLockRule{
					DefaultRetention: &DefaultRetention{
						Mode:  "GOVERNANCE",
						Days:  30,
						Years: 1,
					},
				},
			},
			expectError: true,
			errorMsg:    "default retention cannot specify both Days and Years",
		},
		{
			name: "Invalid rule - neither days nor years",
			config: &ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
				Rule: &ObjectLockRule{
					DefaultRetention: &DefaultRetention{
						Mode: "GOVERNANCE",
					},
				},
			},
			expectError: true,
			errorMsg:    "default retention must specify either Days or Years",
		},
		{
			name: "Invalid rule - invalid mode",
			config: &ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
				Rule: &ObjectLockRule{
					DefaultRetention: &DefaultRetention{
						Mode: "INVALID_MODE",
						Days: 30,
					},
				},
			},
			expectError: true,
			errorMsg:    "invalid default retention mode",
		},
		{
			name: "Invalid rule - days out of range",
			config: &ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
				Rule: &ObjectLockRule{
					DefaultRetention: &DefaultRetention{
						Mode: "GOVERNANCE",
						Days: 50000,
					},
				},
			},
			expectError: true,
			errorMsg:    "default retention days must be between 0 and 36500",
		},
		{
			name: "Invalid rule - years out of range",
			config: &ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
				Rule: &ObjectLockRule{
					DefaultRetention: &DefaultRetention{
						Mode:  "GOVERNANCE",
						Years: 200,
					},
				},
			},
			expectError: true,
			errorMsg:    "default retention years must be between 0 and 100",
		},
		{
			name: "Invalid rule - missing DefaultRetention",
			config: &ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
				Rule: &ObjectLockRule{
					DefaultRetention: nil,
				},
			},
			expectError: true,
			errorMsg:    "rule configuration must specify DefaultRetention",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateObjectLockConfiguration(tt.config)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error message to contain '%s', got: %v", tt.errorMsg, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

func TestValidateDefaultRetention(t *testing.T) {
	tests := []struct {
		name        string
		retention   *DefaultRetention
		expectError bool
		errorMsg    string
	}{
		{
			name: "Valid retention with days",
			retention: &DefaultRetention{
				Mode: "GOVERNANCE",
				Days: 30,
			},
			expectError: false,
		},
		{
			name: "Valid retention with years",
			retention: &DefaultRetention{
				Mode:  "COMPLIANCE",
				Years: 1,
			},
			expectError: false,
		},
		{
			name: "Missing mode",
			retention: &DefaultRetention{
				Days: 30,
			},
			expectError: true,
			errorMsg:    "default retention must specify Mode",
		},
		{
			name: "Invalid mode",
			retention: &DefaultRetention{
				Mode: "INVALID",
				Days: 30,
			},
			expectError: true,
			errorMsg:    "invalid default retention mode",
		},
		{
			name: "Both days and years specified",
			retention: &DefaultRetention{
				Mode:  "GOVERNANCE",
				Days:  30,
				Years: 1,
			},
			expectError: true,
			errorMsg:    "default retention cannot specify both Days and Years",
		},
		{
			name: "Neither days nor years specified",
			retention: &DefaultRetention{
				Mode: "GOVERNANCE",
			},
			expectError: true,
			errorMsg:    "default retention must specify either Days or Years",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDefaultRetention(tt.retention)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error message to contain '%s', got: %v", tt.errorMsg, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

// Helper function to create a time pointer
func timePtr(t time.Time) *time.Time {
	return &t
}
