package s3api

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

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
			err := ValidateRetention(tt.retention)

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
			err := ValidateLegalHold(tt.legalHold)

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

func TestParseObjectRetention(t *testing.T) {
	tests := []struct {
		name           string
		xmlBody        string
		expectError    bool
		errorMsg       string
		expectedResult *ObjectRetention
	}{
		{
			name: "Valid retention XML",
			xmlBody: `<Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
				<Mode>GOVERNANCE</Mode>
				<RetainUntilDate>2024-12-31T23:59:59Z</RetainUntilDate>
			</Retention>`,
			expectError: false,
			expectedResult: &ObjectRetention{
				Mode:            "GOVERNANCE",
				RetainUntilDate: timePtr(time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)),
			},
		},
		{
			name: "Valid compliance retention XML",
			xmlBody: `<Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
				<Mode>COMPLIANCE</Mode>
				<RetainUntilDate>2025-01-01T00:00:00Z</RetainUntilDate>
			</Retention>`,
			expectError: false,
			expectedResult: &ObjectRetention{
				Mode:            "COMPLIANCE",
				RetainUntilDate: timePtr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
			},
		},
		{
			name: "Valid retention XML without namespace (Veeam compatibility)",
			xmlBody: `<Retention>
				<Mode>GOVERNANCE</Mode>
				<RetainUntilDate>2024-12-31T23:59:59Z</RetainUntilDate>
			</Retention>`,
			expectError: false,
			expectedResult: &ObjectRetention{
				Mode:            "GOVERNANCE",
				RetainUntilDate: timePtr(time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)),
			},
		},
		{
			name: "Valid compliance retention XML without namespace (Veeam compatibility)",
			xmlBody: `<Retention>
				<Mode>COMPLIANCE</Mode>
				<RetainUntilDate>2025-01-01T00:00:00Z</RetainUntilDate>
			</Retention>`,
			expectError: false,
			expectedResult: &ObjectRetention{
				Mode:            "COMPLIANCE",
				RetainUntilDate: timePtr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
			},
		},
		{
			name:        "Empty XML body",
			xmlBody:     "",
			expectError: true,
			errorMsg:    "error parsing XML",
		},
		{
			name:        "Invalid XML",
			xmlBody:     `<Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Mode>GOVERNANCE</Mode><RetainUntilDate>invalid-date</RetainUntilDate></Retention>`,
			expectError: true,
			errorMsg:    "cannot parse",
		},
		{
			name:        "Malformed XML",
			xmlBody:     "<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2024-12-31T23:59:59Z</Retention>",
			expectError: true,
			errorMsg:    "error parsing XML",
		},
		{
			name: "Missing Mode",
			xmlBody: `<Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
				<RetainUntilDate>2024-12-31T23:59:59Z</RetainUntilDate>
			</Retention>`,
			expectError: false,
			expectedResult: &ObjectRetention{
				Mode:            "",
				RetainUntilDate: timePtr(time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)),
			},
		},
		{
			name: "Missing RetainUntilDate",
			xmlBody: `<Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
				<Mode>GOVERNANCE</Mode>
			</Retention>`,
			expectError: false,
			expectedResult: &ObjectRetention{
				Mode:            "GOVERNANCE",
				RetainUntilDate: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock HTTP request with XML body
			req := &http.Request{
				Body: io.NopCloser(strings.NewReader(tt.xmlBody)),
			}

			result, err := parseObjectRetention(req)

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
				if result == nil {
					t.Errorf("Expected result but got nil")
				} else {
					if result.Mode != tt.expectedResult.Mode {
						t.Errorf("Expected Mode %s, got %s", tt.expectedResult.Mode, result.Mode)
					}
					if tt.expectedResult.RetainUntilDate == nil {
						if result.RetainUntilDate != nil {
							t.Errorf("Expected RetainUntilDate to be nil, got %v", result.RetainUntilDate)
						}
					} else if result.RetainUntilDate == nil {
						t.Errorf("Expected RetainUntilDate to be %v, got nil", tt.expectedResult.RetainUntilDate)
					} else if !result.RetainUntilDate.Equal(*tt.expectedResult.RetainUntilDate) {
						t.Errorf("Expected RetainUntilDate %v, got %v", tt.expectedResult.RetainUntilDate, result.RetainUntilDate)
					}
				}
			}
		})
	}
}

func TestParseObjectLegalHold(t *testing.T) {
	tests := []struct {
		name           string
		xmlBody        string
		expectError    bool
		errorMsg       string
		expectedResult *ObjectLegalHold
	}{
		{
			name: "Valid legal hold ON",
			xmlBody: `<LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
				<Status>ON</Status>
			</LegalHold>`,
			expectError: false,
			expectedResult: &ObjectLegalHold{
				Status: "ON",
			},
		},
		{
			name: "Valid legal hold OFF",
			xmlBody: `<LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
				<Status>OFF</Status>
			</LegalHold>`,
			expectError: false,
			expectedResult: &ObjectLegalHold{
				Status: "OFF",
			},
		},
		{
			name: "Valid legal hold ON without namespace",
			xmlBody: `<LegalHold>
				<Status>ON</Status>
			</LegalHold>`,
			expectError: false,
			expectedResult: &ObjectLegalHold{
				Status: "ON",
			},
		},
		{
			name: "Valid legal hold OFF without namespace",
			xmlBody: `<LegalHold>
				<Status>OFF</Status>
			</LegalHold>`,
			expectError: false,
			expectedResult: &ObjectLegalHold{
				Status: "OFF",
			},
		},
		{
			name:        "Empty XML body",
			xmlBody:     "",
			expectError: true,
			errorMsg:    "error parsing XML",
		},
		{
			name:        "Invalid XML",
			xmlBody:     "<LegalHold><Status>ON</Status>",
			expectError: true,
			errorMsg:    "error parsing XML",
		},
		{
			name: "Missing Status",
			xmlBody: `<LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			</LegalHold>`,
			expectError: false,
			expectedResult: &ObjectLegalHold{
				Status: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock HTTP request with XML body
			req := &http.Request{
				Body: io.NopCloser(strings.NewReader(tt.xmlBody)),
			}

			result, err := parseObjectLegalHold(req)

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
				if result == nil {
					t.Errorf("Expected result but got nil")
				} else {
					if result.Status != tt.expectedResult.Status {
						t.Errorf("Expected Status %s, got %s", tt.expectedResult.Status, result.Status)
					}
				}
			}
		})
	}
}

func TestParseObjectLockConfiguration(t *testing.T) {
	tests := []struct {
		name           string
		xmlBody        string
		expectError    bool
		errorMsg       string
		expectedResult *ObjectLockConfiguration
	}{
		{
			name: "Valid object lock configuration",
			xmlBody: `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
				<ObjectLockEnabled>Enabled</ObjectLockEnabled>
			</ObjectLockConfiguration>`,
			expectError: false,
			expectedResult: &ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
			},
		},
		{
			name: "Valid object lock configuration with rule",
			xmlBody: `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
				<ObjectLockEnabled>Enabled</ObjectLockEnabled>
				<Rule>
					<DefaultRetention>
						<Mode>GOVERNANCE</Mode>
						<Days>30</Days>
					</DefaultRetention>
				</Rule>
			</ObjectLockConfiguration>`,
			expectError: false,
			expectedResult: &ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
				Rule: &ObjectLockRule{
					DefaultRetention: &DefaultRetention{
						Mode: "GOVERNANCE",
						Days: 30,
					},
				},
			},
		},
		{
			name: "Valid object lock configuration without namespace",
			xmlBody: `<ObjectLockConfiguration>
				<ObjectLockEnabled>Enabled</ObjectLockEnabled>
			</ObjectLockConfiguration>`,
			expectError: false,
			expectedResult: &ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
			},
		},
		{
			name: "Valid object lock configuration with rule without namespace",
			xmlBody: `<ObjectLockConfiguration>
				<ObjectLockEnabled>Enabled</ObjectLockEnabled>
				<Rule>
					<DefaultRetention>
						<Mode>GOVERNANCE</Mode>
						<Days>30</Days>
					</DefaultRetention>
				</Rule>
			</ObjectLockConfiguration>`,
			expectError: false,
			expectedResult: &ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
				Rule: &ObjectLockRule{
					DefaultRetention: &DefaultRetention{
						Mode: "GOVERNANCE",
						Days: 30,
					},
				},
			},
		},
		{
			name:        "Empty XML body",
			xmlBody:     "",
			expectError: true,
			errorMsg:    "error parsing XML",
		},
		{
			name:        "Invalid XML",
			xmlBody:     "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>",
			expectError: true,
			errorMsg:    "error parsing XML",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock HTTP request with XML body
			req := &http.Request{
				Body: io.NopCloser(strings.NewReader(tt.xmlBody)),
			}

			result, err := parseObjectLockConfiguration(req)

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
				if result == nil {
					t.Errorf("Expected result but got nil")
				} else {
					if result.ObjectLockEnabled != tt.expectedResult.ObjectLockEnabled {
						t.Errorf("Expected ObjectLockEnabled %s, got %s", tt.expectedResult.ObjectLockEnabled, result.ObjectLockEnabled)
					}
					if tt.expectedResult.Rule == nil {
						if result.Rule != nil {
							t.Errorf("Expected Rule to be nil, got %v", result.Rule)
						}
					} else if result.Rule == nil {
						t.Errorf("Expected Rule to be non-nil")
					} else {
						if result.Rule.DefaultRetention == nil {
							t.Errorf("Expected DefaultRetention to be non-nil")
						} else {
							if result.Rule.DefaultRetention.Mode != tt.expectedResult.Rule.DefaultRetention.Mode {
								t.Errorf("Expected DefaultRetention Mode %s, got %s", tt.expectedResult.Rule.DefaultRetention.Mode, result.Rule.DefaultRetention.Mode)
							}
							if result.Rule.DefaultRetention.Days != tt.expectedResult.Rule.DefaultRetention.Days {
								t.Errorf("Expected DefaultRetention Days %d, got %d", tt.expectedResult.Rule.DefaultRetention.Days, result.Rule.DefaultRetention.Days)
							}
						}
					}
				}
			}
		})
	}
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
			name: "Missing ObjectLockEnabled",
			config: &ObjectLockConfiguration{
				ObjectLockEnabled: "",
			},
			expectError: true,
			errorMsg:    "object lock configuration must specify ObjectLockEnabled",
		},
		{
			name: "Valid config with rule and days",
			config: &ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
				Rule: &ObjectLockRule{
					DefaultRetention: &DefaultRetention{
						Mode:    "GOVERNANCE",
						Days:    30,
						DaysSet: true,
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
						Mode:     "COMPLIANCE",
						Years:    1,
						YearsSet: true,
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
						Mode:     "GOVERNANCE",
						Days:     30,
						Years:    1,
						DaysSet:  true,
						YearsSet: true,
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
						Mode:    "INVALID_MODE",
						Days:    30,
						DaysSet: true,
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
						Mode:    "GOVERNANCE",
						Days:    50000,
						DaysSet: true,
					},
				},
			},
			expectError: true,
			errorMsg:    fmt.Sprintf("default retention days must be between 0 and %d", MaxRetentionDays),
		},
		{
			name: "Invalid rule - years out of range",
			config: &ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
				Rule: &ObjectLockRule{
					DefaultRetention: &DefaultRetention{
						Mode:     "GOVERNANCE",
						Years:    200,
						YearsSet: true,
					},
				},
			},
			expectError: true,
			errorMsg:    fmt.Sprintf("default retention years must be between 0 and %d", MaxRetentionYears),
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
			err := ValidateObjectLockConfiguration(tt.config)

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
				Mode:    "GOVERNANCE",
				Days:    30,
				DaysSet: true,
			},
			expectError: false,
		},
		{
			name: "Valid retention with years",
			retention: &DefaultRetention{
				Mode:     "COMPLIANCE",
				Years:    1,
				YearsSet: true,
			},
			expectError: false,
		},
		{
			name: "Missing mode",
			retention: &DefaultRetention{
				Days:    30,
				DaysSet: true,
			},
			expectError: true,
			errorMsg:    "default retention must specify Mode",
		},
		{
			name: "Invalid mode",
			retention: &DefaultRetention{
				Mode:    "INVALID",
				Days:    30,
				DaysSet: true,
			},
			expectError: true,
			errorMsg:    "invalid default retention mode",
		},
		{
			name: "Both days and years specified",
			retention: &DefaultRetention{
				Mode:     "GOVERNANCE",
				Days:     30,
				Years:    1,
				DaysSet:  true,
				YearsSet: true,
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
