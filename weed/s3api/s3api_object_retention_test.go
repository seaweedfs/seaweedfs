package s3api

import (
	"io"
	"net/http"
	"strings"
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

func TestParseObjectLockConfiguration(t *testing.T) {
	tests := []struct {
		name           string
		xmlBody        string
		expectError    bool
		errorMsg       string
		expectedConfig *ObjectLockConfiguration
	}{
		{
			name: "Valid configuration with days",
			xmlBody: `<?xml version="1.0" encoding="UTF-8"?>
<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <ObjectLockEnabled>Enabled</ObjectLockEnabled>
    <Rule>
        <DefaultRetention>
            <Mode>GOVERNANCE</Mode>
            <Days>30</Days>
        </DefaultRetention>
    </Rule>
</ObjectLockConfiguration>`,
			expectError: false,
			expectedConfig: &ObjectLockConfiguration{
				ObjectLockEnabled: s3_constants.ObjectLockEnabled,
				Rule: &ObjectLockRule{
					DefaultRetention: &DefaultRetention{
						Mode: s3_constants.RetentionModeGovernance,
						Days: 30,
					},
				},
			},
		},
		{
			name: "Valid configuration with years",
			xmlBody: `<?xml version="1.0" encoding="UTF-8"?>
<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <ObjectLockEnabled>Enabled</ObjectLockEnabled>
    <Rule>
        <DefaultRetention>
            <Mode>COMPLIANCE</Mode>
            <Years>1</Years>
        </DefaultRetention>
    </Rule>
</ObjectLockConfiguration>`,
			expectError: false,
			expectedConfig: &ObjectLockConfiguration{
				ObjectLockEnabled: s3_constants.ObjectLockEnabled,
				Rule: &ObjectLockRule{
					DefaultRetention: &DefaultRetention{
						Mode:  s3_constants.RetentionModeCompliance,
						Years: 1,
					},
				},
			},
		},
		{
			name: "Configuration with ObjectLockEnabled only",
			xmlBody: `<?xml version="1.0" encoding="UTF-8"?>
<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <ObjectLockEnabled>Enabled</ObjectLockEnabled>
</ObjectLockConfiguration>`,
			expectError: false,
			expectedConfig: &ObjectLockConfiguration{
				ObjectLockEnabled: s3_constants.ObjectLockEnabled,
			},
		},
		{
			name:        "Empty body",
			xmlBody:     "",
			expectError: true,
			errorMsg:    "empty request body",
		},
		{
			name:        "Invalid XML",
			xmlBody:     "<InvalidXML>",
			expectError: true,
			errorMsg:    "error parsing XML",
		},
		{
			name: "Malformed XML structure",
			xmlBody: `<?xml version="1.0" encoding="UTF-8"?>
<WrongRootElement>
    <SomeData>Invalid</SomeData>
</WrongRootElement>`,
			expectError: true,
			errorMsg:    "error parsing XML",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req *http.Request
			if tt.xmlBody == "" {
				req = &http.Request{Body: nil}
			} else {
				req = &http.Request{
					Body: io.NopCloser(strings.NewReader(tt.xmlBody)),
				}
			}

			config, err := parseObjectLockConfiguration(req)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error message to contain '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %v", err)
				}
				if config == nil {
					t.Errorf("Expected config but got nil")
				}
				if tt.expectedConfig != nil && config != nil {
					if config.ObjectLockEnabled != tt.expectedConfig.ObjectLockEnabled {
						t.Errorf("Expected ObjectLockEnabled '%s', got '%s'", tt.expectedConfig.ObjectLockEnabled, config.ObjectLockEnabled)
					}
					if (config.Rule == nil) != (tt.expectedConfig.Rule == nil) {
						t.Errorf("Rule presence mismatch")
					}
					if config.Rule != nil && tt.expectedConfig.Rule != nil {
						if (config.Rule.DefaultRetention == nil) != (tt.expectedConfig.Rule.DefaultRetention == nil) {
							t.Errorf("DefaultRetention presence mismatch")
						}
						if config.Rule.DefaultRetention != nil && tt.expectedConfig.Rule.DefaultRetention != nil {
							if config.Rule.DefaultRetention.Mode != tt.expectedConfig.Rule.DefaultRetention.Mode {
								t.Errorf("Expected Mode '%s', got '%s'", tt.expectedConfig.Rule.DefaultRetention.Mode, config.Rule.DefaultRetention.Mode)
							}
							if config.Rule.DefaultRetention.Days != tt.expectedConfig.Rule.DefaultRetention.Days {
								t.Errorf("Expected Days %d, got %d", tt.expectedConfig.Rule.DefaultRetention.Days, config.Rule.DefaultRetention.Days)
							}
							if config.Rule.DefaultRetention.Years != tt.expectedConfig.Rule.DefaultRetention.Years {
								t.Errorf("Expected Years %d, got %d", tt.expectedConfig.Rule.DefaultRetention.Years, config.Rule.DefaultRetention.Years)
							}
						}
					}
				}
			}
		})
	}
}

func TestParseXMLGeneric(t *testing.T) {
	tests := []struct {
		name        string
		xmlBody     string
		expectError bool
		errorMsg    string
	}{
		{
			name: "Valid retention XML",
			xmlBody: `<?xml version="1.0" encoding="UTF-8"?>
<Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Mode>GOVERNANCE</Mode>
    <RetainUntilDate>2024-12-31T23:59:59Z</RetainUntilDate>
</Retention>`,
			expectError: false,
		},
		{
			name:        "Empty body",
			xmlBody:     "",
			expectError: true,
			errorMsg:    "empty request body",
		},
		{
			name:        "Invalid XML",
			xmlBody:     "<InvalidXML>",
			expectError: true,
			errorMsg:    "error parsing XML",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req *http.Request
			if tt.xmlBody == "" {
				req = &http.Request{Body: nil}
			} else {
				req = &http.Request{
					Body: io.NopCloser(strings.NewReader(tt.xmlBody)),
				}
			}

			var retention ObjectRetention
			err := parseXML(req, &retention)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error message to contain '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %v", err)
				}
			}
		})
	}
}
