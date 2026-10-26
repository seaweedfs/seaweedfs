package s3api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
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
		{
			name: "Zero years",
			retention: &DefaultRetention{
				Mode:     "GOVERNANCE",
				Years:    0,
				YearsSet: true,
			},
			expectError: true,
			errorMsg:    "invalid retention period specified",
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

func TestStoreObjectLockConfigurationClearsStaleYears(t *testing.T) {
	entry := &filer_pb.Entry{
		Extended: map[string][]byte{
			s3_constants.ExtObjectLockEnabledKey:      []byte(s3_constants.ObjectLockEnabled),
			s3_constants.ExtObjectLockDefaultModeKey:  []byte(s3_constants.RetentionModeCompliance),
			s3_constants.ExtObjectLockDefaultYearsKey: []byte("1"),
		},
	}
	config := CreateObjectLockConfiguration(true, s3_constants.RetentionModeGovernance, 30, 0)

	if err := StoreObjectLockConfigurationInExtended(entry, config); err != nil {
		t.Fatalf("StoreObjectLockConfigurationInExtended returned error: %v", err)
	}

	if _, exists := entry.Extended[s3_constants.ExtObjectLockDefaultYearsKey]; exists {
		t.Fatalf("stale default retention years key was not cleared")
	}

	loaded, found := LoadObjectLockConfigurationFromExtended(entry)
	if !found {
		t.Fatalf("expected stored object lock configuration to load")
	}
	if loaded.Rule == nil || loaded.Rule.DefaultRetention == nil {
		t.Fatalf("expected loaded configuration to include default retention")
	}

	retention := loaded.Rule.DefaultRetention
	if !retention.DaysSet || retention.Days != 30 {
		t.Fatalf("expected loaded retention days to be 30, got DaysSet=%v Days=%d", retention.DaysSet, retention.Days)
	}
	if retention.YearsSet || retention.Years != 0 {
		t.Fatalf("expected loaded retention years to be cleared, got YearsSet=%v Years=%d", retention.YearsSet, retention.Years)
	}
}

// Helper function to create a time pointer
func timePtr(t time.Time) *time.Time {
	return &t
}

// retentionTestFiler serves the two filer calls a lock write needs: the lookup
// that resolves the version and the update that persists the modified entry.
type retentionTestFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer

	mu         sync.Mutex
	entries    map[string]*filer_pb.Entry
	updates    []string
	lookupErrs map[string]error
}

func (f *retentionTestFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.lookupErrs[req.Directory+"/"+req.Name]; ok {
		return nil, err
	}
	if entry, ok := f.entries[req.Directory+"/"+req.Name]; ok {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: proto.Clone(entry).(*filer_pb.Entry)}, nil
	}
	return &filer_pb.LookupDirectoryEntryResponse{}, nil
}

func (f *retentionTestFiler) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates = append(f.updates, req.Directory+"/"+req.Entry.Name)
	f.entries[req.Directory+"/"+req.Entry.Name] = req.Entry
	return &filer_pb.UpdateEntryResponse{}, nil
}

func newRetentionTestServer(t *testing.T, f *retentionTestFiler) *S3ApiServer {
	t.Helper()
	addr := startFakeFiler(t, f)
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	return &S3ApiServer{
		option: &S3ApiServerOption{
			Filers:         []pb.ServerAddress{addr},
			GrpcDialOption: dialOption,
			BucketsPath:    "/buckets",
		},
		filerClient:       wdclient.NewFilerClient([]pb.ServerAddress{addr}, dialOption, ""),
		bucketConfigCache: NewBucketConfigCache(time.Minute),
	}
}

// A "null" version id resolves to the pre-versioning entry under the plain
// object key, the same place getSpecificObjectVersion reads it from. Writing
// the lock under .versions/v_null leaves the real entry unlocked.
func TestNullVersionLockWritesToPlainEntry(t *testing.T) {
	f := &retentionTestFiler{entries: map[string]*filer_pb.Entry{
		"/buckets/b/key": {
			Name:    "key",
			Chunks:  []*filer_pb.FileChunk{{FileId: "1,0000000000000001"}},
			Content: []byte("inline"),
			Attributes: &filer_pb.FuseAttributes{
				Mime: "text/plain",
				Md5:  []byte("etagmd5"),
			},
			Extended: map[string][]byte{
				"ETag": []byte("\"etagmd5\""),
			},
		},
	}}
	s3a := newRetentionTestServer(t, f)

	retainUntil := time.Now().Add(24 * time.Hour).Truncate(time.Second)
	err := s3a.setObjectRetention("b", "key", "null", &ObjectRetention{
		Mode:            s3_constants.RetentionModeCompliance,
		RetainUntilDate: &retainUntil,
	}, false)
	if err != nil {
		t.Fatalf("setObjectRetention: %v", err)
	}
	if len(f.updates) != 1 || f.updates[0] != "/buckets/b/key" {
		t.Fatalf("retention write landed on %v, want /buckets/b/key", f.updates)
	}
	updated := f.entries["/buckets/b/key"]
	if got := string(updated.Extended[s3_constants.ExtObjectLockModeKey]); got != s3_constants.RetentionModeCompliance {
		t.Fatalf("plain entry lock mode = %q, want COMPLIANCE", got)
	}
	if updated.Attributes.Mime != "text/plain" || len(updated.Content) == 0 || len(updated.Chunks) != 1 || string(updated.Extended["ETag"]) != "\"etagmd5\"" {
		t.Fatalf("lock write dropped entry fields: %+v", updated)
	}

	f.updates = nil
	if err := s3a.setObjectLegalHold("b", "key", "null", &ObjectLegalHold{Status: "ON"}); err != nil {
		t.Fatalf("setObjectLegalHold: %v", err)
	}
	if len(f.updates) != 1 || f.updates[0] != "/buckets/b/key" {
		t.Fatalf("legal hold write landed on %v, want /buckets/b/key", f.updates)
	}
	if got := string(f.entries["/buckets/b/key"].Extended[s3_constants.ExtLegalHoldKey]); got != "ON" {
		t.Fatalf("plain entry legal hold = %q, want ON", got)
	}
}

// getObjectEntry must only report ErrObjectNotFound for a genuine miss. A
// transient filer error used to be collapsed into ErrObjectNotFound, which
// LifecycleDelete resolves permanently as NOOP_RESOLVED - one flaky read at
// dispatch time would silently skip the object's expiration.
func TestGetObjectEntryPreservesTransientErrors(t *testing.T) {
	f := &retentionTestFiler{
		entries: map[string]*filer_pb.Entry{
			"/buckets/b": {Name: "b", IsDirectory: true},
		},
		lookupErrs: map[string]error{
			"/buckets/b/key": status.Error(codes.Unavailable, "filer overloaded"),
		},
	}
	s3a := newRetentionTestServer(t, f)

	_, err := s3a.getObjectEntry("b", "key", "")
	if err == nil {
		t.Fatal("expected error for transient lookup failure")
	}
	if errors.Is(err, ErrObjectNotFound) || errors.Is(err, filer_pb.ErrNotFound) {
		t.Fatalf("transient lookup error must not collapse to not-found: %v", err)
	}
}

// A transport failure whose status text happens to carry the filer not-found
// sentinel (e.g. a proxy re-wrapping a miss as Unavailable) is a failed read,
// not a miss - LookupEntry must not normalize it to ErrNotFound.
func TestGetObjectEntryPreservesUnavailableWithNotFoundMessage(t *testing.T) {
	f := &retentionTestFiler{
		entries: map[string]*filer_pb.Entry{
			"/buckets/b": {Name: "b", IsDirectory: true},
		},
		lookupErrs: map[string]error{
			"/buckets/b/key": status.Error(codes.Unavailable, filer_pb.ErrNotFound.Error()),
		},
	}
	s3a := newRetentionTestServer(t, f)

	_, err := s3a.getObjectEntry("b", "key", "")
	if err == nil {
		t.Fatal("expected error for unavailable lookup")
	}
	if errors.Is(err, ErrObjectNotFound) || errors.Is(err, filer_pb.ErrNotFound) {
		t.Fatalf("unavailable lookup must not collapse to not-found: %v", err)
	}
}

// Only miss-shaped statuses may carry the not-found text into ErrObjectNotFound:
// a raw sentinel crossing gRPC arrives as codes.Unknown, while the same text on
// codes.Internal is a failed read.
func TestGetObjectEntryNotFoundTextStatusGate(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		wantMiss bool
	}{
		{"unknown keeps sentinel text", status.Error(codes.Unknown, filer_pb.ErrNotFound.Error()), true},
		{"internal with sentinel text", status.Error(codes.Internal, filer_pb.ErrNotFound.Error()), false},
		{"not-found status", status.Error(codes.NotFound, "entry gone"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &retentionTestFiler{
				entries: map[string]*filer_pb.Entry{
					"/buckets/b": {Name: "b", IsDirectory: true},
				},
				lookupErrs: map[string]error{
					"/buckets/b/key": tc.err,
				},
			}
			s3a := newRetentionTestServer(t, f)

			_, err := s3a.getObjectEntry("b", "key", "")
			if err == nil {
				t.Fatal("expected error")
			}
			if got := errors.Is(err, ErrObjectNotFound); got != tc.wantMiss {
				t.Fatalf("isObjectEntryMiss = %v, want %v: %v", got, tc.wantMiss, err)
			}
		})
	}
}

// A pre-versioning object under a now-versioned bucket has no .versions
// directory; when the plain-object fallback read fails transiently, that
// failure must surface instead of the directory's not-found error.
func TestGetObjectEntryPreservesTransientFallbackError(t *testing.T) {
	f := &retentionTestFiler{
		entries: map[string]*filer_pb.Entry{
			"/buckets/b": {
				Name:        "b",
				IsDirectory: true,
				Extended:    map[string][]byte{s3_constants.ExtVersioningKey: []byte(s3_constants.VersioningEnabled)},
			},
		},
		lookupErrs: map[string]error{
			"/buckets/b/key": status.Error(codes.Unavailable, "filer overloaded"),
		},
	}
	s3a := newRetentionTestServer(t, f)

	_, err := s3a.getObjectEntry("b", "key", "")
	if err == nil {
		t.Fatal("expected error for transient fallback lookup failure")
	}
	if errors.Is(err, ErrObjectNotFound) || errors.Is(err, filer_pb.ErrNotFound) {
		t.Fatalf("transient fallback error must not collapse to not-found: %v", err)
	}
}

func TestGetObjectEntryInvalidVersionIdStillNotFound(t *testing.T) {
	f := &retentionTestFiler{
		entries: map[string]*filer_pb.Entry{
			"/buckets/b": {Name: "b", IsDirectory: true},
		},
	}
	s3a := newRetentionTestServer(t, f)

	_, err := s3a.getObjectEntry("b", "key", "not/../a-version")
	if !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("invalid version id must report ErrObjectNotFound, got: %v", err)
	}
}

func TestGetObjectEntryMissStillNotFound(t *testing.T) {
	f := &retentionTestFiler{
		entries: map[string]*filer_pb.Entry{
			"/buckets/b": {Name: "b", IsDirectory: true},
		},
	}
	s3a := newRetentionTestServer(t, f)

	_, err := s3a.getObjectEntry("b", "missing", "")
	if !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("missing object must report ErrObjectNotFound, got: %v", err)
	}
}
