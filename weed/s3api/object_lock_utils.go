package s3api

import (
	"encoding/xml"
	"fmt"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// ObjectLockUtils provides shared utilities for Object Lock configuration
// These functions are used by both Admin UI and S3 API handlers to ensure consistency

// VersioningUtils provides shared utilities for bucket versioning configuration
// These functions ensure Admin UI and S3 API use the same versioning keys

// StoreVersioningInExtended stores versioning configuration in entry extended attributes
func StoreVersioningInExtended(entry *filer_pb.Entry, enabled bool) error {
	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	}

	if enabled {
		entry.Extended[s3_constants.ExtVersioningKey] = []byte(s3_constants.VersioningEnabled)
	} else {
		entry.Extended[s3_constants.ExtVersioningKey] = []byte(s3_constants.VersioningSuspended)
	}

	return nil
}

// LoadVersioningFromExtended loads versioning configuration from entry extended attributes
func LoadVersioningFromExtended(entry *filer_pb.Entry) (bool, bool) {
	if entry == nil || entry.Extended == nil {
		return false, false // not found, default to suspended
	}

	// Check for S3 API compatible key
	if versioningBytes, exists := entry.Extended[s3_constants.ExtVersioningKey]; exists {
		enabled := string(versioningBytes) == s3_constants.VersioningEnabled
		return enabled, true
	}

	return false, false // not found
}

// CreateObjectLockConfiguration creates a new ObjectLockConfiguration with the specified parameters
func CreateObjectLockConfiguration(enabled bool, mode string, days int, years int) *ObjectLockConfiguration {
	if !enabled {
		return nil
	}

	config := &ObjectLockConfiguration{
		ObjectLockEnabled: s3_constants.ObjectLockEnabled,
	}

	// Add default retention rule if mode and period are specified
	if mode != "" && (days > 0 || years > 0) {
		config.Rule = &ObjectLockRule{
			DefaultRetention: &DefaultRetention{
				Mode:  mode,
				Days:  days,
				Years: years,
			},
		}
	}

	return config
}

// ObjectLockConfigurationToXML converts ObjectLockConfiguration to XML bytes
func ObjectLockConfigurationToXML(config *ObjectLockConfiguration) ([]byte, error) {
	if config == nil {
		return nil, fmt.Errorf("object lock configuration is nil")
	}

	return xml.Marshal(config)
}

// StoreObjectLockConfigurationInExtended stores Object Lock configuration in entry extended attributes
func StoreObjectLockConfigurationInExtended(entry *filer_pb.Entry, config *ObjectLockConfiguration) error {
	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	}

	if config == nil {
		// Remove Object Lock configuration
		delete(entry.Extended, s3_constants.ExtObjectLockEnabledKey)
		delete(entry.Extended, s3_constants.ExtObjectLockDefaultModeKey)
		delete(entry.Extended, s3_constants.ExtObjectLockDefaultDaysKey)
		delete(entry.Extended, s3_constants.ExtObjectLockDefaultYearsKey)
		return nil
	}

	// Store the enabled flag
	entry.Extended[s3_constants.ExtObjectLockEnabledKey] = []byte(config.ObjectLockEnabled)

	// Store default retention configuration if present
	if config.Rule != nil && config.Rule.DefaultRetention != nil {
		defaultRetention := config.Rule.DefaultRetention

		// Store mode
		if defaultRetention.Mode != "" {
			entry.Extended[s3_constants.ExtObjectLockDefaultModeKey] = []byte(defaultRetention.Mode)
		}

		// Store days
		if defaultRetention.Days > 0 {
			entry.Extended[s3_constants.ExtObjectLockDefaultDaysKey] = []byte(strconv.Itoa(defaultRetention.Days))
		}

		// Store years
		if defaultRetention.Years > 0 {
			entry.Extended[s3_constants.ExtObjectLockDefaultYearsKey] = []byte(strconv.Itoa(defaultRetention.Years))
		}
	} else {
		// Remove default retention if not present
		delete(entry.Extended, s3_constants.ExtObjectLockDefaultModeKey)
		delete(entry.Extended, s3_constants.ExtObjectLockDefaultDaysKey)
		delete(entry.Extended, s3_constants.ExtObjectLockDefaultYearsKey)
	}

	return nil
}

// LoadObjectLockConfigurationFromExtended loads Object Lock configuration from entry extended attributes
func LoadObjectLockConfigurationFromExtended(entry *filer_pb.Entry) (*ObjectLockConfiguration, bool) {
	if entry == nil || entry.Extended == nil {
		return nil, false
	}

	// Check if Object Lock is enabled
	enabledBytes, exists := entry.Extended[s3_constants.ExtObjectLockEnabledKey]
	if !exists {
		return nil, false
	}

	enabled := string(enabledBytes)
	if enabled != s3_constants.ObjectLockEnabled && enabled != "true" {
		return nil, false
	}

	// Create basic configuration
	config := &ObjectLockConfiguration{
		ObjectLockEnabled: s3_constants.ObjectLockEnabled,
	}

	// Load default retention configuration if present
	if modeBytes, exists := entry.Extended[s3_constants.ExtObjectLockDefaultModeKey]; exists {
		mode := string(modeBytes)

		// Parse days and years
		var days, years int
		if daysBytes, exists := entry.Extended[s3_constants.ExtObjectLockDefaultDaysKey]; exists {
			if parsed, err := strconv.Atoi(string(daysBytes)); err == nil {
				days = parsed
			}
		}
		if yearsBytes, exists := entry.Extended[s3_constants.ExtObjectLockDefaultYearsKey]; exists {
			if parsed, err := strconv.Atoi(string(yearsBytes)); err == nil {
				years = parsed
			}
		}

		// Create rule if we have a mode and at least days or years
		if mode != "" && (days > 0 || years > 0) {
			config.Rule = &ObjectLockRule{
				DefaultRetention: &DefaultRetention{
					Mode:  mode,
					Days:  days,
					Years: years,
				},
			}
		}
	}

	return config, true
}

// ExtractObjectLockInfoFromConfig extracts basic Object Lock information from configuration
// Returns: enabled, mode, duration (for UI display)
func ExtractObjectLockInfoFromConfig(config *ObjectLockConfiguration) (bool, string, int32) {
	if config == nil || config.ObjectLockEnabled != s3_constants.ObjectLockEnabled {
		return false, "", 0
	}

	if config.Rule == nil || config.Rule.DefaultRetention == nil {
		return true, "", 0
	}

	defaultRetention := config.Rule.DefaultRetention

	// Convert years to days for consistent representation
	days := defaultRetention.Days
	if defaultRetention.Years > 0 {
		days += defaultRetention.Years * 365
	}

	return true, defaultRetention.Mode, int32(days)
}

// CreateObjectLockConfigurationFromParams creates ObjectLockConfiguration from individual parameters
// This is a convenience function for Admin UI usage
func CreateObjectLockConfigurationFromParams(enabled bool, mode string, duration int32) *ObjectLockConfiguration {
	if !enabled {
		return nil
	}

	return CreateObjectLockConfiguration(enabled, mode, int(duration), 0)
}

// ValidateObjectLockParameters validates Object Lock parameters before creating configuration
func ValidateObjectLockParameters(enabled bool, mode string, duration int32) error {
	if !enabled {
		return nil
	}

	if mode != s3_constants.RetentionModeGovernance && mode != s3_constants.RetentionModeCompliance {
		return ErrInvalidObjectLockMode
	}

	if duration <= 0 {
		return ErrInvalidObjectLockDuration
	}

	if duration > MaxRetentionDays {
		return ErrObjectLockDurationExceeded
	}

	return nil
}
