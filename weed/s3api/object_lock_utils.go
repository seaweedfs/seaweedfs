package s3api

import (
	"encoding/xml"
	"fmt"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_objectlock"
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
				Mode:     mode,
				Days:     days,
				Years:    years,
				DaysSet:  days > 0,
				YearsSet: years > 0,
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
		if defaultRetention.DaysSet && defaultRetention.Days > 0 {
			entry.Extended[s3_constants.ExtObjectLockDefaultDaysKey] = []byte(strconv.Itoa(defaultRetention.Days))
		}

		// Store years
		if defaultRetention.YearsSet && defaultRetention.Years > 0 {
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
					Mode:     mode,
					Days:     days,
					Years:    years,
					DaysSet:  days > 0,
					YearsSet: years > 0,
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
	days := 0
	if defaultRetention.DaysSet {
		days = defaultRetention.Days
	}
	if defaultRetention.YearsSet && defaultRetention.Years > 0 {
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

// ====================================================================
// OBJECT LOCK VALIDATION FUNCTIONS
// ====================================================================
// These validation functions provide comprehensive validation for
// all Object Lock related configurations and requests.

// ValidateRetention validates retention configuration for object-level retention
func ValidateRetention(retention *ObjectRetention) error {
	// Check if mode is specified
	if retention.Mode == "" {
		return ErrRetentionMissingMode
	}

	// Check if retain until date is specified
	if retention.RetainUntilDate == nil {
		return ErrRetentionMissingRetainUntilDate
	}

	// Check if mode is valid
	if retention.Mode != s3_constants.RetentionModeGovernance && retention.Mode != s3_constants.RetentionModeCompliance {
		return ErrInvalidRetentionModeValue
	}

	// Check if retain until date is in the future
	if retention.RetainUntilDate.Before(time.Now()) {
		return ErrRetentionDateMustBeFuture
	}

	return nil
}

// ValidateLegalHold validates legal hold configuration
func ValidateLegalHold(legalHold *ObjectLegalHold) error {
	// Check if status is valid
	if legalHold.Status != s3_constants.LegalHoldOn && legalHold.Status != s3_constants.LegalHoldOff {
		return ErrInvalidLegalHoldStatus
	}

	return nil
}

// ValidateObjectLockConfiguration validates object lock configuration at bucket level
func ValidateObjectLockConfiguration(config *ObjectLockConfiguration) error {
	// ObjectLockEnabled is required for bucket-level configuration
	if config.ObjectLockEnabled == "" {
		return ErrObjectLockConfigurationMissingEnabled
	}

	// Validate ObjectLockEnabled value
	if config.ObjectLockEnabled != s3_constants.ObjectLockEnabled {
		// ObjectLockEnabled can only be 'Enabled', any other value (including 'Disabled') is malformed XML
		return ErrInvalidObjectLockEnabledValue
	}

	// Validate Rule if present
	if config.Rule != nil {
		if config.Rule.DefaultRetention == nil {
			return ErrRuleMissingDefaultRetention
		}
		return validateDefaultRetention(config.Rule.DefaultRetention)
	}

	return nil
}

// validateDefaultRetention validates default retention configuration for bucket-level settings
func validateDefaultRetention(retention *DefaultRetention) error {
	glog.V(2).Infof("validateDefaultRetention: Mode=%s, Days=%d (set=%v), Years=%d (set=%v)",
		retention.Mode, retention.Days, retention.DaysSet, retention.Years, retention.YearsSet)

	// Mode is required
	if retention.Mode == "" {
		return ErrDefaultRetentionMissingMode
	}

	// Mode must be valid
	if retention.Mode != s3_constants.RetentionModeGovernance && retention.Mode != s3_constants.RetentionModeCompliance {
		return ErrInvalidDefaultRetentionMode
	}

	// Check for invalid Years value (negative values are always invalid)
	if retention.YearsSet && retention.Years < 0 {
		return ErrInvalidRetentionPeriod
	}

	// Check for invalid Days value (negative values are invalid)
	if retention.DaysSet && retention.Days < 0 {
		return ErrInvalidRetentionPeriod
	}

	// Check for invalid Days value (zero is invalid when explicitly provided)
	if retention.DaysSet && retention.Days == 0 {
		return ErrInvalidRetentionPeriod
	}

	// Check for neither Days nor Years being specified
	if !retention.DaysSet && !retention.YearsSet {
		return ErrDefaultRetentionMissingPeriod
	}

	// Check for both Days and Years being specified
	if retention.DaysSet && retention.YearsSet {
		return ErrDefaultRetentionBothDaysAndYears
	}

	// Validate Days if specified
	if retention.DaysSet && retention.Days > 0 {
		if retention.Days > MaxRetentionDays {
			return ErrDefaultRetentionDaysOutOfRange
		}
	}

	// Validate Years if specified
	if retention.YearsSet && retention.Years > 0 {
		if retention.Years > MaxRetentionYears {
			return ErrDefaultRetentionYearsOutOfRange
		}
	}

	return nil
}

// ====================================================================
// SHARED OBJECT LOCK CHECKING FUNCTIONS
// ====================================================================
// These functions delegate to s3_objectlock package to avoid code duplication.
// They are kept here for backward compatibility with existing callers.

// EntryHasActiveLock checks if an entry has an active retention or legal hold
// Delegates to s3_objectlock.EntryHasActiveLock
func EntryHasActiveLock(entry *filer_pb.Entry, currentTime time.Time) bool {
	return s3_objectlock.EntryHasActiveLock(entry, currentTime)
}

// HasObjectsWithActiveLocks checks if any objects in the bucket have active retention or legal hold
// Delegates to s3_objectlock.HasObjectsWithActiveLocks
func HasObjectsWithActiveLocks(client filer_pb.SeaweedFilerClient, bucketPath string) (bool, error) {
	return s3_objectlock.HasObjectsWithActiveLocks(client, bucketPath)
}

// CheckBucketForLockedObjects is a unified function that checks if a bucket has Object Lock enabled
// and if so, scans for objects with active locks.
// Delegates to s3_objectlock.CheckBucketForLockedObjects
func CheckBucketForLockedObjects(client filer_pb.SeaweedFilerClient, bucketsPath, bucketName string) error {
	return s3_objectlock.CheckBucketForLockedObjects(client, bucketsPath, bucketName)
}
