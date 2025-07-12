package s3api

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

const (
	// Maximum retention period limits according to AWS S3 specifications
	MaxRetentionDays  = 36500 // Maximum number of days for object retention (100 years)
	MaxRetentionYears = 100   // Maximum number of years for object retention
)

// ObjectRetention represents S3 Object Retention configuration
type ObjectRetention struct {
	XMLName         xml.Name   `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Retention"`
	Mode            string     `xml:"Mode,omitempty"`
	RetainUntilDate *time.Time `xml:"RetainUntilDate,omitempty"`
}

// ObjectLegalHold represents S3 Object Legal Hold configuration
type ObjectLegalHold struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LegalHold"`
	Status  string   `xml:"Status,omitempty"`
}

// ObjectLockConfiguration represents S3 Object Lock Configuration
type ObjectLockConfiguration struct {
	XMLName           xml.Name        `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ObjectLockConfiguration"`
	ObjectLockEnabled string          `xml:"ObjectLockEnabled,omitempty"`
	Rule              *ObjectLockRule `xml:"Rule,omitempty"`
}

// ObjectLockRule represents an Object Lock Rule
type ObjectLockRule struct {
	XMLName          xml.Name          `xml:"Rule"`
	DefaultRetention *DefaultRetention `xml:"DefaultRetention,omitempty"`
}

// DefaultRetention represents default retention settings
type DefaultRetention struct {
	XMLName xml.Name `xml:"DefaultRetention"`
	Mode    string   `xml:"Mode,omitempty"`
	Days    int      `xml:"Days,omitempty"`
	Years   int      `xml:"Years,omitempty"`
}

// Custom time unmarshalling for AWS S3 ISO8601 format
func (or *ObjectRetention) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type Alias ObjectRetention
	aux := &struct {
		*Alias
		RetainUntilDate *string `xml:"RetainUntilDate,omitempty"`
	}{
		Alias: (*Alias)(or),
	}

	if err := d.DecodeElement(aux, &start); err != nil {
		return err
	}

	if aux.RetainUntilDate != nil {
		t, err := time.Parse(time.RFC3339, *aux.RetainUntilDate)
		if err != nil {
			return err
		}
		or.RetainUntilDate = &t
	}

	return nil
}

// parseXML is a generic helper function to parse XML from request body
// This implementation uses xml.Decoder for streaming XML parsing, which is more memory-efficient
// and avoids loading the entire request body into memory.
func parseXML[T any](r *http.Request, result *T) error {
	if r.Body == nil {
		return fmt.Errorf("empty request body")
	}
	defer r.Body.Close()

	decoder := xml.NewDecoder(r.Body)
	if err := decoder.Decode(result); err != nil {
		return fmt.Errorf("error parsing XML: %v", err)
	}

	return nil
}

// parseObjectRetention parses XML retention configuration from request body
func parseObjectRetention(r *http.Request) (*ObjectRetention, error) {
	var retention ObjectRetention
	if err := parseXML(r, &retention); err != nil {
		return nil, err
	}
	return &retention, nil
}

// parseObjectLegalHold parses XML legal hold configuration from request body
func parseObjectLegalHold(r *http.Request) (*ObjectLegalHold, error) {
	var legalHold ObjectLegalHold
	if err := parseXML(r, &legalHold); err != nil {
		return nil, err
	}
	return &legalHold, nil
}

// parseObjectLockConfiguration parses XML object lock configuration from request body
func parseObjectLockConfiguration(r *http.Request) (*ObjectLockConfiguration, error) {
	var config ObjectLockConfiguration
	if err := parseXML(r, &config); err != nil {
		return nil, err
	}
	return &config, nil
}

// validateRetention validates retention configuration
func validateRetention(retention *ObjectRetention) error {
	// AWS requires both Mode and RetainUntilDate for PutObjectRetention
	if retention.Mode == "" {
		return fmt.Errorf("retention configuration must specify Mode")
	}

	if retention.RetainUntilDate == nil {
		return fmt.Errorf("retention configuration must specify RetainUntilDate")
	}

	if retention.Mode != s3_constants.RetentionModeGovernance && retention.Mode != s3_constants.RetentionModeCompliance {
		return fmt.Errorf("invalid retention mode: %s", retention.Mode)
	}

	if retention.RetainUntilDate.Before(time.Now()) {
		return fmt.Errorf("retain until date must be in the future")
	}

	return nil
}

// validateLegalHold validates legal hold configuration
func validateLegalHold(legalHold *ObjectLegalHold) error {
	if legalHold.Status != s3_constants.LegalHoldOn && legalHold.Status != s3_constants.LegalHoldOff {
		return fmt.Errorf("invalid legal hold status: %s", legalHold.Status)
	}

	return nil
}

// validateObjectLockConfiguration validates object lock configuration
func validateObjectLockConfiguration(config *ObjectLockConfiguration) error {
	// ObjectLockEnabled is required for bucket-level configuration
	if config.ObjectLockEnabled == "" {
		return fmt.Errorf("object lock configuration must specify ObjectLockEnabled")
	}

	// Validate ObjectLockEnabled value
	if config.ObjectLockEnabled != s3_constants.ObjectLockEnabled {
		return fmt.Errorf("invalid object lock enabled value: %s", config.ObjectLockEnabled)
	}

	// Validate Rule if present
	if config.Rule != nil {
		if config.Rule.DefaultRetention == nil {
			return fmt.Errorf("rule configuration must specify DefaultRetention")
		}
		return validateDefaultRetention(config.Rule.DefaultRetention)
	}

	return nil
}

// validateDefaultRetention validates default retention configuration
func validateDefaultRetention(retention *DefaultRetention) error {
	// Mode is required
	if retention.Mode == "" {
		return fmt.Errorf("default retention must specify Mode")
	}

	// Mode must be valid
	if retention.Mode != s3_constants.RetentionModeGovernance && retention.Mode != s3_constants.RetentionModeCompliance {
		return fmt.Errorf("invalid default retention mode: %s", retention.Mode)
	}

	// Exactly one of Days or Years must be specified
	if retention.Days == 0 && retention.Years == 0 {
		return fmt.Errorf("default retention must specify either Days or Years")
	}

	if retention.Days > 0 && retention.Years > 0 {
		return fmt.Errorf("default retention cannot specify both Days and Years")
	}

	// Validate ranges
	if retention.Days < 0 || retention.Days > MaxRetentionDays {
		return fmt.Errorf("default retention days must be between 0 and %d", MaxRetentionDays)
	}

	if retention.Years < 0 || retention.Years > MaxRetentionYears {
		return fmt.Errorf("default retention years must be between 0 and %d", MaxRetentionYears)
	}

	return nil
}

// getObjectEntry retrieves the appropriate object entry based on versioning and versionId
func (s3a *S3ApiServer) getObjectEntry(bucket, object, versionId string) (*filer_pb.Entry, error) {
	var entry *filer_pb.Entry
	var err error

	if versionId != "" {
		entry, err = s3a.getSpecificObjectVersion(bucket, object, versionId)
	} else {
		// Check if versioning is enabled
		versioningEnabled, vErr := s3a.isVersioningEnabled(bucket)
		if vErr != nil {
			return nil, fmt.Errorf("error checking versioning: %v", vErr)
		}

		if versioningEnabled {
			entry, err = s3a.getLatestObjectVersion(bucket, object)
		} else {
			bucketDir := s3a.option.BucketsPath + "/" + bucket
			entry, err = s3a.getEntry(bucketDir, object)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("object not found: %v", err)
	}

	return entry, nil
}

// getObjectRetention retrieves retention configuration from object metadata
func (s3a *S3ApiServer) getObjectRetention(bucket, object, versionId string) (*ObjectRetention, error) {
	entry, err := s3a.getObjectEntry(bucket, object, versionId)
	if err != nil {
		return nil, err
	}

	if entry.Extended == nil {
		return nil, fmt.Errorf("no retention configuration found")
	}

	retention := &ObjectRetention{}

	if modeBytes, exists := entry.Extended[s3_constants.ExtObjectLockModeKey]; exists {
		retention.Mode = string(modeBytes)
	}

	if dateBytes, exists := entry.Extended[s3_constants.ExtRetentionUntilDateKey]; exists {
		if timestamp, err := strconv.ParseInt(string(dateBytes), 10, 64); err == nil {
			t := time.Unix(timestamp, 0)
			retention.RetainUntilDate = &t
		} else {
			return nil, fmt.Errorf("failed to parse retention timestamp for %s/%s: %v (stored value: %s)", bucket, object, err, string(dateBytes))
		}
	}

	if retention.Mode == "" || retention.RetainUntilDate == nil {
		return nil, fmt.Errorf("no retention configuration found")
	}

	return retention, nil
}

// setObjectRetention sets retention configuration on object metadata
func (s3a *S3ApiServer) setObjectRetention(bucket, object, versionId string, retention *ObjectRetention, bypassGovernance bool) error {
	var entry *filer_pb.Entry
	var err error
	var entryPath string

	if versionId != "" {
		entry, err = s3a.getSpecificObjectVersion(bucket, object, versionId)
		if err != nil {
			return fmt.Errorf("version not found: %v", err)
		}
		// For versioned objects, we need to update the version file
		entryPath = object + ".versions/" + s3a.getVersionFileName(versionId)
	} else {
		// Check if versioning is enabled
		versioningEnabled, vErr := s3a.isVersioningEnabled(bucket)
		if vErr != nil {
			return fmt.Errorf("error checking versioning: %v", vErr)
		}

		if versioningEnabled {
			entry, err = s3a.getLatestObjectVersion(bucket, object)
			if err != nil {
				return fmt.Errorf("latest version not found: %v", err)
			}
			// Extract version ID from entry metadata
			if entry.Extended != nil {
				if versionIdBytes, exists := entry.Extended[s3_constants.ExtVersionIdKey]; exists {
					versionId = string(versionIdBytes)
					entryPath = object + ".versions/" + s3a.getVersionFileName(versionId)
				}
			}
		} else {
			bucketDir := s3a.option.BucketsPath + "/" + bucket
			entry, err = s3a.getEntry(bucketDir, object)
			if err != nil {
				return fmt.Errorf("object not found: %v", err)
			}
			entryPath = object
		}
	}

	// Check if object is already under retention
	if entry.Extended != nil {
		if existingMode, exists := entry.Extended[s3_constants.ExtObjectLockModeKey]; exists {
			if string(existingMode) == s3_constants.RetentionModeCompliance && !bypassGovernance {
				return fmt.Errorf("cannot modify retention on object under COMPLIANCE mode")
			}

			if existingDateBytes, dateExists := entry.Extended[s3_constants.ExtRetentionUntilDateKey]; dateExists {
				if timestamp, err := strconv.ParseInt(string(existingDateBytes), 10, 64); err == nil {
					existingDate := time.Unix(timestamp, 0)
					if existingDate.After(time.Now()) && string(existingMode) == s3_constants.RetentionModeGovernance && !bypassGovernance {
						return fmt.Errorf("cannot modify retention on object under GOVERNANCE mode without bypass")
					}
				}
			}
		}
	}

	// Update retention metadata
	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	}

	if retention.Mode != "" {
		entry.Extended[s3_constants.ExtObjectLockModeKey] = []byte(retention.Mode)
	}

	if retention.RetainUntilDate != nil {
		entry.Extended[s3_constants.ExtRetentionUntilDateKey] = []byte(strconv.FormatInt(retention.RetainUntilDate.Unix(), 10))

		// Also update the existing WORM fields for compatibility
		entry.WormEnforcedAtTsNs = time.Now().UnixNano()
	}

	// Update the entry
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	return s3a.mkFile(bucketDir, entryPath, entry.Chunks, func(updatedEntry *filer_pb.Entry) {
		updatedEntry.Extended = entry.Extended
		updatedEntry.WormEnforcedAtTsNs = entry.WormEnforcedAtTsNs
	})
}

// getObjectLegalHold retrieves legal hold configuration from object metadata
func (s3a *S3ApiServer) getObjectLegalHold(bucket, object, versionId string) (*ObjectLegalHold, error) {
	entry, err := s3a.getObjectEntry(bucket, object, versionId)
	if err != nil {
		return nil, err
	}

	if entry.Extended == nil {
		return nil, fmt.Errorf("no legal hold configuration found")
	}

	legalHold := &ObjectLegalHold{}

	if statusBytes, exists := entry.Extended[s3_constants.ExtLegalHoldKey]; exists {
		legalHold.Status = string(statusBytes)
	} else {
		return nil, fmt.Errorf("no legal hold configuration found")
	}

	return legalHold, nil
}

// setObjectLegalHold sets legal hold configuration on object metadata
func (s3a *S3ApiServer) setObjectLegalHold(bucket, object, versionId string, legalHold *ObjectLegalHold) error {
	var entry *filer_pb.Entry
	var err error
	var entryPath string

	if versionId != "" {
		entry, err = s3a.getSpecificObjectVersion(bucket, object, versionId)
		if err != nil {
			return fmt.Errorf("version not found: %v", err)
		}
		entryPath = object + ".versions/" + s3a.getVersionFileName(versionId)
	} else {
		// Check if versioning is enabled
		versioningEnabled, vErr := s3a.isVersioningEnabled(bucket)
		if vErr != nil {
			return fmt.Errorf("error checking versioning: %v", vErr)
		}

		if versioningEnabled {
			entry, err = s3a.getLatestObjectVersion(bucket, object)
			if err != nil {
				return fmt.Errorf("latest version not found: %v", err)
			}
			// Extract version ID from entry metadata
			if entry.Extended != nil {
				if versionIdBytes, exists := entry.Extended[s3_constants.ExtVersionIdKey]; exists {
					versionId = string(versionIdBytes)
					entryPath = object + ".versions/" + s3a.getVersionFileName(versionId)
				}
			}
		} else {
			bucketDir := s3a.option.BucketsPath + "/" + bucket
			entry, err = s3a.getEntry(bucketDir, object)
			if err != nil {
				return fmt.Errorf("object not found: %v", err)
			}
			entryPath = object
		}
	}

	// Update legal hold metadata
	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	}

	entry.Extended[s3_constants.ExtLegalHoldKey] = []byte(legalHold.Status)

	// Update the entry
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	return s3a.mkFile(bucketDir, entryPath, entry.Chunks, func(updatedEntry *filer_pb.Entry) {
		updatedEntry.Extended = entry.Extended
	})
}

// isObjectRetentionActive checks if an object is currently under retention
func (s3a *S3ApiServer) isObjectRetentionActive(bucket, object, versionId string) (bool, error) {
	retention, err := s3a.getObjectRetention(bucket, object, versionId)
	if err != nil {
		// If no retention found, object is not under retention
		if strings.Contains(err.Error(), "no retention configuration found") {
			return false, nil
		}
		return false, err
	}

	if retention.RetainUntilDate != nil && retention.RetainUntilDate.After(time.Now()) {
		return true, nil
	}

	return false, nil
}

// isObjectLegalHoldActive checks if an object is currently under legal hold
func (s3a *S3ApiServer) isObjectLegalHoldActive(bucket, object, versionId string) (bool, error) {
	legalHold, err := s3a.getObjectLegalHold(bucket, object, versionId)
	if err != nil {
		// If no legal hold found, object is not under legal hold
		if strings.Contains(err.Error(), "no legal hold configuration found") {
			return false, nil
		}
		return false, err
	}

	return legalHold.Status == s3_constants.LegalHoldOn, nil
}

// checkObjectLockPermissions checks if an object can be deleted or modified
func (s3a *S3ApiServer) checkObjectLockPermissions(bucket, object, versionId string, bypassGovernance bool) error {
	// Check if object is under retention
	retentionActive, err := s3a.isObjectRetentionActive(bucket, object, versionId)
	if err != nil {
		glog.Warningf("Error checking retention for %s/%s: %v", bucket, object, err)
	}

	// Check if object is under legal hold
	legalHoldActive, err := s3a.isObjectLegalHoldActive(bucket, object, versionId)
	if err != nil {
		glog.Warningf("Error checking legal hold for %s/%s: %v", bucket, object, err)
	}

	// If object is under legal hold, it cannot be deleted or modified
	if legalHoldActive {
		return fmt.Errorf("object is under legal hold and cannot be deleted or modified")
	}

	// If object is under retention, check the mode
	if retentionActive {
		retention, err := s3a.getObjectRetention(bucket, object, versionId)
		if err != nil {
			return fmt.Errorf("error getting retention configuration: %v", err)
		}

		if retention.Mode == s3_constants.RetentionModeCompliance {
			return fmt.Errorf("object is under COMPLIANCE mode retention and cannot be deleted or modified")
		}

		if retention.Mode == s3_constants.RetentionModeGovernance && !bypassGovernance {
			return fmt.Errorf("object is under GOVERNANCE mode retention and cannot be deleted or modified without bypass")
		}
	}

	return nil
}

// isObjectLockAvailable checks if Object Lock features are available for the bucket
// Object Lock requires versioning to be enabled (AWS S3 requirement)
func (s3a *S3ApiServer) isObjectLockAvailable(bucket string) error {
	versioningEnabled, err := s3a.isVersioningEnabled(bucket)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return fmt.Errorf("bucket not found")
		}
		return fmt.Errorf("error checking versioning status: %v", err)
	}

	if !versioningEnabled {
		return fmt.Errorf("object lock requires versioning to be enabled")
	}

	return nil
}

// checkObjectLockPermissionsForPut checks object lock permissions for PUT operations
// This is a shared helper to avoid code duplication in PUT handlers
func (s3a *S3ApiServer) checkObjectLockPermissionsForPut(bucket, object string, bypassGovernance bool, versioningEnabled bool) error {
	// Object Lock only applies to versioned buckets (AWS S3 requirement)
	if !versioningEnabled {
		return nil
	}

	// For PUT operations, we check permissions on the current object (empty versionId)
	if err := s3a.checkObjectLockPermissions(bucket, object, "", bypassGovernance); err != nil {
		glog.V(2).Infof("checkObjectLockPermissionsForPut: object lock check failed for %s/%s: %v", bucket, object, err)
		return err
	}
	return nil
}
