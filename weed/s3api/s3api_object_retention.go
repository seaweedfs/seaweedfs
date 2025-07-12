package s3api

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
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

// Custom time marshalling for AWS S3 ISO8601 format
func (or *ObjectRetention) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type Alias ObjectRetention
	aux := &struct {
		*Alias
		RetainUntilDate *string `xml:"RetainUntilDate,omitempty"`
	}{
		Alias: (*Alias)(or),
	}

	if or.RetainUntilDate != nil {
		dateStr := or.RetainUntilDate.UTC().Format(time.RFC3339)
		aux.RetainUntilDate = &dateStr
	}

	return e.EncodeElement(aux, start)
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
func parseXML[T any](r *http.Request, result *T) error {
	if r.Body == nil {
		return fmt.Errorf("empty request body")
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("error reading request body: %v", err)
	}

	if err := xml.Unmarshal(body, result); err != nil {
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

// getObjectRetention retrieves retention configuration from object metadata
func (s3a *S3ApiServer) getObjectRetention(bucket, object, versionId string) (*ObjectRetention, error) {
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

	if entry.Extended == nil {
		return nil, fmt.Errorf("no retention configuration found")
	}

	retention := &ObjectRetention{}

	if modeBytes, exists := entry.Extended[s3_constants.ExtRetentionModeKey]; exists {
		retention.Mode = string(modeBytes)
	}

	if dateBytes, exists := entry.Extended[s3_constants.ExtRetentionUntilDateKey]; exists {
		if timestamp, err := strconv.ParseInt(string(dateBytes), 10, 64); err == nil {
			t := time.Unix(timestamp, 0)
			retention.RetainUntilDate = &t
		}
	}

	if retention.Mode == "" && retention.RetainUntilDate == nil {
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
		if existingMode, exists := entry.Extended[s3_constants.ExtRetentionModeKey]; exists {
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
		entry.Extended[s3_constants.ExtRetentionModeKey] = []byte(retention.Mode)
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

	// Check existing WORM enforcement for compatibility
	if err := s3a.checkLegacyWormEnforcement(bucket, object, versionId, bypassGovernance); err != nil {
		return err
	}

	return nil
}

// checkLegacyWormEnforcement checks the existing WORM infrastructure for compatibility
func (s3a *S3ApiServer) checkLegacyWormEnforcement(bucket, object, versionId string, bypassGovernance bool) error {
	var entry *filer_pb.Entry
	var err error

	if versionId != "" {
		entry, err = s3a.getSpecificObjectVersion(bucket, object, versionId)
	} else {
		// Check if versioning is enabled
		versioningEnabled, vErr := s3a.isVersioningEnabled(bucket)
		if vErr != nil {
			// If we can't check versioning, skip WORM check to avoid false positives
			return nil
		}

		if versioningEnabled {
			entry, err = s3a.getLatestObjectVersion(bucket, object)
		} else {
			bucketDir := s3a.option.BucketsPath + "/" + bucket
			entry, err = s3a.getEntry(bucketDir, object)
		}
	}

	if err != nil {
		// If object doesn't exist, no WORM enforcement applies
		return nil
	}

	// Check if legacy WORM is enforced
	if entry.WormEnforcedAtTsNs == 0 {
		return nil
	}

	// Check if this is under legacy WORM enforcement
	// For compatibility, we treat legacy WORM similar to GOVERNANCE mode
	// (can be bypassed with appropriate permissions)
	if !bypassGovernance {
		return fmt.Errorf("object is under legacy WORM enforcement and cannot be deleted or modified without bypass")
	}

	return nil
}

// isObjectWormProtected checks both S3 retention and legacy WORM for complete protection status
func (s3a *S3ApiServer) isObjectWormProtected(bucket, object, versionId string) (bool, error) {
	// Check S3 object retention
	retentionActive, err := s3a.isObjectRetentionActive(bucket, object, versionId)
	if err != nil {
		glog.V(4).Infof("Error checking S3 retention for %s/%s: %v", bucket, object, err)
	}

	// Check S3 legal hold
	legalHoldActive, err := s3a.isObjectLegalHoldActive(bucket, object, versionId)
	if err != nil {
		glog.V(4).Infof("Error checking S3 legal hold for %s/%s: %v", bucket, object, err)
	}

	// Check legacy WORM enforcement
	legacyWormActive, err := s3a.isLegacyWormActive(bucket, object, versionId)
	if err != nil {
		glog.V(4).Infof("Error checking legacy WORM for %s/%s: %v", bucket, object, err)
	}

	return retentionActive || legalHoldActive || legacyWormActive, nil
}

// isLegacyWormActive checks if an object is under legacy WORM enforcement
func (s3a *S3ApiServer) isLegacyWormActive(bucket, object, versionId string) (bool, error) {
	var entry *filer_pb.Entry
	var err error

	if versionId != "" {
		entry, err = s3a.getSpecificObjectVersion(bucket, object, versionId)
	} else {
		// Check if versioning is enabled
		versioningEnabled, vErr := s3a.isVersioningEnabled(bucket)
		if vErr != nil {
			return false, nil
		}

		if versioningEnabled {
			entry, err = s3a.getLatestObjectVersion(bucket, object)
		} else {
			bucketDir := s3a.option.BucketsPath + "/" + bucket
			entry, err = s3a.getEntry(bucketDir, object)
		}
	}

	if err != nil {
		return false, nil
	}

	// Check if legacy WORM is enforced and still active
	if entry.WormEnforcedAtTsNs == 0 {
		return false, nil
	}

	// For now, we consider legacy WORM as always active when set
	// The original WORM system should handle time-based expiration
	return true, nil
}
