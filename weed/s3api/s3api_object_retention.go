package s3api

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// ====================================================================
// ERROR DEFINITIONS
// ====================================================================

// Sentinel errors for proper error handling instead of string matching
var (
	ErrNoRetentionConfiguration = errors.New("no retention configuration found")
	ErrNoLegalHoldConfiguration = errors.New("no legal hold configuration found")
	ErrBucketNotFound           = errors.New("bucket not found")
	ErrObjectNotFound           = errors.New("object not found")
	ErrVersionNotFound          = errors.New("version not found")
	ErrLatestVersionNotFound    = errors.New("latest version not found")
	ErrComplianceModeActive     = errors.New("object is under COMPLIANCE mode retention and cannot be deleted or modified")
	ErrGovernanceModeActive     = errors.New("object is under GOVERNANCE mode retention and cannot be deleted or modified without bypass")
)

// Error definitions for Object Lock
var (
	ErrObjectUnderLegalHold         = errors.New("object is under legal hold and cannot be deleted or modified")
	ErrGovernanceBypassNotPermitted = errors.New("user does not have permission to bypass governance retention")
	ErrInvalidRetentionPeriod       = errors.New("invalid retention period specified")
	ErrBothDaysAndYearsSpecified    = errors.New("both days and years cannot be specified in the same retention configuration")
	ErrMalformedXML                 = errors.New("malformed XML in request body")

	// Validation error constants with specific messages for tests
	ErrRetentionMissingMode            = errors.New("retention configuration must specify Mode")
	ErrRetentionMissingRetainUntilDate = errors.New("retention configuration must specify RetainUntilDate")
	ErrInvalidRetentionModeValue       = errors.New("invalid retention mode")
)

const (
	// Maximum retention period limits according to AWS S3 specifications
	MaxRetentionDays  = 36500 // Maximum number of days for object retention (100 years)
	MaxRetentionYears = 100   // Maximum number of years for object retention
)

// ====================================================================
// DATA STRUCTURES
// ====================================================================

// ObjectRetention represents S3 Object Retention configuration
type ObjectRetention struct {
	XMLNS           string     `xml:"xmlns,attr,omitempty"`
	XMLName         xml.Name   `xml:"Retention"`
	Mode            string     `xml:"Mode,omitempty"`
	RetainUntilDate *time.Time `xml:"RetainUntilDate,omitempty"`
}

// ObjectLegalHold represents S3 Object Legal Hold configuration
type ObjectLegalHold struct {
	XMLNS   string   `xml:"xmlns,attr,omitempty"`
	XMLName xml.Name `xml:"LegalHold"`
	Status  string   `xml:"Status,omitempty"`
}

// ObjectLockConfiguration represents S3 Object Lock Configuration
type ObjectLockConfiguration struct {
	XMLNS             string          `xml:"xmlns,attr,omitempty"`
	XMLName           xml.Name        `xml:"ObjectLockConfiguration"`
	ObjectLockEnabled string          `xml:"ObjectLockEnabled,omitempty"`
	Rule              *ObjectLockRule `xml:"Rule,omitempty"`
}

// ObjectLockRule represents an Object Lock Rule
type ObjectLockRule struct {
	XMLName          xml.Name          `xml:"Rule"`
	DefaultRetention *DefaultRetention `xml:"DefaultRetention,omitempty"`
}

// DefaultRetention represents default retention settings
// Implements custom XML unmarshal to track if Days/Years were present in XML
type DefaultRetention struct {
	XMLName  xml.Name `xml:"DefaultRetention"`
	Mode     string   `xml:"Mode,omitempty"`
	Days     int      `xml:"Days,omitempty"`
	Years    int      `xml:"Years,omitempty"`
	DaysSet  bool     `xml:"-"`
	YearsSet bool     `xml:"-"`
}

// ====================================================================
// XML PARSING
// ====================================================================

// UnmarshalXML implements custom XML unmarshaling for DefaultRetention
// to track whether Days/Years fields were explicitly present in the XML
func (dr *DefaultRetention) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type Alias DefaultRetention
	aux := &struct {
		*Alias
		Days  *int `xml:"Days,omitempty"`
		Years *int `xml:"Years,omitempty"`
	}{Alias: (*Alias)(dr)}
	if err := d.DecodeElement(aux, &start); err != nil {
		glog.V(2).Infof("DefaultRetention.UnmarshalXML: decode error: %v", err)
		return err
	}
	if aux.Days != nil {
		dr.Days = *aux.Days
		dr.DaysSet = true
		glog.V(4).Infof("DefaultRetention.UnmarshalXML: Days present, value=%d", dr.Days)
	} else {
		glog.V(4).Infof("DefaultRetention.UnmarshalXML: Days not present")
	}
	if aux.Years != nil {
		dr.Years = *aux.Years
		dr.YearsSet = true
		glog.V(4).Infof("DefaultRetention.UnmarshalXML: Years present, value=%d", dr.Years)
	} else {
		glog.V(4).Infof("DefaultRetention.UnmarshalXML: Years not present")
	}
	return nil
}

// parseXML is a generic helper function to parse XML from an HTTP request body.
// It uses xml.Decoder for streaming XML parsing, which is more memory-efficient
// and avoids loading the entire request body into memory.
//
// The function assumes:
// - The request body is not nil (returns error if it is)
// - The request body will be closed after parsing (deferred close)
// - The XML content matches the structure of the provided result type T
//
// This approach is optimized for small XML payloads typical in S3 API requests
// (retention configurations, legal hold settings, etc.) where the overhead of
// streaming parsing is acceptable for the memory efficiency benefits.
func parseXML[T any](request *http.Request, result *T) error {
	if request.Body == nil {
		return fmt.Errorf("error parsing XML: empty request body")
	}
	defer request.Body.Close()

	decoder := xml.NewDecoder(request.Body)
	if err := decoder.Decode(result); err != nil {
		return fmt.Errorf("error parsing XML: %w", err)
	}

	return nil
}

// parseObjectRetention parses XML retention configuration from request body
func parseObjectRetention(request *http.Request) (*ObjectRetention, error) {
	var retention ObjectRetention
	if err := parseXML(request, &retention); err != nil {
		return nil, err
	}
	return &retention, nil
}

// parseObjectLegalHold parses XML legal hold configuration from request body
func parseObjectLegalHold(request *http.Request) (*ObjectLegalHold, error) {
	var legalHold ObjectLegalHold
	if err := parseXML(request, &legalHold); err != nil {
		return nil, err
	}
	return &legalHold, nil
}

// parseObjectLockConfiguration parses XML object lock configuration from request body
func parseObjectLockConfiguration(request *http.Request) (*ObjectLockConfiguration, error) {
	var config ObjectLockConfiguration
	if err := parseXML(request, &config); err != nil {
		return nil, err
	}
	return &config, nil
}

// ====================================================================
// OBJECT ENTRY OPERATIONS
// ====================================================================

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
			return nil, fmt.Errorf("error checking versioning: %w", vErr)
		}

		if versioningEnabled {
			entry, err = s3a.getLatestObjectVersion(bucket, object)
		} else {
			entry, err = s3a.fetchObjectEntryRequired(bucket, object)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to retrieve object %s/%s: %w", bucket, object, ErrObjectNotFound)
	}

	return entry, nil
}

// ====================================================================
// RETENTION OPERATIONS
// ====================================================================

// getObjectRetention retrieves object retention configuration
func (s3a *S3ApiServer) getObjectRetention(bucket, object, versionId string) (*ObjectRetention, error) {
	entry, err := s3a.getObjectEntry(bucket, object, versionId)
	if err != nil {
		return nil, err
	}

	if entry.Extended == nil {
		return nil, ErrNoRetentionConfiguration
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
			return nil, fmt.Errorf("failed to parse retention timestamp for %s/%s: corrupted timestamp data", bucket, object)
		}
	}

	if retention.Mode == "" || retention.RetainUntilDate == nil {
		return nil, ErrNoRetentionConfiguration
	}

	// Set namespace for S3 compatibility (matches MinIO behavior)
	retention.XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"
	return retention, nil
}

// setObjectRetention sets object retention configuration
func (s3a *S3ApiServer) setObjectRetention(bucket, object, versionId string, retention *ObjectRetention, bypassGovernance bool) error {
	var entry *filer_pb.Entry
	var err error
	var entryPath string

	if versionId != "" {
		entry, err = s3a.getSpecificObjectVersion(bucket, object, versionId)
		if err != nil {
			return fmt.Errorf("failed to get version %s for object %s/%s: %w", versionId, bucket, object, ErrVersionNotFound)
		}
		entryPath = object + ".versions/" + s3a.getVersionFileName(versionId)
	} else {
		// Check if versioning is enabled
		versioningEnabled, vErr := s3a.isVersioningEnabled(bucket)
		if vErr != nil {
			return fmt.Errorf("error checking versioning: %w", vErr)
		}

		if versioningEnabled {
			entry, err = s3a.getLatestObjectVersion(bucket, object)
			if err != nil {
				return fmt.Errorf("failed to get latest version for object %s/%s: %w", bucket, object, ErrLatestVersionNotFound)
			}
			// Extract version ID from entry metadata
			entryPath = object // default to regular object path
			if entry.Extended != nil {
				if versionIdBytes, exists := entry.Extended[s3_constants.ExtVersionIdKey]; exists {
					versionId = string(versionIdBytes)
					if versionId != "null" {
						entryPath = object + ".versions/" + s3a.getVersionFileName(versionId)
					}
				}
			}
		} else {
			entry, err = s3a.fetchObjectEntryRequired(bucket, object)
			if err != nil {
				return fmt.Errorf("failed to get object %s/%s: %w", bucket, object, ErrObjectNotFound)
			}
			entryPath = object
		}
	}

	// Check if object is already under retention
	if entry.Extended != nil {
		if existingMode, exists := entry.Extended[s3_constants.ExtObjectLockModeKey]; exists {
			// Check if attempting to change retention mode
			if retention.Mode != "" && string(existingMode) != retention.Mode {
				// Attempting to change retention mode
				if string(existingMode) == s3_constants.RetentionModeCompliance {
					// Cannot change compliance mode retention without bypass
					return ErrComplianceModeActive
				}

				if string(existingMode) == s3_constants.RetentionModeGovernance && !bypassGovernance {
					// Cannot change governance mode retention without bypass
					return ErrGovernanceModeActive
				}
			}

			if existingDateBytes, dateExists := entry.Extended[s3_constants.ExtRetentionUntilDateKey]; dateExists {
				if timestamp, err := strconv.ParseInt(string(existingDateBytes), 10, 64); err == nil {
					existingDate := time.Unix(timestamp, 0)

					// Check if the new retention date is earlier than the existing one
					if retention.RetainUntilDate != nil && retention.RetainUntilDate.Before(existingDate) {
						// Attempting to decrease retention period
						if string(existingMode) == s3_constants.RetentionModeCompliance {
							// Cannot decrease compliance mode retention without bypass
							return ErrComplianceModeActive
						}

						if string(existingMode) == s3_constants.RetentionModeGovernance && !bypassGovernance {
							// Cannot decrease governance mode retention without bypass
							return ErrGovernanceModeActive
						}
					}

					// If new retention date is later or same, allow the operation
					// This covers both increasing retention period and overriding with same/later date
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
	// NOTE: Potential race condition exists if concurrent calls to PutObjectRetention
	// and PutObjectLegalHold update the same object simultaneously, as they might
	// overwrite each other's Extended map changes. This is mitigated by the fact
	// that mkFile operations are typically serialized at the filer level, but
	// future implementations might consider using atomic update operations or
	// entry-level locking for complete safety.
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	return s3a.mkFile(bucketDir, entryPath, entry.Chunks, func(updatedEntry *filer_pb.Entry) {
		updatedEntry.Extended = entry.Extended
		updatedEntry.WormEnforcedAtTsNs = entry.WormEnforcedAtTsNs
	})
}

// ====================================================================
// LEGAL HOLD OPERATIONS
// ====================================================================

// getObjectLegalHold retrieves object legal hold configuration
func (s3a *S3ApiServer) getObjectLegalHold(bucket, object, versionId string) (*ObjectLegalHold, error) {
	entry, err := s3a.getObjectEntry(bucket, object, versionId)
	if err != nil {
		return nil, err
	}

	if entry.Extended == nil {
		return nil, ErrNoLegalHoldConfiguration
	}

	legalHold := &ObjectLegalHold{}

	if statusBytes, exists := entry.Extended[s3_constants.ExtLegalHoldKey]; exists {
		legalHold.Status = string(statusBytes)
	} else {
		return nil, ErrNoLegalHoldConfiguration
	}

	// Set namespace for S3 compatibility (matches MinIO behavior)
	legalHold.XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"
	return legalHold, nil
}

// setObjectLegalHold sets object legal hold configuration
func (s3a *S3ApiServer) setObjectLegalHold(bucket, object, versionId string, legalHold *ObjectLegalHold) error {
	var entry *filer_pb.Entry
	var err error
	var entryPath string

	if versionId != "" {
		entry, err = s3a.getSpecificObjectVersion(bucket, object, versionId)
		if err != nil {
			return fmt.Errorf("failed to get version %s for object %s/%s: %w", versionId, bucket, object, ErrVersionNotFound)
		}
		entryPath = object + ".versions/" + s3a.getVersionFileName(versionId)
	} else {
		// Check if versioning is enabled
		versioningEnabled, vErr := s3a.isVersioningEnabled(bucket)
		if vErr != nil {
			return fmt.Errorf("error checking versioning: %w", vErr)
		}

		if versioningEnabled {
			entry, err = s3a.getLatestObjectVersion(bucket, object)
			if err != nil {
				return fmt.Errorf("failed to get latest version for object %s/%s: %w", bucket, object, ErrLatestVersionNotFound)
			}
			// Extract version ID from entry metadata
			entryPath = object // default to regular object path
			if entry.Extended != nil {
				if versionIdBytes, exists := entry.Extended[s3_constants.ExtVersionIdKey]; exists {
					versionId = string(versionIdBytes)
					if versionId != "null" {
						entryPath = object + ".versions/" + s3a.getVersionFileName(versionId)
					}
				}
			}
		} else {
			entry, err = s3a.fetchObjectEntryRequired(bucket, object)
			if err != nil {
				return fmt.Errorf("failed to get object %s/%s: %w", bucket, object, ErrObjectNotFound)
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
	// NOTE: Potential race condition exists if concurrent calls to PutObjectRetention
	// and PutObjectLegalHold update the same object simultaneously, as they might
	// overwrite each other's Extended map changes. This is mitigated by the fact
	// that mkFile operations are typically serialized at the filer level, but
	// future implementations might consider using atomic update operations or
	// entry-level locking for complete safety.
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	return s3a.mkFile(bucketDir, entryPath, entry.Chunks, func(updatedEntry *filer_pb.Entry) {
		updatedEntry.Extended = entry.Extended
	})
}

// ====================================================================
// PROTECTION ENFORCEMENT
// ====================================================================

// isObjectRetentionActive checks if object has active retention
func (s3a *S3ApiServer) isObjectRetentionActive(bucket, object, versionId string) (bool, error) {
	retention, err := s3a.getObjectRetention(bucket, object, versionId)
	if err != nil {
		// If no retention found, object is not under retention
		if errors.Is(err, ErrNoRetentionConfiguration) {
			return false, nil
		}
		return false, err
	}

	if retention.RetainUntilDate != nil && retention.RetainUntilDate.After(time.Now()) {
		return true, nil
	}

	return false, nil
}

// getRetentionFromEntry extracts retention configuration from filer entry
func (s3a *S3ApiServer) getRetentionFromEntry(entry *filer_pb.Entry) (*ObjectRetention, bool, error) {
	if entry.Extended == nil {
		return nil, false, nil
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
			return nil, false, fmt.Errorf("failed to parse retention timestamp: corrupted timestamp data")
		}
	}

	if retention.Mode == "" || retention.RetainUntilDate == nil {
		return nil, false, nil
	}

	// Check if retention is currently active
	isActive := retention.RetainUntilDate.After(time.Now())
	return retention, isActive, nil
}

// getLegalHoldFromEntry extracts legal hold configuration from filer entry
func (s3a *S3ApiServer) getLegalHoldFromEntry(entry *filer_pb.Entry) (*ObjectLegalHold, bool, error) {
	if entry.Extended == nil {
		return nil, false, nil
	}

	legalHold := &ObjectLegalHold{}

	if statusBytes, exists := entry.Extended[s3_constants.ExtLegalHoldKey]; exists {
		legalHold.Status = string(statusBytes)
	} else {
		return nil, false, nil
	}

	isActive := legalHold.Status == s3_constants.LegalHoldOn
	return legalHold, isActive, nil
}

// ====================================================================
// GOVERNANCE BYPASS
// ====================================================================

// checkGovernanceBypassPermission checks if user has permission to bypass governance retention
func (s3a *S3ApiServer) checkGovernanceBypassPermission(request *http.Request, bucket, object string) bool {
	// Use the existing IAM auth system to check the specific permission
	// Create the governance bypass action with proper bucket/object concatenation
	// Note: path.Join would drop bucket if object has leading slash, so use explicit formatting
	resource := fmt.Sprintf("%s/%s", bucket, strings.TrimPrefix(object, "/"))
	action := Action(fmt.Sprintf("%s:%s", s3_constants.ACTION_BYPASS_GOVERNANCE_RETENTION, resource))

	// Use the IAM system to authenticate and authorize this specific action
	identity, errCode := s3a.iam.authRequest(request, action)
	if errCode != s3err.ErrNone {
		glog.V(3).Infof("IAM auth failed for governance bypass: %v", errCode)
		return false
	}

	// Verify that the authenticated identity can perform this action
	if identity != nil && identity.canDo(action, bucket, object) {
		return true
	}

	// Additional check: allow users with Admin action to bypass governance retention
	// Use the proper S3 Admin action constant instead of generic isAdmin() method
	adminAction := Action(fmt.Sprintf("%s:%s", s3_constants.ACTION_ADMIN, resource))
	if identity != nil && identity.canDo(adminAction, bucket, object) {
		glog.V(2).Infof("Admin user %s granted governance bypass permission for %s/%s", identity.Name, bucket, object)
		return true
	}

	return false
}

// evaluateGovernanceBypassRequest evaluates if governance bypass is requested and permitted
func (s3a *S3ApiServer) evaluateGovernanceBypassRequest(r *http.Request, bucket, object string) bool {
	// Step 1: Check if governance bypass was requested via header
	bypassRequested := r.Header.Get("x-amz-bypass-governance-retention") == "true"
	if !bypassRequested {
		// No bypass requested - normal retention enforcement applies
		return false
	}

	// Step 2: Validate user has permission to bypass governance retention
	hasPermission := s3a.checkGovernanceBypassPermission(r, bucket, object)
	if !hasPermission {
		glog.V(2).Infof("Governance bypass denied for %s/%s: user lacks s3:BypassGovernanceRetention permission", bucket, object)
		return false
	}

	glog.V(2).Infof("Governance bypass granted for %s/%s: header present and user has permission", bucket, object)
	return true
}

// enforceObjectLockProtections enforces object lock protections for operations
func (s3a *S3ApiServer) enforceObjectLockProtections(request *http.Request, bucket, object, versionId string, governanceBypassAllowed bool) error {
	// Get the object entry to check both retention and legal hold
	// For delete operations without versionId, we need to check the latest version
	var entry *filer_pb.Entry
	var err error

	if versionId != "" {
		// Check specific version
		entry, err = s3a.getObjectEntry(bucket, object, versionId)
	} else {
		// Check latest version for delete marker creation
		entry, err = s3a.getObjectEntry(bucket, object, "")
	}

	if err != nil {
		// If object doesn't exist, it's not under retention or legal hold - this is expected during delete operations
		if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrVersionNotFound) || errors.Is(err, ErrLatestVersionNotFound) {
			// Object doesn't exist, so it can't be under retention or legal hold - this is normal
			glog.V(4).Infof("Object %s/%s (versionId: %s) not found during object lock check (expected during delete operations)", bucket, object, versionId)
			return nil
		}
		glog.Warningf("Error retrieving object %s/%s (versionId: %s) for lock check: %v", bucket, object, versionId, err)
		return err
	}

	// Extract retention information from the entry
	retention, retentionActive, err := s3a.getRetentionFromEntry(entry)
	if err != nil {
		glog.Warningf("Error parsing retention for %s/%s (versionId: %s): %v", bucket, object, versionId, err)
		// Continue with legal hold check even if retention parsing fails
	}

	// Extract legal hold information from the entry
	_, legalHoldActive, err := s3a.getLegalHoldFromEntry(entry)
	if err != nil {
		glog.Warningf("Error parsing legal hold for %s/%s (versionId: %s): %v", bucket, object, versionId, err)
		// Continue with retention check even if legal hold parsing fails
	}

	// If object is under legal hold, it cannot be deleted or modified (including delete marker creation)
	if legalHoldActive {
		return ErrObjectUnderLegalHold
	}

	// If object is under retention, check the mode
	if retentionActive && retention != nil {
		if retention.Mode == s3_constants.RetentionModeCompliance {
			return ErrComplianceModeActive
		}

		if retention.Mode == s3_constants.RetentionModeGovernance {
			if !governanceBypassAllowed {
				return ErrGovernanceModeActive
			}
			// Note: governanceBypassAllowed parameter is already validated by evaluateGovernanceBypassRequest()
			// which checks both header presence and IAM permissions, so we trust it here
		}
	}

	return nil
}

// ====================================================================
// AVAILABILITY CHECKS
// ====================================================================

// isObjectLockAvailable checks if object lock is available for the bucket
func (s3a *S3ApiServer) isObjectLockAvailable(bucket string) error {
	versioningEnabled, err := s3a.isVersioningEnabled(bucket)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return ErrBucketNotFound
		}
		return fmt.Errorf("error checking versioning status: %w", err)
	}

	if !versioningEnabled {
		return fmt.Errorf("object lock requires versioning to be enabled")
	}

	return nil
}

// handleObjectLockAvailabilityCheck handles object lock availability checks for API endpoints
func (s3a *S3ApiServer) handleObjectLockAvailabilityCheck(w http.ResponseWriter, request *http.Request, bucket, handlerName string) bool {
	if err := s3a.isObjectLockAvailable(bucket); err != nil {
		glog.Errorf("%s: object lock not available for bucket %s: %v", handlerName, bucket, err)
		if errors.Is(err, ErrBucketNotFound) {
			s3err.WriteErrorResponse(w, request, s3err.ErrNoSuchBucket)
		} else {
			// Return InvalidRequest for object lock operations on buckets without object lock enabled
			// This matches AWS S3 behavior and s3-tests expectations (400 Bad Request)
			s3err.WriteErrorResponse(w, request, s3err.ErrInvalidRequest)
		}
		return false
	}
	return true
}
