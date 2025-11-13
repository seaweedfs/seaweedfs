package s3api

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"math"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/aws/aws-sdk-go/private/protocol/xml/xmlutil"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3bucket"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func (s3a *S3ApiServer) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {

	glog.V(3).Infof("ListBucketsHandler")

	var identity *Identity
	var s3Err s3err.ErrorCode
	if s3a.iam.isEnabled() {
		// Use authRequest instead of authUser for consistency with other endpoints
		// This ensures the same authentication flow and any fixes (like prefix handling) are applied
		identity, s3Err = s3a.iam.authRequest(r, s3_constants.ACTION_LIST)
		if s3Err != s3err.ErrNone {
			s3err.WriteErrorResponse(w, r, s3Err)
			return
		}
	}

	var response ListAllMyBucketsResult

	entries, _, err := s3a.list(s3a.option.BucketsPath, "", "", false, math.MaxInt32)

	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	identityId := r.Header.Get(s3_constants.AmzIdentityId)

	var listBuckets ListAllMyBucketsList
	for _, entry := range entries {
		if entry.IsDirectory {
			// Check permissions for each bucket
			if identity != nil {
				// For JWT-authenticated users, use IAM authorization
				sessionToken := r.Header.Get("X-SeaweedFS-Session-Token")
				if s3a.iam.iamIntegration != nil && sessionToken != "" {
					// Use IAM authorization for JWT users
					errCode := s3a.iam.authorizeWithIAM(r, identity, s3_constants.ACTION_LIST, entry.Name, "")
					if errCode != s3err.ErrNone {
						continue
					}
				} else {
					// Use legacy authorization for non-JWT users
					if !identity.canDo(s3_constants.ACTION_LIST, entry.Name, "") {
						continue
					}
				}
			}
			listBuckets.Bucket = append(listBuckets.Bucket, ListAllMyBucketsEntry{
				Name:         entry.Name,
				CreationDate: time.Unix(entry.Attributes.Crtime, 0).UTC(),
			})
		}
	}

	response = ListAllMyBucketsResult{
		Owner: CanonicalUser{
			ID:          identityId,
			DisplayName: identityId,
		},
		Buckets: listBuckets,
	}

	writeSuccessResponseXML(w, r, response)
}

func (s3a *S3ApiServer) PutBucketHandler(w http.ResponseWriter, r *http.Request) {

	// collect parameters
	bucket, _ := s3_constants.GetBucketAndObject(r)

	// validate the bucket name
	err := s3bucket.VerifyS3BucketName(bucket)
	if err != nil {
		glog.Errorf("put invalid bucket name: %v %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidBucketName)
		return
	}

	// Check if bucket already exists and handle ownership/settings
	currentIdentityId := r.Header.Get(s3_constants.AmzIdentityId)

	// Check collection existence first
	collectionExists := false
	if err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		if resp, err := client.CollectionList(context.Background(), &filer_pb.CollectionListRequest{
			IncludeEcVolumes:     true,
			IncludeNormalVolumes: true,
		}); err != nil {
			glog.Errorf("list collection: %v", err)
			return fmt.Errorf("list collections: %w", err)
		} else {
			for _, c := range resp.Collections {
				if s3a.getCollectionName(bucket) == c.Name {
					collectionExists = true
					break
				}
			}
		}
		return nil
	}); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Check bucket directory existence and get metadata
	if exist, err := s3a.exists(s3a.option.BucketsPath, bucket, true); err == nil && exist {
		// Bucket exists, check ownership and settings
		if entry, err := s3a.getEntry(s3a.option.BucketsPath, bucket); err == nil {
			// Get existing bucket owner
			var existingOwnerId string
			if entry.Extended != nil {
				if id, ok := entry.Extended[s3_constants.AmzIdentityId]; ok {
					existingOwnerId = string(id)
				}
			}

			// Check ownership
			if existingOwnerId != "" && existingOwnerId != currentIdentityId {
				// Different owner - always fail with BucketAlreadyExists
				glog.V(3).Infof("PutBucketHandler: bucket %s owned by %s, requested by %s", bucket, existingOwnerId, currentIdentityId)
				s3err.WriteErrorResponse(w, r, s3err.ErrBucketAlreadyExists)
				return
			}

			// Same owner or no owner set - check for conflicting settings
			objectLockRequested := strings.EqualFold(r.Header.Get(s3_constants.AmzBucketObjectLockEnabled), "true")

			// Get current bucket configuration
			bucketConfig, errCode := s3a.getBucketConfig(bucket)
			if errCode != s3err.ErrNone {
				glog.Errorf("PutBucketHandler: failed to get bucket config for %s: %v", bucket, errCode)
				// If we can't get config, assume no conflict and allow recreation
			} else {
				// Check for Object Lock conflict
				currentObjectLockEnabled := bucketConfig.ObjectLockConfig != nil &&
					bucketConfig.ObjectLockConfig.ObjectLockEnabled == s3_constants.ObjectLockEnabled

				if objectLockRequested != currentObjectLockEnabled {
					// Conflicting Object Lock settings - fail with BucketAlreadyExists
					glog.V(3).Infof("PutBucketHandler: bucket %s has conflicting Object Lock settings (requested: %v, current: %v)",
						bucket, objectLockRequested, currentObjectLockEnabled)
					s3err.WriteErrorResponse(w, r, s3err.ErrBucketAlreadyExists)
					return
				}
			}

			// Bucket already exists - always return BucketAlreadyExists per S3 specification
			// The S3 tests expect BucketAlreadyExists in all cases, not BucketAlreadyOwnedByYou
			glog.V(3).Infof("PutBucketHandler: bucket %s already exists", bucket)
			s3err.WriteErrorResponse(w, r, s3err.ErrBucketAlreadyExists)
			return
		}
	}

	// If collection exists but bucket directory doesn't, this is an inconsistent state
	if collectionExists {
		glog.Errorf("PutBucketHandler: collection exists but bucket directory missing for %s", bucket)
		s3err.WriteErrorResponse(w, r, s3err.ErrBucketAlreadyExists)
		return
	}

	fn := func(entry *filer_pb.Entry) {
		if identityId := r.Header.Get(s3_constants.AmzIdentityId); identityId != "" {
			if entry.Extended == nil {
				entry.Extended = make(map[string][]byte)
			}
			entry.Extended[s3_constants.AmzIdentityId] = []byte(identityId)
		}
	}

	// create the folder for bucket, but lazily create actual collection
	if err := s3a.mkdir(s3a.option.BucketsPath, bucket, fn); err != nil {
		glog.Errorf("PutBucketHandler mkdir: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Remove bucket from negative cache after successful creation
	if s3a.bucketConfigCache != nil {
		s3a.bucketConfigCache.RemoveNegativeCache(bucket)
	}

	// Check for x-amz-bucket-object-lock-enabled header (S3 standard compliance)
	if objectLockHeaderValue := r.Header.Get(s3_constants.AmzBucketObjectLockEnabled); strings.EqualFold(objectLockHeaderValue, "true") {
		glog.V(3).Infof("PutBucketHandler: enabling Object Lock and Versioning for bucket %s due to x-amz-bucket-object-lock-enabled header", bucket)

		// Atomically update the configuration of the specified bucket. See the updateBucketConfig
		// function definition for detailed documentation on parameters and behavior.
		errCode := s3a.updateBucketConfig(bucket, func(bucketConfig *BucketConfig) error {
			// Enable versioning (required for Object Lock)
			bucketConfig.Versioning = s3_constants.VersioningEnabled

			// Create basic Object Lock configuration (enabled without default retention)
			objectLockConfig := &ObjectLockConfiguration{
				ObjectLockEnabled: s3_constants.ObjectLockEnabled,
			}

			// Set the cached Object Lock configuration
			bucketConfig.ObjectLockConfig = objectLockConfig
			glog.V(3).Infof("PutBucketHandler: set ObjectLockConfig for bucket %s: %+v", bucket, objectLockConfig)

			return nil
		})

		if errCode != s3err.ErrNone {
			glog.Errorf("PutBucketHandler: failed to enable Object Lock for bucket %s: %v", bucket, errCode)
			s3err.WriteErrorResponse(w, r, errCode)
			return
		}
		glog.V(3).Infof("PutBucketHandler: enabled Object Lock and Versioning for bucket %s", bucket)
	}

	w.Header().Set("Location", "/"+bucket)
	writeSuccessResponseEmpty(w, r)
}

func (s3a *S3ApiServer) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {

	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("DeleteBucketHandler %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	// Check if bucket has object lock enabled
	bucketConfig, errCode := s3a.getBucketConfig(bucket)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// If object lock is enabled, check for objects with active locks
	if bucketConfig.ObjectLockConfig != nil {
		hasLockedObjects, checkErr := s3a.hasObjectsWithActiveLocks(bucket)
		if checkErr != nil {
			glog.Errorf("DeleteBucketHandler: failed to check for locked objects in bucket %s: %v", bucket, checkErr)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}
		if hasLockedObjects {
			glog.V(3).Infof("DeleteBucketHandler: bucket %s has objects with active object locks, cannot delete", bucket)
			s3err.WriteErrorResponse(w, r, s3err.ErrBucketNotEmpty)
			return
		}
	}

	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		if !s3a.option.AllowDeleteBucketNotEmpty {
			entries, _, err := s3a.list(s3a.option.BucketsPath+"/"+bucket, "", "", false, 2)
			if err != nil {
				return fmt.Errorf("failed to list bucket %s: %v", bucket, err)
			}
			for _, entry := range entries {
				// Allow bucket deletion if only special directories remain
				if entry.Name != s3_constants.MultipartUploadsFolder &&
					!strings.HasSuffix(entry.Name, s3_constants.VersionsFolder) {
					return errors.New(s3err.GetAPIError(s3err.ErrBucketNotEmpty).Code)
				}
			}
		}

		// delete collection
		deleteCollectionRequest := &filer_pb.DeleteCollectionRequest{
			Collection: s3a.getCollectionName(bucket),
		}

		glog.V(1).Infof("delete collection: %v", deleteCollectionRequest)
		if _, err := client.DeleteCollection(context.Background(), deleteCollectionRequest); err != nil {
			return fmt.Errorf("delete collection %s: %v", bucket, err)
		}

		return nil
	})

	if err != nil {
		s3ErrorCode := s3err.ErrInternalError
		if err.Error() == s3err.GetAPIError(s3err.ErrBucketNotEmpty).Code {
			s3ErrorCode = s3err.ErrBucketNotEmpty
		}
		s3err.WriteErrorResponse(w, r, s3ErrorCode)
		return
	}

	err = s3a.rm(s3a.option.BucketsPath, bucket, false, true)

	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Clean up bucket-related caches and locks after successful deletion
	s3a.invalidateBucketConfigCache(bucket)

	s3err.WriteEmptyResponse(w, r, http.StatusNoContent)
}

// hasObjectsWithActiveLocks checks if any objects in the bucket have active retention or legal hold
func (s3a *S3ApiServer) hasObjectsWithActiveLocks(bucket string) (bool, error) {
	bucketPath := s3a.option.BucketsPath + "/" + bucket

	// Check all objects including versions for active locks
	// Establish current time once at the start for consistency across the entire scan
	hasLocks := false
	currentTime := time.Now()
	err := s3a.recursivelyCheckLocks(bucketPath, "", &hasLocks, currentTime)
	if err != nil {
		return false, fmt.Errorf("error checking for locked objects: %w", err)
	}

	return hasLocks, nil
}

const (
	// lockCheckPaginationSize is the page size for listing directories during lock checks
	lockCheckPaginationSize = 10000
)

// errStopPagination is a sentinel error to signal early termination of pagination
var errStopPagination = errors.New("stop pagination")

// paginateEntries iterates through directory entries with pagination
// Calls fn for each page of entries. If fn returns errStopPagination, iteration stops successfully.
func (s3a *S3ApiServer) paginateEntries(dir string, fn func(entries []*filer_pb.Entry) error) error {
	startFrom := ""
	for {
		entries, isLast, err := s3a.list(dir, "", startFrom, false, lockCheckPaginationSize)
		if err != nil {
			// Fail-safe: propagate error to prevent incorrect bucket deletion
			return fmt.Errorf("failed to list directory %s: %w", dir, err)
		}

		if err := fn(entries); err != nil {
			if errors.Is(err, errStopPagination) {
				return nil
			}
			return err
		}

		if isLast || len(entries) == 0 {
			break
		}
		// Use the last entry name as the start point for next page
		startFrom = entries[len(entries)-1].Name
	}
	return nil
}

// recursivelyCheckLocks recursively checks all objects and versions for active locks
// Uses pagination to handle directories with more than 10,000 entries
func (s3a *S3ApiServer) recursivelyCheckLocks(dir string, relativePath string, hasLocks *bool, currentTime time.Time) error {
	if *hasLocks {
		// Early exit if we've already found a locked object
		return nil
	}

	// Process entries in the current directory with pagination
	err := s3a.paginateEntries(dir, func(entries []*filer_pb.Entry) error {
		for _, entry := range entries {
			if *hasLocks {
				// Early exit if we've already found a locked object
				return errStopPagination
			}

			// Skip special directories (multipart uploads, etc)
			if entry.Name == s3_constants.MultipartUploadsFolder {
				continue
			}

			if entry.IsDirectory {
				subDir := path.Join(dir, entry.Name)
				if strings.HasSuffix(entry.Name, s3_constants.VersionsFolder) {
					// If it's a .versions directory, check all version files with pagination
					err := s3a.paginateEntries(subDir, func(versionEntries []*filer_pb.Entry) error {
						for _, versionEntry := range versionEntries {
							if s3a.entryHasActiveLock(versionEntry, currentTime) {
								*hasLocks = true
								glog.V(2).Infof("Found object with active lock in versions: %s/%s", subDir, versionEntry.Name)
								return errStopPagination
							}
						}
						return nil
					})
					if err != nil {
						return err
					}
				} else {
					// Recursively check other subdirectories
					subRelativePath := path.Join(relativePath, entry.Name)
					if err := s3a.recursivelyCheckLocks(subDir, subRelativePath, hasLocks, currentTime); err != nil {
						return err
					}
					// Early exit if a locked object was found in the subdirectory
					if *hasLocks {
						return errStopPagination
					}
				}
			} else {
				// Check regular files for locks
				if s3a.entryHasActiveLock(entry, currentTime) {
					*hasLocks = true
					objectPath := path.Join(relativePath, entry.Name)
					glog.V(2).Infof("Found object with active lock: %s", objectPath)
					return errStopPagination
				}
			}
		}
		return nil
	})

	return err
}

// entryHasActiveLock checks if an entry has an active retention or legal hold
func (s3a *S3ApiServer) entryHasActiveLock(entry *filer_pb.Entry, currentTime time.Time) bool {
	if entry.Extended == nil {
		return false
	}

	// Check for active legal hold
	if legalHoldBytes, exists := entry.Extended[s3_constants.ExtLegalHoldKey]; exists {
		if string(legalHoldBytes) == s3_constants.LegalHoldOn {
			return true
		}
	}

	// Check for active retention
	if modeBytes, exists := entry.Extended[s3_constants.ExtObjectLockModeKey]; exists {
		mode := string(modeBytes)
		if mode == s3_constants.RetentionModeCompliance || mode == s3_constants.RetentionModeGovernance {
			// Check if retention is still active
			if dateBytes, dateExists := entry.Extended[s3_constants.ExtRetentionUntilDateKey]; dateExists {
				timestamp, err := strconv.ParseInt(string(dateBytes), 10, 64)
				if err != nil {
					// Fail-safe: if we can't parse the retention date, assume the object is locked
					// to prevent accidental data loss
					glog.Warningf("Failed to parse retention date '%s' for entry, assuming locked: %v", string(dateBytes), err)
					return true
				}
				retainUntil := time.Unix(timestamp, 0)
				if retainUntil.After(currentTime) {
					return true
				}
			}
		}
	}

	return false
}

func (s3a *S3ApiServer) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {

	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("HeadBucketHandler %s", bucket)

	if entry, err := s3a.getEntry(s3a.option.BucketsPath, bucket); entry == nil || errors.Is(err, filer_pb.ErrNotFound) {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		return
	}

	writeSuccessResponseEmpty(w, r)
}

func (s3a *S3ApiServer) checkBucket(r *http.Request, bucket string) s3err.ErrorCode {
	// Use cached bucket config instead of direct getEntry call (optimization)
	config, errCode := s3a.getBucketConfig(bucket)
	if errCode != s3err.ErrNone {
		return errCode
	}

	//if iam is enabled, the access was already checked before
	if s3a.iam.isEnabled() {
		return s3err.ErrNone
	}
	if !s3a.hasAccess(r, config.Entry) {
		return s3err.ErrAccessDenied
	}
	return s3err.ErrNone
}

func (s3a *S3ApiServer) hasAccess(r *http.Request, entry *filer_pb.Entry) bool {
	// Check if user is properly authenticated as admin through IAM system
	if s3a.isUserAdmin(r) {
		return true
	}

	if entry.Extended == nil {
		return true
	}

	identityId := r.Header.Get(s3_constants.AmzIdentityId)
	if id, ok := entry.Extended[s3_constants.AmzIdentityId]; ok {
		if identityId != string(id) {
			glog.V(3).Infof("hasAccess: %s != %s (entry.Extended = %v)", identityId, id, entry.Extended)
			return false
		}
	}
	return true
}

// isUserAdmin securely checks if the authenticated user is an admin
// This validates admin status through proper IAM authentication, not spoofable headers
func (s3a *S3ApiServer) isUserAdmin(r *http.Request) bool {
	// Use a minimal admin action to authenticate and check admin status
	adminAction := Action("Admin")
	identity, errCode := s3a.iam.authRequest(r, adminAction)
	if errCode != s3err.ErrNone {
		return false
	}

	// Check if the authenticated identity has admin privileges
	return identity != nil && identity.isAdmin()
}

// isBucketPublicRead checks if a bucket allows anonymous read access based on its cached ACL status
func (s3a *S3ApiServer) isBucketPublicRead(bucket string) bool {
	// Get bucket configuration which contains cached public-read status
	config, errCode := s3a.getBucketConfig(bucket)
	if errCode != s3err.ErrNone {
		glog.V(4).Infof("isBucketPublicRead: failed to get bucket config for %s: %v", bucket, errCode)
		return false
	}

	glog.V(4).Infof("isBucketPublicRead: bucket=%s, IsPublicRead=%v", bucket, config.IsPublicRead)
	// Return the cached public-read status (no JSON parsing needed)
	return config.IsPublicRead
}

// isPublicReadGrants checks if the grants allow public read access
func isPublicReadGrants(grants []*s3.Grant) bool {
	for _, grant := range grants {
		if grant.Grantee != nil && grant.Grantee.URI != nil && grant.Permission != nil {
			// Check for AllUsers group with Read permission
			if *grant.Grantee.URI == s3_constants.GranteeGroupAllUsers &&
				(*grant.Permission == s3_constants.PermissionRead || *grant.Permission == s3_constants.PermissionFullControl) {
				return true
			}
		}
	}
	return false
}

// buildResourceARN builds a resource ARN from bucket and object
// Used by the policy engine wrapper
func buildResourceARN(bucket, object string) string {
	if object == "" || object == "/" {
		return fmt.Sprintf("arn:aws:s3:::%s", bucket)
	}
	// Remove leading slash if present
	object = strings.TrimPrefix(object, "/")
	return fmt.Sprintf("arn:aws:s3:::%s/%s", bucket, object)
}

// AuthWithPublicRead creates an auth wrapper that allows anonymous access for public-read buckets
func (s3a *S3ApiServer) AuthWithPublicRead(handler http.HandlerFunc, action Action) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bucket, object := s3_constants.GetBucketAndObject(r)
		authType := getRequestAuthType(r)
		isAnonymous := authType == authTypeAnonymous

		glog.V(4).Infof("AuthWithPublicRead: bucket=%s, object=%s, authType=%v, isAnonymous=%v", bucket, object, authType, isAnonymous)

		// For anonymous requests, check if bucket allows public read via ACLs or bucket policies
		if isAnonymous {
			// First check ACL-based public access
			isPublic := s3a.isBucketPublicRead(bucket)
			glog.V(4).Infof("AuthWithPublicRead: bucket=%s, isPublicACL=%v", bucket, isPublic)
			if isPublic {
				glog.V(3).Infof("AuthWithPublicRead: allowing anonymous access to public-read bucket %s (ACL)", bucket)
				handler(w, r)
				return
			}

			// Check bucket policy for anonymous access using the policy engine
			principal := "*" // Anonymous principal
			allowed, evaluated, err := s3a.policyEngine.EvaluatePolicy(bucket, object, string(action), principal)
			if err != nil {
				// SECURITY: Fail-close on policy evaluation errors
				// If we can't evaluate the policy, deny access rather than falling through to IAM
				glog.Errorf("AuthWithPublicRead: error evaluating bucket policy for %s/%s: %v - denying access", bucket, object, err)
				s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
				return
			} else if evaluated && allowed {
				glog.V(3).Infof("AuthWithPublicRead: allowing anonymous access to bucket %s (bucket policy)", bucket)
				handler(w, r)
				return
			}
			glog.V(3).Infof("AuthWithPublicRead: bucket %s does not allow public access, falling back to IAM auth", bucket)
		}

		// For all authenticated requests and anonymous requests to non-public buckets,
		// use normal IAM auth to enforce policies
		s3a.iam.Auth(handler, action)(w, r)
	}
}

// GetBucketAclHandler Get Bucket ACL
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAcl.html
func (s3a *S3ApiServer) GetBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	// collect parameters
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetBucketAclHandler %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	amzAccountId := r.Header.Get(s3_constants.AmzAccountId)
	amzDisplayName := s3a.iam.GetAccountNameById(amzAccountId)
	response := AccessControlPolicy{
		Owner: CanonicalUser{
			ID:          amzAccountId,
			DisplayName: amzDisplayName,
		},
	}
	response.AccessControlList.Grant = append(response.AccessControlList.Grant, Grant{
		Grantee: Grantee{
			ID:          amzAccountId,
			DisplayName: amzDisplayName,
			Type:        "CanonicalUser",
			XMLXSI:      "CanonicalUser",
			XMLNS:       "http://www.w3.org/2001/XMLSchema-instance"},
		Permission: s3.PermissionFullControl,
	})
	writeSuccessResponseXML(w, r, response)
}

// PutBucketAclHandler Put bucket ACL
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html //
func (s3a *S3ApiServer) PutBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	// collect parameters
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutBucketAclHandler %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	// Get account information for ACL processing
	amzAccountId := r.Header.Get(s3_constants.AmzAccountId)

	// Get bucket ownership settings (these would be used for ownership validation in a full implementation)
	bucketOwnership := ""         // Default/simplified for now - in a full implementation this would be retrieved from bucket config
	bucketOwnerId := amzAccountId // Simplified - bucket owner is current account

	// Use the existing ACL parsing logic to handle both canned ACLs and XML body
	grants, errCode := ExtractAcl(r, s3a.iam, bucketOwnership, bucketOwnerId, amzAccountId, amzAccountId)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	glog.V(3).Infof("PutBucketAclHandler: bucket=%s, extracted %d grants", bucket, len(grants))
	isPublic := isPublicReadGrants(grants)
	glog.V(3).Infof("PutBucketAclHandler: bucket=%s, isPublicReadGrants=%v", bucket, isPublic)

	// Store the bucket ACL in bucket metadata
	errCode = s3a.updateBucketConfig(bucket, func(config *BucketConfig) error {
		if len(grants) > 0 {
			grantsBytes, err := json.Marshal(grants)
			if err != nil {
				glog.Errorf("PutBucketAclHandler: failed to marshal grants: %v", err)
				return err
			}
			config.ACL = grantsBytes
			// Cache the public-read status to avoid JSON parsing on every request
			config.IsPublicRead = isPublicReadGrants(grants)
			glog.V(4).Infof("PutBucketAclHandler: bucket=%s, setting IsPublicRead=%v", bucket, config.IsPublicRead)
		} else {
			config.ACL = nil
			config.IsPublicRead = false
		}
		config.Owner = amzAccountId
		return nil
	})

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	glog.V(3).Infof("PutBucketAclHandler: Successfully stored ACL for bucket %s with %d grants", bucket, len(grants))

	// Small delay to ensure ACL propagation across distributed caches
	// This prevents race conditions in tests where anonymous access is attempted immediately after ACL change
	time.Sleep(50 * time.Millisecond)

	writeSuccessResponseEmpty(w, r)
}

// GetBucketLifecycleConfigurationHandler Get Bucket Lifecycle configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html
func (s3a *S3ApiServer) GetBucketLifecycleConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	// collect parameters
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetBucketLifecycleConfigurationHandler %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}
	fc, err := filer.ReadFilerConf(s3a.option.Filer, s3a.option.GrpcDialOption, nil)
	if err != nil {
		glog.Errorf("GetBucketLifecycleConfigurationHandler: %s", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	ttls := fc.GetCollectionTtls(s3a.getCollectionName(bucket))
	if len(ttls) == 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchLifecycleConfiguration)
		return
	}

	response := Lifecycle{}
	// Sort locationPrefixes to ensure consistent ordering of lifecycle rules
	var locationPrefixes []string
	for locationPrefix := range ttls {
		locationPrefixes = append(locationPrefixes, locationPrefix)
	}
	sort.Strings(locationPrefixes)

	for _, locationPrefix := range locationPrefixes {
		internalTtl := ttls[locationPrefix]
		ttl, _ := needle.ReadTTL(internalTtl)
		days := int(ttl.Minutes() / 60 / 24)
		if days == 0 {
			continue
		}
		prefix, found := strings.CutPrefix(locationPrefix, fmt.Sprintf("%s/%s/", s3a.option.BucketsPath, bucket))
		if !found {
			continue
		}
		response.Rules = append(response.Rules, Rule{
			ID:         prefix,
			Status:     Enabled,
			Prefix:     Prefix{val: prefix, set: true},
			Expiration: Expiration{Days: days, set: true},
		})
	}

	writeSuccessResponseXML(w, r, response)
}

// PutBucketLifecycleConfigurationHandler Put Bucket Lifecycle configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html
func (s3a *S3ApiServer) PutBucketLifecycleConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	// collect parameters
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutBucketLifecycleConfigurationHandler %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	lifeCycleConfig := Lifecycle{}
	if err := xmlDecoder(r.Body, &lifeCycleConfig, r.ContentLength); err != nil {
		glog.Warningf("PutBucketLifecycleConfigurationHandler xml decode: %s", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	fc, err := filer.ReadFilerConf(s3a.option.Filer, s3a.option.GrpcDialOption, nil)
	if err != nil {
		glog.Errorf("PutBucketLifecycleConfigurationHandler read filer config: %s", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	collectionName := s3a.getCollectionName(bucket)
	collectionTtls := fc.GetCollectionTtls(collectionName)
	changed := false

	for _, rule := range lifeCycleConfig.Rules {
		if rule.Status != Enabled {
			continue
		}
		var rulePrefix string
		switch {
		case rule.Filter.Prefix.set:
			rulePrefix = rule.Filter.Prefix.val
		case rule.Prefix.set:
			rulePrefix = rule.Prefix.val
		case !rule.Expiration.Date.IsZero() || rule.Transition.Days > 0 || !rule.Transition.Date.IsZero():
			s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
			return
		}

		if rule.Expiration.Days == 0 {
			continue
		}
		locationPrefix := fmt.Sprintf("%s/%s/%s", s3a.option.BucketsPath, bucket, rulePrefix)
		locConf := &filer_pb.FilerConf_PathConf{
			LocationPrefix: locationPrefix,
			Collection:     collectionName,
			Ttl:            fmt.Sprintf("%dd", rule.Expiration.Days),
		}
		if ttl, ok := collectionTtls[locConf.LocationPrefix]; ok && ttl == locConf.Ttl {
			continue
		}
		if err := fc.AddLocationConf(locConf); err != nil {
			glog.Errorf("PutBucketLifecycleConfigurationHandler add location config: %s", err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}
		ttlSec := int32((time.Duration(rule.Expiration.Days) * util.LifeCycleInterval).Seconds())
		glog.V(2).Infof("Start updating TTL for %s", locationPrefix)
		if updErr := s3a.updateEntriesTTL(locationPrefix, ttlSec); updErr != nil {
			glog.Errorf("PutBucketLifecycleConfigurationHandler update TTL for %s: %s", locationPrefix, updErr)
		} else {
			glog.V(2).Infof("Finished updating TTL for %s", locationPrefix)
		}
		changed = true
	}

	if changed {
		var buf bytes.Buffer
		if err := fc.ToText(&buf); err != nil {
			glog.Errorf("PutBucketLifecycleConfigurationHandler save config to text: %s", err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		}
		if err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			return filer.SaveInsideFiler(client, filer.DirectoryEtcSeaweedFS, filer.FilerConfName, buf.Bytes())
		}); err != nil {
			glog.Errorf("PutBucketLifecycleConfigurationHandler save config inside filer: %s", err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}
	}

	writeSuccessResponseEmpty(w, r)
}

// DeleteBucketLifecycleHandler Delete Bucket Lifecycle
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketLifecycle.html
func (s3a *S3ApiServer) DeleteBucketLifecycleHandler(w http.ResponseWriter, r *http.Request) {
	// collect parameters
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("DeleteBucketLifecycleHandler %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	fc, err := filer.ReadFilerConf(s3a.option.Filer, s3a.option.GrpcDialOption, nil)
	if err != nil {
		glog.Errorf("DeleteBucketLifecycleHandler read filer config: %s", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	collectionTtls := fc.GetCollectionTtls(s3a.getCollectionName(bucket))
	changed := false
	for prefix, ttl := range collectionTtls {
		bucketPrefix := fmt.Sprintf("%s/%s/", s3a.option.BucketsPath, bucket)
		if strings.HasPrefix(prefix, bucketPrefix) && strings.HasSuffix(ttl, "d") {
			pathConf, found := fc.GetLocationConf(prefix)
			if found {
				pathConf.Ttl = ""
				fc.SetLocationConf(pathConf)
			}
			changed = true
		}
	}

	if changed {
		var buf bytes.Buffer
		if err := fc.ToText(&buf); err != nil {
			glog.Errorf("DeleteBucketLifecycleHandler save config to text: %s", err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		}
		if err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			return filer.SaveInsideFiler(client, filer.DirectoryEtcSeaweedFS, filer.FilerConfName, buf.Bytes())
		}); err != nil {
			glog.Errorf("DeleteBucketLifecycleHandler save config inside filer: %s", err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}
	}

	s3err.WriteEmptyResponse(w, r, http.StatusNoContent)
}

// GetBucketLocationHandler Get bucket location
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
func (s3a *S3ApiServer) GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	writeSuccessResponseXML(w, r, CreateBucketConfiguration{})
}

// GetBucketRequestPaymentHandler Get bucket location
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketRequestPayment.html
func (s3a *S3ApiServer) GetBucketRequestPaymentHandler(w http.ResponseWriter, r *http.Request) {
	writeSuccessResponseXML(w, r, RequestPaymentConfiguration{Payer: "BucketOwner"})
}

// PutBucketOwnershipControls https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketOwnershipControls.html
func (s3a *S3ApiServer) PutBucketOwnershipControls(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutBucketOwnershipControls %s", bucket)

	errCode := s3a.checkAccessByOwnership(r, bucket)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	if r.Body == nil || r.Body == http.NoBody {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	var v s3.OwnershipControls
	defer util_http.CloseRequest(r)

	err := xmlutil.UnmarshalXML(&v, xml.NewDecoder(r.Body), "")
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	if len(v.Rules) != 1 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	printOwnership := true
	ownership := *v.Rules[0].ObjectOwnership
	switch ownership {
	case s3_constants.OwnershipObjectWriter:
	case s3_constants.OwnershipBucketOwnerPreferred:
	case s3_constants.OwnershipBucketOwnerEnforced:
		printOwnership = false
	default:
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	// Check if ownership needs to be updated
	currentOwnership, errCode := s3a.getBucketOwnership(bucket)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	if currentOwnership != ownership {
		errCode = s3a.setBucketOwnership(bucket, ownership)
		if errCode != s3err.ErrNone {
			s3err.WriteErrorResponse(w, r, errCode)
			return
		}
	}

	if printOwnership {
		result := &s3.PutBucketOwnershipControlsInput{
			OwnershipControls: &v,
		}
		s3err.WriteAwsXMLResponse(w, r, http.StatusOK, result)
	} else {
		writeSuccessResponseEmpty(w, r)
	}
}

// GetBucketOwnershipControls https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketOwnershipControls.html
func (s3a *S3ApiServer) GetBucketOwnershipControls(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetBucketOwnershipControls %s", bucket)

	errCode := s3a.checkAccessByOwnership(r, bucket)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// Get ownership using new bucket config system
	ownership, errCode := s3a.getBucketOwnership(bucket)
	if errCode == s3err.ErrNoSuchBucket {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
		return
	} else if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, s3err.OwnershipControlsNotFoundError)
		return
	}

	result := &s3.PutBucketOwnershipControlsInput{
		OwnershipControls: &s3.OwnershipControls{
			Rules: []*s3.OwnershipControlsRule{
				{
					ObjectOwnership: &ownership,
				},
			},
		},
	}

	s3err.WriteAwsXMLResponse(w, r, http.StatusOK, result)
}

// DeleteBucketOwnershipControls https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketOwnershipControls.html
func (s3a *S3ApiServer) DeleteBucketOwnershipControls(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutBucketOwnershipControls %s", bucket)

	errCode := s3a.checkAccessByOwnership(r, bucket)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	bucketEntry, err := s3a.getEntry(s3a.option.BucketsPath, bucket)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	_, ok := bucketEntry.Extended[s3_constants.ExtOwnershipKey]
	if !ok {
		s3err.WriteErrorResponse(w, r, s3err.OwnershipControlsNotFoundError)
		return
	}

	delete(bucketEntry.Extended, s3_constants.ExtOwnershipKey)
	err = s3a.updateEntry(s3a.option.BucketsPath, bucketEntry)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	emptyOwnershipControls := &s3.OwnershipControls{
		Rules: []*s3.OwnershipControlsRule{},
	}
	s3err.WriteAwsXMLResponse(w, r, http.StatusOK, emptyOwnershipControls)
}

// GetBucketVersioningHandler Get Bucket Versioning status
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html
func (s3a *S3ApiServer) GetBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetBucketVersioning %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	// Get versioning status using new bucket config system
	versioningStatus, errCode := s3a.getBucketVersioningStatus(bucket)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// AWS S3 behavior: If versioning was never configured, don't return Status field
	var response *s3.PutBucketVersioningInput
	if versioningStatus == "" {
		// No versioning configuration - return empty response (no Status field)
		response = &s3.PutBucketVersioningInput{
			VersioningConfiguration: &s3.VersioningConfiguration{},
		}
	} else {
		// Versioning was explicitly configured - return the status
		response = &s3.PutBucketVersioningInput{
			VersioningConfiguration: &s3.VersioningConfiguration{
				Status: aws.String(versioningStatus),
			},
		}
	}
	s3err.WriteAwsXMLResponse(w, r, http.StatusOK, response)
}

// PutBucketVersioningHandler Put bucket Versioning
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html
func (s3a *S3ApiServer) PutBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutBucketVersioning %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	if r.Body == nil || r.Body == http.NoBody {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	var versioningConfig s3.VersioningConfiguration
	defer util_http.CloseRequest(r)

	err := xmlutil.UnmarshalXML(&versioningConfig, xml.NewDecoder(r.Body), "")
	if err != nil {
		glog.Warningf("PutBucketVersioningHandler xml decode: %s", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	if versioningConfig.Status == nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	status := *versioningConfig.Status
	if status != s3_constants.VersioningEnabled && status != s3_constants.VersioningSuspended {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}

	// Check if trying to suspend versioning on a bucket with object lock enabled
	if status == s3_constants.VersioningSuspended {
		// Get bucket configuration to check for object lock
		bucketConfig, errCode := s3a.getBucketConfig(bucket)
		if errCode == s3err.ErrNone && bucketConfig.ObjectLockConfig != nil {
			// Object lock is enabled, cannot suspend versioning
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidBucketState)
			return
		}
	}

	// Update bucket versioning configuration using new bucket config system
	if errCode := s3a.setBucketVersioningStatus(bucket, status); errCode != s3err.ErrNone {
		glog.Errorf("PutBucketVersioningHandler save config: %d", errCode)
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	writeSuccessResponseEmpty(w, r)
}
