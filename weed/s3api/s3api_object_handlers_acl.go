package s3api

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// GetObjectAclHandler Get object ACL
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html
func (s3a *S3ApiServer) GetObjectAclHandler(w http.ResponseWriter, r *http.Request) {
	// collect parameters
	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetObjectAclHandler %s %s", bucket, object)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	// Check if object exists and get its metadata
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	entry, err := s3a.getEntry(bucketDir, object)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
			return
		}
		glog.Errorf("GetObjectAclHandler: error checking object %s/%s: %v", bucket, object, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if entry == nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
		return
	}

	// Get object owner from metadata, fallback to request account
	var objectOwner string
	var objectOwnerDisplayName string
	amzAccountId := r.Header.Get(s3_constants.AmzAccountId)

	if entry.Extended != nil {
		if ownerBytes, exists := entry.Extended[s3_constants.ExtAmzOwnerKey]; exists {
			objectOwner = string(ownerBytes)
		}
	}

	// Fallback to current account if no owner stored
	if objectOwner == "" {
		objectOwner = amzAccountId
	}

	objectOwnerDisplayName = s3a.iam.GetAccountNameById(objectOwner)

	// Build ACL response
	response := AccessControlPolicy{
		Owner: CanonicalUser{
			ID:          objectOwner,
			DisplayName: objectOwnerDisplayName,
		},
	}

	// Get grants from stored ACL metadata
	grants := GetAcpGrants(entry.Extended)
	if len(grants) > 0 {
		// Convert AWS SDK grants to local Grant format
		for _, grant := range grants {
			localGrant := Grant{
				Permission: Permission(*grant.Permission),
			}

			if grant.Grantee != nil {
				localGrant.Grantee = Grantee{
					Type:   *grant.Grantee.Type,
					XMLXSI: "CanonicalUser",
					XMLNS:  "http://www.w3.org/2001/XMLSchema-instance",
				}

				if grant.Grantee.ID != nil {
					localGrant.Grantee.ID = *grant.Grantee.ID
					localGrant.Grantee.DisplayName = s3a.iam.GetAccountNameById(*grant.Grantee.ID)
				}

				if grant.Grantee.URI != nil {
					localGrant.Grantee.URI = *grant.Grantee.URI
				}
			}

			response.AccessControlList.Grant = append(response.AccessControlList.Grant, localGrant)
		}
	} else {
		// Fallback to default full control for object owner
		response.AccessControlList.Grant = append(response.AccessControlList.Grant, Grant{
			Grantee: Grantee{
				ID:          objectOwner,
				DisplayName: objectOwnerDisplayName,
				Type:        "CanonicalUser",
				XMLXSI:      "CanonicalUser",
				XMLNS:       "http://www.w3.org/2001/XMLSchema-instance"},
			Permission: Permission(s3_constants.PermissionFullControl),
		})
	}

	writeSuccessResponseXML(w, r, response)
}

// PutObjectAclHandler Put object ACL
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html
func (s3a *S3ApiServer) PutObjectAclHandler(w http.ResponseWriter, r *http.Request) {
	// collect parameters
	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("PutObjectAclHandler %s %s", bucket, object)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	// Check if object exists and get its metadata
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	entry, err := s3a.getEntry(bucketDir, object)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
			return
		}
		glog.Errorf("PutObjectAclHandler: error checking object %s/%s: %v", bucket, object, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}
	if entry == nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
		return
	}

	// Get current object owner from metadata
	var objectOwner string
	amzAccountId := r.Header.Get(s3_constants.AmzAccountId)

	if entry.Extended != nil {
		if ownerBytes, exists := entry.Extended[s3_constants.ExtAmzOwnerKey]; exists {
			objectOwner = string(ownerBytes)
		}
	}

	// Fallback to current account if no owner stored
	if objectOwner == "" {
		objectOwner = amzAccountId
	}

	// **PERMISSION CHECKS**

	// 1. Check if user is admin (admins can modify any ACL)
	if !s3a.isUserAdmin(r) {
		// 2. Check object ownership - only object owner can modify ACL (unless admin)
		if objectOwner != amzAccountId {
			glog.V(3).Infof("PutObjectAclHandler: Access denied - user %s is not owner of object %s/%s (owner: %s)",
				amzAccountId, bucket, object, objectOwner)
			s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
			return
		}

		// 3. Check object-level WRITE_ACP permission
		// Create the specific action for this object
		writeAcpAction := Action(fmt.Sprintf("WriteAcp:%s/%s", bucket, object))
		identity, errCode := s3a.iam.authRequest(r, writeAcpAction)
		if errCode != s3err.ErrNone {
			glog.V(3).Infof("PutObjectAclHandler: Auth failed for WriteAcp action on %s/%s: %v", bucket, object, errCode)
			s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
			return
		}

		// 4. Verify the authenticated identity can perform WriteAcp on this specific object
		if identity == nil || !identity.canDo(writeAcpAction, bucket, object) {
			glog.V(3).Infof("PutObjectAclHandler: Identity %v cannot perform WriteAcp on %s/%s", identity, bucket, object)
			s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
			return
		}
	} else {
		glog.V(3).Infof("PutObjectAclHandler: Admin user %s granted ACL modification permission for %s/%s", amzAccountId, bucket, object)
	}

	// Get bucket config for ownership settings
	bucketConfig, errCode := s3a.getBucketConfig(bucket)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	bucketOwnership := bucketConfig.Ownership
	bucketOwnerId := bucketConfig.Owner

	// Extract ACL from request (either canned ACL or XML body)
	// This function also validates that the owner in the request matches the object owner
	grants, errCode := ExtractAcl(r, s3a.iam, bucketOwnership, bucketOwnerId, objectOwner, amzAccountId)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// Store ACL in object metadata
	if errCode := AssembleEntryWithAcp(entry, objectOwner, grants); errCode != s3err.ErrNone {
		glog.Errorf("PutObjectAclHandler: failed to assemble entry with ACP: %v", errCode)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Update the object with new ACL metadata
	err = s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.UpdateEntryRequest{
			Directory: bucketDir,
			Entry:     entry,
		}

		if _, err := client.UpdateEntry(context.Background(), request); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		glog.Errorf("PutObjectAclHandler: failed to update entry: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	glog.V(3).Infof("PutObjectAclHandler: Successfully updated ACL for %s/%s by user %s", bucket, object, amzAccountId)
	writeSuccessResponseEmpty(w, r)
}
