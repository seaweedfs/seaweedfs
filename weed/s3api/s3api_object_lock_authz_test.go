package s3api

import (
	"testing"

	iamlib "github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestPutObjectLockConfigurationUsesDedicatedAction(t *testing.T) {
	bindings := handlerActionBindings(t)
	if got := bindings["PutObjectLockConfigurationHandler"]; got != "ACTION_PUT_BUCKET_OBJECT_LOCK_CONFIG" {
		t.Fatalf("PutObjectLockConfigurationHandler is gated on %s, want ACTION_PUT_BUCKET_OBJECT_LOCK_CONFIG", got)
	}
}

func TestObjectWriteDoesNotAllowPutObjectLockConfiguration(t *testing.T) {
	objectWriter := &Identity{
		Name:    "object-writer",
		Actions: []Action{Action(s3_constants.ACTION_WRITE + ":test-bucket")},
	}

	if !objectWriter.CanDo(s3_constants.ACTION_WRITE, "test-bucket", "some/key") {
		t.Fatal("precondition failed: the identity should be able to write objects")
	}
	if objectWriter.CanDo(s3_constants.ACTION_PUT_BUCKET_OBJECT_LOCK_CONFIG, "test-bucket", "") {
		t.Fatal("an object writer was allowed to change the bucket Object Lock configuration")
	}

	lockManager := &Identity{
		Name: "lock-manager",
		Actions: []Action{
			Action(s3_constants.ACTION_PUT_BUCKET_OBJECT_LOCK_CONFIG + ":test-bucket"),
		},
	}
	if !lockManager.CanDo(s3_constants.ACTION_PUT_BUCKET_OBJECT_LOCK_CONFIG, "test-bucket", "") {
		t.Fatal("an explicitly delegated identity was denied permission to change the bucket Object Lock configuration")
	}
}

func TestPutObjectLockConfigurationActionRoundTrips(t *testing.T) {
	for _, policyAction := range []string{
		"s3:PutBucketObjectLockConfiguration",
		"PutBucketObjectLockConfiguration",
	} {
		if got := iamlib.MapToStatementAction(policyAction); got != s3_constants.ACTION_PUT_BUCKET_OBJECT_LOCK_CONFIG {
			t.Errorf("MapToStatementAction(%q) = %q, want %q", policyAction, got, s3_constants.ACTION_PUT_BUCKET_OBJECT_LOCK_CONFIG)
		}
	}

	if got := iamlib.MapToIdentitiesAction(s3_constants.ACTION_PUT_BUCKET_OBJECT_LOCK_CONFIG); got != "PutBucketObjectLockConfiguration" {
		t.Errorf("MapToIdentitiesAction(%q) = %q, want PutBucketObjectLockConfiguration", s3_constants.ACTION_PUT_BUCKET_OBJECT_LOCK_CONFIG, got)
	}
	if got := mapBaseActionToS3Format(s3_constants.ACTION_PUT_BUCKET_OBJECT_LOCK_CONFIG); got != s3_constants.S3_ACTION_PUT_BUCKET_OBJECT_LOCK {
		t.Errorf("mapBaseActionToS3Format(%q) = %q, want %q", s3_constants.ACTION_PUT_BUCKET_OBJECT_LOCK_CONFIG, got, s3_constants.S3_ACTION_PUT_BUCKET_OBJECT_LOCK)
	}
}
