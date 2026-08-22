package app

import "github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"

// PolicyActionSuggestions feeds the <datalist> used by the structured policy
// editor's Action inputs. This is an input-assistance aid, NOT a validation
// list: the editor accepts any action string typed by hand, since IAM policy
// actions are not restricted to this set (custom/future actions, wildcards
// like "s3:Get*", etc.).
var PolicyActionSuggestions = buildPolicyActionSuggestions()

func buildPolicyActionSuggestions() []string {
	return []string{
		// s3: actions, sourced from the constants used elsewhere for policy
		// evaluation (weed/s3api/s3_constants/s3_action_strings.go) so the
		// suggestion list can't drift from the strings the engine actually
		// understands.
		s3_constants.S3_ACTION_ALL,
		s3_constants.S3_ACTION_GET_OBJECT,
		s3_constants.S3_ACTION_PUT_OBJECT,
		s3_constants.S3_ACTION_DELETE_OBJECT,
		s3_constants.S3_ACTION_DELETE_OBJECT_VERSION,
		s3_constants.S3_ACTION_GET_OBJECT_VERSION,
		s3_constants.S3_ACTION_GET_OBJECT_ATTRIBUTES,
		s3_constants.S3_ACTION_GET_OBJECT_ACL,
		s3_constants.S3_ACTION_PUT_OBJECT_ACL,
		s3_constants.S3_ACTION_GET_OBJECT_TAGGING,
		s3_constants.S3_ACTION_PUT_OBJECT_TAGGING,
		s3_constants.S3_ACTION_DELETE_OBJECT_TAGGING,
		s3_constants.S3_ACTION_GET_OBJECT_RETENTION,
		s3_constants.S3_ACTION_PUT_OBJECT_RETENTION,
		s3_constants.S3_ACTION_GET_OBJECT_LEGAL_HOLD,
		s3_constants.S3_ACTION_PUT_OBJECT_LEGAL_HOLD,
		s3_constants.S3_ACTION_BYPASS_GOVERNANCE,
		s3_constants.S3_ACTION_CREATE_MULTIPART,
		s3_constants.S3_ACTION_UPLOAD_PART,
		s3_constants.S3_ACTION_COMPLETE_MULTIPART,
		s3_constants.S3_ACTION_ABORT_MULTIPART,
		s3_constants.S3_ACTION_UPLOAD_PART_COPY,
		s3_constants.S3_ACTION_LIST_PARTS,
		s3_constants.S3_ACTION_LIST_MULTIPART_UPLOADS,
		s3_constants.S3_ACTION_CREATE_BUCKET,
		s3_constants.S3_ACTION_DELETE_BUCKET,
		s3_constants.S3_ACTION_LIST_BUCKET,
		s3_constants.S3_ACTION_LIST_BUCKET_VERSIONS,
		s3_constants.S3_ACTION_GET_BUCKET_ACL,
		s3_constants.S3_ACTION_PUT_BUCKET_ACL,
		s3_constants.S3_ACTION_GET_BUCKET_POLICY,
		s3_constants.S3_ACTION_PUT_BUCKET_POLICY,
		s3_constants.S3_ACTION_DELETE_BUCKET_POLICY,
		s3_constants.S3_ACTION_GET_BUCKET_TAGGING,
		s3_constants.S3_ACTION_PUT_BUCKET_TAGGING,
		s3_constants.S3_ACTION_DELETE_BUCKET_TAGGING,
		s3_constants.S3_ACTION_GET_BUCKET_CORS,
		s3_constants.S3_ACTION_PUT_BUCKET_CORS,
		s3_constants.S3_ACTION_DELETE_BUCKET_CORS,
		s3_constants.S3_ACTION_GET_BUCKET_LIFECYCLE,
		s3_constants.S3_ACTION_PUT_BUCKET_LIFECYCLE,
		s3_constants.S3_ACTION_GET_BUCKET_VERSIONING,
		s3_constants.S3_ACTION_PUT_BUCKET_VERSIONING,
		s3_constants.S3_ACTION_GET_BUCKET_LOCATION,
		s3_constants.S3_ACTION_GET_BUCKET_NOTIFICATION,
		s3_constants.S3_ACTION_PUT_BUCKET_NOTIFICATION,
		s3_constants.S3_ACTION_GET_BUCKET_OBJECT_LOCK,
		s3_constants.S3_ACTION_PUT_BUCKET_OBJECT_LOCK,

		// s3tables: actions, sourced from weed/s3api/s3_constants so this list
		// can't drift from the strings the s3tables engine actually understands.
		s3_constants.S3TABLES_ACTION_ALL,
		s3_constants.S3TABLES_ACTION_CREATE_TABLE_BUCKET,
		s3_constants.S3TABLES_ACTION_GET_TABLE_BUCKET,
		s3_constants.S3TABLES_ACTION_LIST_TABLE_BUCKETS,
		s3_constants.S3TABLES_ACTION_DELETE_TABLE_BUCKET,
		s3_constants.S3TABLES_ACTION_PUT_TABLE_BUCKET_POLICY,
		s3_constants.S3TABLES_ACTION_GET_TABLE_BUCKET_POLICY,
		s3_constants.S3TABLES_ACTION_DELETE_TABLE_BUCKET_POLICY,
		s3_constants.S3TABLES_ACTION_CREATE_NAMESPACE,
		s3_constants.S3TABLES_ACTION_GET_NAMESPACE,
		s3_constants.S3TABLES_ACTION_UPDATE_NAMESPACE,
		s3_constants.S3TABLES_ACTION_LIST_NAMESPACES,
		s3_constants.S3TABLES_ACTION_DELETE_NAMESPACE,
		s3_constants.S3TABLES_ACTION_CREATE_TABLE,
		s3_constants.S3TABLES_ACTION_REGISTER_TABLE,
		s3_constants.S3TABLES_ACTION_GET_TABLE,
		s3_constants.S3TABLES_ACTION_LIST_TABLES,
		s3_constants.S3TABLES_ACTION_UPDATE_TABLE,
		s3_constants.S3TABLES_ACTION_DELETE_TABLE,
		s3_constants.S3TABLES_ACTION_RENAME_TABLE,
		s3_constants.S3TABLES_ACTION_CREATE_VIEW,
		s3_constants.S3TABLES_ACTION_GET_VIEW,
		s3_constants.S3TABLES_ACTION_LIST_VIEWS,
		s3_constants.S3TABLES_ACTION_UPDATE_VIEW,
		s3_constants.S3TABLES_ACTION_DELETE_VIEW,
		s3_constants.S3TABLES_ACTION_RENAME_VIEW,
		s3_constants.S3TABLES_ACTION_PUT_TABLE_POLICY,
		s3_constants.S3TABLES_ACTION_GET_TABLE_POLICY,
		s3_constants.S3TABLES_ACTION_DELETE_TABLE_POLICY,
		s3_constants.S3TABLES_ACTION_PUT_TABLE_BUCKET_MAINTENANCE_CONFIGURATION,
		s3_constants.S3TABLES_ACTION_GET_TABLE_BUCKET_MAINTENANCE_CONFIGURATION,
		s3_constants.S3TABLES_ACTION_PUT_TABLE_MAINTENANCE_CONFIGURATION,
		s3_constants.S3TABLES_ACTION_GET_TABLE_MAINTENANCE_CONFIGURATION,
		s3_constants.S3TABLES_ACTION_GET_TABLE_MAINTENANCE_JOB_STATUS,
		s3_constants.S3TABLES_ACTION_TAG_RESOURCE,
		s3_constants.S3TABLES_ACTION_LIST_TAGS_FOR_RESOURCE,
		s3_constants.S3TABLES_ACTION_UNTAG_RESOURCE,
	}
}
