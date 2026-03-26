package shell

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

const (
	publicAccessBucketSid = "PublicAccessBucket"
	publicAccessObjectSid = "PublicAccessObject"
)

func init() {
	Commands = append(Commands, &commandS3BucketAccess{})
}

type commandS3BucketAccess struct {
}

func (c *commandS3BucketAccess) Name() string {
	return "s3.bucket.access"
}

func (c *commandS3BucketAccess) Help() string {
	return `view or change the anonymous access policy of an S3 bucket

	Example:
		# View the current anonymous access policy
		s3.bucket.access -name <bucket_name>

		# Grant anonymous read and list access
		s3.bucket.access -name <bucket_name> -access read,list

		# Grant full anonymous access
		s3.bucket.access -name <bucket_name> -access read,write,list

		# Remove all anonymous access (make private)
		s3.bucket.access -name <bucket_name> -access none

	Supported permission names (comma-separated):
		read    - anonymous s3:GetObject
		write   - anonymous s3:PutObject, s3:DeleteObject, and multipart uploads
		list    - anonymous s3:ListBucket

	Setting access writes a bucket policy with Principal "*" for anonymous
	access. Setting "none" removes the anonymous policy statements; other
	policy statements are preserved.

	NOTE: For write access, an "anonymous" identity must also be configured
	in IAM. Read and list access work with bucket policy alone.
`
}

func (c *commandS3BucketAccess) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3BucketAccess) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucketName := bucketCommand.String("name", "", "bucket name")
	access := bucketCommand.String("access", "", "comma-separated permissions: read,write,list or none")
	if err = bucketCommand.Parse(args); err != nil {
		return nil
	}

	if *bucketName == "" {
		return fmt.Errorf("empty bucket name")
	}

	accessStr := strings.ToLower(strings.TrimSpace(*access))

	// Validate permissions
	if accessStr != "" && accessStr != "none" {
		for _, p := range strings.Split(accessStr, ",") {
			p = strings.TrimSpace(p)
			if p != "read" && p != "write" && p != "list" {
				return fmt.Errorf("invalid permission %q: must be read, write, list, or none", p)
			}
		}
	}

	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get filer configuration: %w", err)
		}
		filerBucketsPath := resp.DirBuckets

		lookupResp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: filerBucketsPath,
			Name:      *bucketName,
		})
		if err != nil {
			return fmt.Errorf("lookup bucket %s: %w", *bucketName, err)
		}

		entry := lookupResp.Entry

		// View mode
		if accessStr == "" {
			return displayCurrentAccess(writer, *bucketName, entry)
		}

		// Warn if anonymous identity is missing and write access is requested
		if strings.Contains(accessStr, "write") {
			warnIfNoAnonymousIdentity(client, writer)
		}

		// Set mode
		return setAccess(client, writer, filerBucketsPath, *bucketName, entry, accessStr)
	})

	return err
}

func displayCurrentAccess(writer io.Writer, bucketName string, entry *filer_pb.Entry) error {
	fmt.Fprintf(writer, "Bucket: %s\n", bucketName)

	if entry.Extended == nil {
		fmt.Fprintln(writer, "Access: private (no bucket policy)")
		return nil
	}

	policyJSON, exists := entry.Extended[s3_constants.ExtBucketPolicyMetadataKey]
	if !exists || len(policyJSON) == 0 {
		fmt.Fprintln(writer, "Access: private (no bucket policy)")
		return nil
	}

	var policyDoc policy_engine.PolicyDocument
	if err := json.Unmarshal(policyJSON, &policyDoc); err != nil {
		fmt.Fprintf(writer, "Access: unknown (failed to parse bucket policy: %v)\n", err)
		return nil
	}

	perms := detectPermissions(&policyDoc)
	if len(perms) == 0 {
		fmt.Fprintln(writer, "Access: private (no anonymous access statements)")
	} else {
		sort.Strings(perms)
		fmt.Fprintf(writer, "Access: %s\n", strings.Join(perms, ", "))
	}
	return nil
}

func setAccess(client filer_pb.SeaweedFilerClient, writer io.Writer, filerBucketsPath, bucketName string, entry *filer_pb.Entry, accessStr string) error {
	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	}

	// Load existing policy
	var policyDoc policy_engine.PolicyDocument
	if existingJSON, exists := entry.Extended[s3_constants.ExtBucketPolicyMetadataKey]; exists && len(existingJSON) > 0 {
		if err := json.Unmarshal(existingJSON, &policyDoc); err != nil {
			policyDoc = policy_engine.PolicyDocument{}
		}
	}

	// Remove existing public-access statements
	var preserved []policy_engine.PolicyStatement
	for _, stmt := range policyDoc.Statement {
		if stmt.Sid != publicAccessBucketSid && stmt.Sid != publicAccessObjectSid {
			preserved = append(preserved, stmt)
		}
	}
	policyDoc.Statement = preserved

	if accessStr == "none" {
		if len(policyDoc.Statement) == 0 {
			delete(entry.Extended, s3_constants.ExtBucketPolicyMetadataKey)
		} else {
			policyJSON, err := json.Marshal(&policyDoc)
			if err != nil {
				return fmt.Errorf("failed to serialize policy: %w", err)
			}
			entry.Extended[s3_constants.ExtBucketPolicyMetadataKey] = policyJSON
		}

		if _, err := client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
			Directory: filerBucketsPath,
			Entry:     entry,
		}); err != nil {
			return fmt.Errorf("failed to update bucket: %w", err)
		}

		fmt.Fprintf(writer, "Bucket %s access set to private.\n", bucketName)
		return nil
	}

	// Build new statements
	permissions := parsePermissions(accessStr)
	bucketActions := buildBucketActions(permissions)
	objectActions := buildObjectActions(permissions)

	if policyDoc.Version == "" {
		policyDoc.Version = policy_engine.PolicyVersion2012_10_17
	}

	bucketResource := fmt.Sprintf("arn:aws:s3:::%s", bucketName)
	objectResource := fmt.Sprintf("arn:aws:s3:::%s/*", bucketName)

	if len(bucketActions) > 0 {
		sort.Strings(bucketActions)
		policyDoc.Statement = append(policyDoc.Statement, policy_engine.PolicyStatement{
			Sid:       publicAccessBucketSid,
			Effect:    policy_engine.PolicyEffectAllow,
			Principal: policy_engine.NewStringOrStringSlicePtr("*"),
			Action:    policy_engine.NewStringOrStringSlice(bucketActions...),
			Resource:  policy_engine.NewStringOrStringSlicePtr(bucketResource),
		})
	}

	if len(objectActions) > 0 {
		sort.Strings(objectActions)
		policyDoc.Statement = append(policyDoc.Statement, policy_engine.PolicyStatement{
			Sid:       publicAccessObjectSid,
			Effect:    policy_engine.PolicyEffectAllow,
			Principal: policy_engine.NewStringOrStringSlicePtr("*"),
			Action:    policy_engine.NewStringOrStringSlice(objectActions...),
			Resource:  policy_engine.NewStringOrStringSlicePtr(objectResource),
		})
	}

	policyJSON, err := json.Marshal(&policyDoc)
	if err != nil {
		return fmt.Errorf("failed to serialize policy: %w", err)
	}
	entry.Extended[s3_constants.ExtBucketPolicyMetadataKey] = policyJSON

	if _, err := client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
		Directory: filerBucketsPath,
		Entry:     entry,
	}); err != nil {
		return fmt.Errorf("failed to update bucket: %w", err)
	}

	fmt.Fprintf(writer, "Bucket %s access set to %s.\n", bucketName, accessStr)
	return nil
}

func parsePermissions(accessStr string) []string {
	var perms []string
	for _, p := range strings.Split(accessStr, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			perms = append(perms, p)
		}
	}
	return perms
}

func buildBucketActions(permissions []string) []string {
	actionSet := make(map[string]bool)
	for _, p := range permissions {
		switch p {
		case "read":
			actionSet[s3_constants.S3_ACTION_GET_BUCKET_LOCATION] = true
		case "write":
			actionSet[s3_constants.S3_ACTION_GET_BUCKET_LOCATION] = true
			actionSet[s3_constants.S3_ACTION_LIST_MULTIPART_UPLOADS] = true
		case "list":
			actionSet[s3_constants.S3_ACTION_LIST_BUCKET] = true
			actionSet[s3_constants.S3_ACTION_GET_BUCKET_LOCATION] = true
		}
	}
	var actions []string
	for a := range actionSet {
		actions = append(actions, a)
	}
	return actions
}

func buildObjectActions(permissions []string) []string {
	actionSet := make(map[string]bool)
	for _, p := range permissions {
		switch p {
		case "read":
			actionSet[s3_constants.S3_ACTION_GET_OBJECT] = true
		case "write":
			actionSet[s3_constants.S3_ACTION_PUT_OBJECT] = true
			actionSet[s3_constants.S3_ACTION_DELETE_OBJECT] = true
			actionSet[s3_constants.S3_ACTION_ABORT_MULTIPART] = true
			actionSet[s3_constants.S3_ACTION_LIST_PARTS] = true
		}
	}
	var actions []string
	for a := range actionSet {
		actions = append(actions, a)
	}
	return actions
}

// detectPermissions inspects the public-access statements in a policy and returns
// the SeaweedFS permission names (read, write, list) that are granted.
func detectPermissions(policyDoc *policy_engine.PolicyDocument) []string {
	// Collect all actions from our public-access statements
	var allActions []string
	for _, stmt := range policyDoc.Statement {
		if stmt.Sid != publicAccessBucketSid && stmt.Sid != publicAccessObjectSid {
			continue
		}
		if stmt.Effect != policy_engine.PolicyEffectAllow {
			continue
		}
		if stmt.Principal == nil || !slices.Contains(stmt.Principal.Strings(), "*") {
			continue
		}
		allActions = append(allActions, stmt.Action.Strings()...)
	}

	actionSet := make(map[string]bool)
	for _, a := range allActions {
		actionSet[a] = true
	}

	var perms []string
	if actionSet[s3_constants.S3_ACTION_GET_OBJECT] {
		perms = append(perms, "read")
	}
	if actionSet[s3_constants.S3_ACTION_PUT_OBJECT] {
		perms = append(perms, "write")
	}
	if actionSet[s3_constants.S3_ACTION_LIST_BUCKET] {
		perms = append(perms, "list")
	}
	return perms
}

func warnIfNoAnonymousIdentity(client filer_pb.SeaweedFilerClient, writer io.Writer) {
	identitiesDir := filer.IamConfigDirectory + "/identities"
	_, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
		Directory: identitiesDir,
		Name:      "anonymous.json",
	})
	if err != nil {
		fmt.Fprintln(writer, "WARNING: No anonymous identity found in IAM.")
		fmt.Fprintln(writer, "  Write access requires an 'anonymous' identity.")
		fmt.Fprintln(writer, "  Create one via the admin UI or: s3.configure -user=anonymous -apply")
	}
}
