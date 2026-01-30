package shell

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

func init() {
	Commands = append(Commands, &commandS3TablesBucket{})
}

type commandS3TablesBucket struct{}

func (c *commandS3TablesBucket) Name() string {
	return "s3tables.bucket"
}

func (c *commandS3TablesBucket) Help() string {
	return `manage s3tables table buckets

# create a table bucket
s3tables.bucket -create -name <bucket> [-tags key1=val1,key2=val2]

# list table buckets
s3tables.bucket -list [-prefix <prefix>] [-limit <n>] [-continuation <token>]

# get a table bucket
s3tables.bucket -get -arn <table_bucket_arn>

# delete a table bucket
s3tables.bucket -delete -arn <table_bucket_arn>

# manage bucket policy
s3tables.bucket -put-policy -arn <table_bucket_arn> -file policy.json
s3tables.bucket -get-policy -arn <table_bucket_arn>
s3tables.bucket -delete-policy -arn <table_bucket_arn>
`
}

func (c *commandS3TablesBucket) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3TablesBucket) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	cmd := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	create := cmd.Bool("create", false, "create table bucket")
	list := cmd.Bool("list", false, "list table buckets")
	get := cmd.Bool("get", false, "get table bucket")
	deleteBucket := cmd.Bool("delete", false, "delete table bucket")
	putPolicy := cmd.Bool("put-policy", false, "put table bucket policy")
	getPolicy := cmd.Bool("get-policy", false, "get table bucket policy")
	deletePolicy := cmd.Bool("delete-policy", false, "delete table bucket policy")

	name := cmd.String("name", "", "table bucket name")
	arn := cmd.String("arn", "", "table bucket ARN")
	prefix := cmd.String("prefix", "", "bucket prefix")
	limit := cmd.Int("limit", 100, "max buckets to return")
	continuation := cmd.String("continuation", "", "continuation token")
	tags := cmd.String("tags", "", "comma separated tags key=value")
	policyFile := cmd.String("file", "", "policy file (json)")

	if err := cmd.Parse(args); err != nil {
		return nil
	}

	actions := []*bool{create, list, get, deleteBucket, putPolicy, getPolicy, deletePolicy}
	count := 0
	for _, action := range actions {
		if *action {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("exactly one action must be specified")
	}

	switch {
	case *create:
		if *name == "" {
			return fmt.Errorf("-name is required")
		}
		req := &s3tables.CreateTableBucketRequest{Name: *name}
		if *tags != "" {
			parsed, err := parseS3TablesTags(*tags)
			if err != nil {
				return err
			}
			req.Tags = parsed
		}
		var resp s3tables.CreateTableBucketResponse
		if err := executeS3Tables(commandEnv, "CreateTableBucket", req, &resp); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintf(writer, "ARN: %s\n", resp.ARN)
	case *list:
		req := &s3tables.ListTableBucketsRequest{Prefix: *prefix, ContinuationToken: *continuation, MaxBuckets: *limit}
		var resp s3tables.ListTableBucketsResponse
		if err := executeS3Tables(commandEnv, "ListTableBuckets", req, &resp); err != nil {
			return parseS3TablesError(err)
		}
		if len(resp.TableBuckets) == 0 {
			fmt.Fprintln(writer, "No table buckets found")
			return nil
		}
		for _, bucket := range resp.TableBuckets {
			fmt.Fprintf(writer, "Name: %s\n", bucket.Name)
			fmt.Fprintf(writer, "ARN: %s\n", bucket.ARN)
			fmt.Fprintf(writer, "CreatedAt: %s\n", bucket.CreatedAt.Format(timeFormat))
			fmt.Fprintln(writer, "---")
		}
		if resp.ContinuationToken != "" {
			fmt.Fprintf(writer, "ContinuationToken: %s\n", resp.ContinuationToken)
		}
	case *get:
		if *arn == "" {
			return fmt.Errorf("-arn is required")
		}
		req := &s3tables.GetTableBucketRequest{TableBucketARN: *arn}
		var resp s3tables.GetTableBucketResponse
		if err := executeS3Tables(commandEnv, "GetTableBucket", req, &resp); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintf(writer, "Name: %s\n", resp.Name)
		fmt.Fprintf(writer, "ARN: %s\n", resp.ARN)
		fmt.Fprintf(writer, "OwnerAccountID: %s\n", resp.OwnerAccountID)
		fmt.Fprintf(writer, "CreatedAt: %s\n", resp.CreatedAt.Format(timeFormat))
	case *deleteBucket:
		if *arn == "" {
			return fmt.Errorf("-arn is required")
		}
		req := &s3tables.DeleteTableBucketRequest{TableBucketARN: *arn}
		if err := executeS3Tables(commandEnv, "DeleteTableBucket", req, nil); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintln(writer, "Deleted table bucket")
	case *putPolicy:
		if *arn == "" {
			return fmt.Errorf("-arn is required")
		}
		if *policyFile == "" {
			return fmt.Errorf("-file is required")
		}
		content, err := os.ReadFile(*policyFile)
		if err != nil {
			return err
		}
		req := &s3tables.PutTableBucketPolicyRequest{TableBucketARN: *arn, ResourcePolicy: string(content)}
		if err := executeS3Tables(commandEnv, "PutTableBucketPolicy", req, nil); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintln(writer, "Bucket policy updated")
	case *getPolicy:
		if *arn == "" {
			return fmt.Errorf("-arn is required")
		}
		req := &s3tables.GetTableBucketPolicyRequest{TableBucketARN: *arn}
		var resp s3tables.GetTableBucketPolicyResponse
		if err := executeS3Tables(commandEnv, "GetTableBucketPolicy", req, &resp); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintln(writer, resp.ResourcePolicy)
	case *deletePolicy:
		if *arn == "" {
			return fmt.Errorf("-arn is required")
		}
		req := &s3tables.DeleteTableBucketPolicyRequest{TableBucketARN: *arn}
		if err := executeS3Tables(commandEnv, "DeleteTableBucketPolicy", req, nil); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintln(writer, "Bucket policy deleted")
	}
	return nil
}
