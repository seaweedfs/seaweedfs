package shell

import (
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

func init() {
	Commands = append(Commands, &commandS3TablesTag{})
}

type commandS3TablesTag struct{}

func (c *commandS3TablesTag) Name() string {
	return "s3tables.tag"
}

func (c *commandS3TablesTag) Help() string {
	return `manage s3tables tags

# tag a table bucket
s3tables.tag -put -bucket <bucket> -account <account_id> -tags key1=val1,key2=val2

# tag a table
s3tables.tag -put -bucket <bucket> -account <account_id> -namespace <namespace> -name <table> -tags key1=val1,key2=val2

# list tags for a resource
s3tables.tag -list -bucket <bucket> -account <account_id> [-namespace <namespace> -name <table>]

# remove tags
s3tables.tag -delete -bucket <bucket> -account <account_id> [-namespace <namespace> -name <table>] -keys key1,key2
`
}

func (c *commandS3TablesTag) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3TablesTag) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	cmd := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	put := cmd.Bool("put", false, "tag resource")
	list := cmd.Bool("list", false, "list tags")
	del := cmd.Bool("delete", false, "delete tags")

	bucket := cmd.String("bucket", "", "table bucket name")
	account := cmd.String("account", "", "owner account id")
	namespace := cmd.String("namespace", "", "namespace")
	name := cmd.String("name", "", "table name")
	tags := cmd.String("tags", "", "comma separated tags key=value")
	keys := cmd.String("keys", "", "comma separated tag keys")

	if err := cmd.Parse(args); err != nil {
		return err
	}

	actions := []*bool{put, list, del}
	count := 0
	for _, action := range actions {
		if *action {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("exactly one action must be specified")
	}
	if *bucket == "" {
		return fmt.Errorf("-bucket is required")
	}
	if *account == "" {
		return fmt.Errorf("-account is required")
	}
	resourceArn, err := buildS3TablesBucketARN(*bucket, *account)
	if err != nil {
		return err
	}
	if *namespace != "" || *name != "" {
		if *namespace == "" || *name == "" {
			return fmt.Errorf("-namespace and -name are required for table tags")
		}
		resourceArn, err = buildS3TablesTableARN(*bucket, *namespace, *name, *account)
		if err != nil {
			return err
		}
	}

	switch {
	case *put:
		if *tags == "" {
			return fmt.Errorf("-tags is required")
		}
		parsed, err := parseS3TablesTags(*tags)
		if err != nil {
			return err
		}
		req := &s3tables.TagResourceRequest{ResourceARN: resourceArn, Tags: parsed}
		if err := executeS3Tables(commandEnv, "TagResource", req, nil, *account); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintln(writer, "Tags updated")
	case *list:
		req := &s3tables.ListTagsForResourceRequest{ResourceARN: resourceArn}
		var resp s3tables.ListTagsForResourceResponse
		if err := executeS3Tables(commandEnv, "ListTagsForResource", req, &resp, *account); err != nil {
			return parseS3TablesError(err)
		}
		if len(resp.Tags) == 0 {
			fmt.Fprintln(writer, "No tags found")
			return nil
		}
		for k, v := range resp.Tags {
			fmt.Fprintf(writer, "%s=%s\n", k, v)
		}
	case *del:
		if *keys == "" {
			return fmt.Errorf("-keys is required")
		}
		parsed, err := parseS3TablesTagKeys(*keys)
		if err != nil {
			return err
		}
		req := &s3tables.UntagResourceRequest{ResourceARN: resourceArn, TagKeys: parsed}
		if err := executeS3Tables(commandEnv, "UntagResource", req, nil, *account); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintln(writer, "Tags removed")
	}
	return nil
}
