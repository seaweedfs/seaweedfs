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

# tag a resource
s3tables.tag -put -arn <resource_arn> -tags key1=val1,key2=val2

# list tags for a resource
s3tables.tag -list -arn <resource_arn>

# remove tags
s3tables.tag -delete -arn <resource_arn> -keys key1,key2
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

	arn := cmd.String("arn", "", "resource ARN")
	tags := cmd.String("tags", "", "comma separated tags key=value")
	keys := cmd.String("keys", "", "comma separated tag keys")

	if err := cmd.Parse(args); err != nil {
		return nil
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
	if *arn == "" {
		return fmt.Errorf("-arn is required")
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
		req := &s3tables.TagResourceRequest{ResourceARN: *arn, Tags: parsed}
		if err := executeS3Tables(commandEnv, "TagResource", req, nil); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintln(writer, "Tags updated")
	case *list:
		req := &s3tables.ListTagsForResourceRequest{ResourceARN: *arn}
		var resp s3tables.ListTagsForResourceResponse
		if err := executeS3Tables(commandEnv, "ListTagsForResource", req, &resp); err != nil {
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
		req := &s3tables.UntagResourceRequest{ResourceARN: *arn, TagKeys: parsed}
		if err := executeS3Tables(commandEnv, "UntagResource", req, nil); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintln(writer, "Tags removed")
	}
	return nil
}
