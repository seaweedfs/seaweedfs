package shell

import (
	"context"
	"encoding/json"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func init() {
	Commands = append(Commands, &commandS3GroupList{})
}

type commandS3GroupList struct {
}

func (c *commandS3GroupList) Name() string {
	return "s3.group.list"
}

func (c *commandS3GroupList) Help() string {
	return `list S3 IAM groups

	s3.group.list

	Output: JSON array of groups with members and policies.
`
}

func (c *commandS3GroupList) HasTag(CommandTag) bool {
	return false
}

type s3GroupListEntry struct {
	Name     string   `json:"name"`
	Status   string   `json:"status"`
	Members  int      `json:"members"`
	Policies []string `json:"policies"`
}

func (c *commandS3GroupList) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	return commandEnv.withIamClient(func(ctx context.Context, client iam_pb.SeaweedIdentityAccessManagementClient) error {
		resp, err := client.GetConfiguration(ctx, &iam_pb.GetConfigurationRequest{})
		if err != nil {
			return err
		}

		var result []s3GroupListEntry
		for _, g := range resp.Configuration.GetGroups() {
			status := "enabled"
			if g.Disabled {
				status = "disabled"
			}
			policies := g.PolicyNames
			if policies == nil {
				policies = []string{}
			}
			result = append(result, s3GroupListEntry{
				Name:     g.Name,
				Status:   status,
				Members:  len(g.Members),
				Policies: policies,
			})
		}
		if result == nil {
			result = []s3GroupListEntry{}
		}
		return json.NewEncoder(writer).Encode(result)
	})
}
