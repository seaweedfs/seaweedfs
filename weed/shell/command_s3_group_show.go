package shell

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func init() {
	Commands = append(Commands, &commandS3GroupShow{})
}

type commandS3GroupShow struct {
}

func (c *commandS3GroupShow) Name() string {
	return "s3.group.show"
}

func (c *commandS3GroupShow) Help() string {
	return `show details of an S3 IAM group

	s3.group.show -name <groupname>

	Output: JSON with group name, status, members, and attached policies.
`
}

func (c *commandS3GroupShow) HasTag(CommandTag) bool {
	return false
}

type s3GroupShowResult struct {
	Name     string   `json:"name"`
	Status   string   `json:"status"`
	Members  []string `json:"members"`
	Policies []string `json:"policies"`
}

func (c *commandS3GroupShow) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	name := f.String("name", "", "group name")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return fmt.Errorf("-name is required")
	}

	return commandEnv.withIamClient(func(ctx context.Context, client iam_pb.SeaweedIdentityAccessManagementClient) error {
		resp, err := client.GetConfiguration(ctx, &iam_pb.GetConfigurationRequest{})
		if err != nil {
			return err
		}

		for _, g := range resp.Configuration.GetGroups() {
			if g.Name == *name {
				status := "enabled"
				if g.Disabled {
					status = "disabled"
				}
				members := g.Members
				if members == nil {
					members = []string{}
				}
				policies := g.PolicyNames
				if policies == nil {
					policies = []string{}
				}
				return json.NewEncoder(writer).Encode(s3GroupShowResult{
					Name:     g.Name,
					Status:   status,
					Members:  members,
					Policies: policies,
				})
			}
		}
		return fmt.Errorf("group %s not found", *name)
	})
}
