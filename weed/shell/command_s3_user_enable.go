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
	Commands = append(Commands, &commandS3UserEnable{})
}

type commandS3UserEnable struct {
}

func (c *commandS3UserEnable) Name() string {
	return "s3.user.enable"
}

func (c *commandS3UserEnable) Help() string {
	return `enable a disabled S3 IAM user

	s3.user.enable -name <username>
`
}

func (c *commandS3UserEnable) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3UserEnable) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	name := f.String("name", "", "user name")
	if err := f.Parse(args); err != nil {
		return err
	}

	if *name == "" {
		return fmt.Errorf("-name is required")
	}

	err := commandEnv.withIamClient(func(ctx context.Context, client iam_pb.SeaweedIdentityAccessManagementClient) error {
		resp, err := client.GetUser(ctx, &iam_pb.GetUserRequest{Username: *name})
		if err != nil {
			return fmt.Errorf("get user %q: %w", *name, err)
		}
		if resp.Identity == nil {
			return fmt.Errorf("user %q returned empty identity", *name)
		}

		if resp.Identity.Disabled {
			resp.Identity.Disabled = false
			_, err = client.UpdateUser(ctx, &iam_pb.UpdateUserRequest{
				Username: *name,
				Identity: resp.Identity,
			})
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	return json.NewEncoder(writer).Encode(map[string]string{"name": *name, "status": "enabled"})
}
