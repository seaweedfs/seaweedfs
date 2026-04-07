package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandS3PolicyDetach{})
}

type commandS3PolicyDetach struct {
}

func (c *commandS3PolicyDetach) Name() string {
	return "s3.policy.detach"
}

func (c *commandS3PolicyDetach) Help() string {
	return `detach a policy from an S3 IAM user

	s3.policy.detach -policy <policy_name> -user <username>
`
}

func (c *commandS3PolicyDetach) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3PolicyDetach) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	policy := f.String("policy", "", "policy name")
	user := f.String("user", "", "user name")
	if err := f.Parse(args); err != nil {
		return err
	}

	if *policy == "" {
		return fmt.Errorf("-policy is required")
	}
	if *user == "" {
		return fmt.Errorf("-user is required")
	}

	return pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		resp, err := client.GetUser(ctx, &iam_pb.GetUserRequest{Username: *user})
		if err != nil {
			return fmt.Errorf("get user %q: %w", *user, err)
		}
		if resp.Identity == nil {
			return fmt.Errorf("user %q returned empty identity", *user)
		}

		found := false
		var kept []string
		for _, p := range resp.Identity.PolicyNames {
			if p == *policy {
				found = true
			} else {
				kept = append(kept, p)
			}
		}
		if !found {
			return fmt.Errorf("policy %q is not attached to user %q", *policy, *user)
		}

		resp.Identity.PolicyNames = kept
		_, err = client.UpdateUser(ctx, &iam_pb.UpdateUserRequest{
			Username: *user,
			Identity: resp.Identity,
		})
		if err != nil {
			return err
		}

		fmt.Fprintf(writer, "Detached policy %q from user %q\n", *policy, *user)
		return nil
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}
