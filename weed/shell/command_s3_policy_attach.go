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
	Commands = append(Commands, &commandS3PolicyAttach{})
}

type commandS3PolicyAttach struct {
}

func (c *commandS3PolicyAttach) Name() string {
	return "s3.policy.attach"
}

func (c *commandS3PolicyAttach) Help() string {
	return `attach a policy to an S3 IAM user

	s3.policy.attach -policy <policy_name> -user <username>

	The policy must already exist (create it with s3.policy -put).
`
}

func (c *commandS3PolicyAttach) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3PolicyAttach) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
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

		// Verify the policy exists
		_, err := client.GetPolicy(ctx, &iam_pb.GetPolicyRequest{Name: *policy})
		if err != nil {
			return fmt.Errorf("get policy %q: %w", *policy, err)
		}

		// Get the user
		resp, err := client.GetUser(ctx, &iam_pb.GetUserRequest{Username: *user})
		if err != nil {
			return err
		}

		// Check if already attached
		for _, p := range resp.Identity.PolicyNames {
			if p == *policy {
				fmt.Fprintf(writer, "Policy %q is already attached to user %q.\n", *policy, *user)
				return nil
			}
		}

		resp.Identity.PolicyNames = append(resp.Identity.PolicyNames, *policy)
		_, err = client.UpdateUser(ctx, &iam_pb.UpdateUserRequest{
			Username: *user,
			Identity: resp.Identity,
		})
		if err != nil {
			return err
		}

		fmt.Fprintf(writer, "Attached policy %q to user %q\n", *policy, *user)
		return nil
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}
