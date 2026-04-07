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
	Commands = append(Commands, &commandS3UserDisable{})
}

type commandS3UserDisable struct {
}

func (c *commandS3UserDisable) Name() string {
	return "s3.user.disable"
}

func (c *commandS3UserDisable) Help() string {
	return `disable an S3 IAM user

	s3.user.disable -name <username>

	Disabled users cannot authenticate. Their credentials and policies
	are preserved and will take effect again when the user is re-enabled.
`
}

func (c *commandS3UserDisable) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3UserDisable) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	name := f.String("name", "", "user name")
	if err := f.Parse(args); err != nil {
		return err
	}

	if *name == "" {
		return fmt.Errorf("-name is required")
	}

	err := pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		resp, err := client.GetUser(ctx, &iam_pb.GetUserRequest{Username: *name})
		if err != nil {
			return fmt.Errorf("get user %q: %w", *name, err)
		}
		if resp.Identity == nil {
			return fmt.Errorf("user %q returned empty identity", *name)
		}

		if resp.Identity.Disabled {
			fmt.Fprintf(writer, "User %q is already disabled.\n", *name)
			return nil
		}

		resp.Identity.Disabled = true
		_, err = client.UpdateUser(ctx, &iam_pb.UpdateUserRequest{
			Username: *name,
			Identity: resp.Identity,
		})
		return err
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
	if err != nil {
		return err
	}

	fmt.Fprintf(writer, "Disabled user %q\n", *name)
	return nil
}
