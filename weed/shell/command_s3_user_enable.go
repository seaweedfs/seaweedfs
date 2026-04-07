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
		return nil
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
			return err
		}

		if !resp.Identity.Disabled {
			fmt.Fprintf(writer, "User %q is already enabled.\n", *name)
			return nil
		}

		resp.Identity.Disabled = false
		_, err = client.UpdateUser(ctx, &iam_pb.UpdateUserRequest{
			Username: *name,
			Identity: resp.Identity,
		})
		return err
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
	if err != nil {
		return err
	}

	fmt.Fprintf(writer, "Enabled user %q\n", *name)
	return nil
}
