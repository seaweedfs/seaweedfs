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
	Commands = append(Commands, &commandS3UserDelete{})
}

type commandS3UserDelete struct {
}

func (c *commandS3UserDelete) Name() string {
	return "s3.user.delete"
}

func (c *commandS3UserDelete) Help() string {
	return `delete an S3 IAM user

	s3.user.delete -name <username>
`
}

func (c *commandS3UserDelete) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3UserDelete) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
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
		_, err := client.DeleteUser(ctx, &iam_pb.DeleteUserRequest{Username: *name})
		return err
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
	if err != nil {
		return err
	}

	fmt.Fprintf(writer, "Deleted user %q\n", *name)
	return nil
}
