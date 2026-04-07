package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandS3AccessKeyList{})
}

type commandS3AccessKeyList struct {
}

func (c *commandS3AccessKeyList) Name() string {
	return "s3.accesskey.list"
}

func (c *commandS3AccessKeyList) Help() string {
	return `list access keys for an S3 IAM user

	s3.accesskey.list -user <username>
`
}

func (c *commandS3AccessKeyList) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3AccessKeyList) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	user := f.String("user", "", "user name")
	if err := f.Parse(args); err != nil {
		return err
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
			return err
		}

		if len(resp.Identity.Credentials) == 0 {
			fmt.Fprintf(writer, "No access keys for user %q.\n", *user)
			return nil
		}

		tw := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "ACCESS KEY\tSTATUS")
		for _, cred := range resp.Identity.Credentials {
			st := cred.Status
			if st == "" {
				st = "Active"
			}
			fmt.Fprintf(tw, "%s\t%s\n", cred.AccessKey, st)
		}
		return tw.Flush()
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}
