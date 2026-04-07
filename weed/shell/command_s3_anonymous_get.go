package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func init() {
	Commands = append(Commands, &commandS3AnonymousGet{})
}

type commandS3AnonymousGet struct {
}

func (c *commandS3AnonymousGet) Name() string {
	return "s3.anonymous.get"
}

func (c *commandS3AnonymousGet) Help() string {
	return `show anonymous access for a bucket

	s3.anonymous.get -bucket <bucket_name>
`
}

func (c *commandS3AnonymousGet) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3AnonymousGet) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucket := f.String("bucket", "", "bucket name")
	if err := f.Parse(args); err != nil {
		return nil
	}

	if *bucket == "" {
		return fmt.Errorf("-bucket is required")
	}

	return pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		resp, err := client.GetUser(ctx, &iam_pb.GetUserRequest{Username: anonymousUserName})
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.NotFound {
				fmt.Fprintf(writer, "Bucket: %s\nAccess: none\n", *bucket)
				return nil
			}
			return err
		}

		suffix := ":" + *bucket
		var actions []string
		for _, a := range resp.Identity.Actions {
			if strings.HasSuffix(a, suffix) {
				actions = append(actions, strings.TrimSuffix(a, suffix))
			}
		}

		fmt.Fprintf(writer, "Bucket: %s\n", *bucket)
		if len(actions) == 0 {
			fmt.Fprintln(writer, "Access: none")
		} else {
			sort.Strings(actions)
			fmt.Fprintf(writer, "Access: %s\n", strings.Join(actions, ", "))
		}

		return nil
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}
