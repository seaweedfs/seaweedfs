package shell

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func init() {
	Commands = append(Commands, &commandS3AnonymousList{})
}

type commandS3AnonymousList struct {
}

func (c *commandS3AnonymousList) Name() string {
	return "s3.anonymous.list"
}

func (c *commandS3AnonymousList) Help() string {
	return `list all buckets with anonymous access

	s3.anonymous.list
`
}

func (c *commandS3AnonymousList) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3AnonymousList) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	return commandEnv.withIamClient(func(ctx context.Context, client iam_pb.SeaweedIdentityAccessManagementClient) error {
		resp, err := client.GetUser(ctx, &iam_pb.GetUserRequest{Username: anonymousUserName})
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.NotFound {
				fmt.Fprintln(writer, "No anonymous access configured.")
				return nil
			}
			return err
		}
		if resp.Identity == nil {
			fmt.Fprintln(writer, "No anonymous access configured.")
			return nil
		}

		// Group actions by bucket
		bucketActions := map[string][]string{}
		for _, a := range resp.Identity.Actions {
			parts := strings.SplitN(a, ":", 2)
			if len(parts) == 2 {
				bucketActions[parts[1]] = append(bucketActions[parts[1]], parts[0])
			}
		}

		if len(bucketActions) == 0 {
			fmt.Fprintln(writer, "No anonymous access configured.")
			return nil
		}

		// Sort bucket names
		buckets := make([]string, 0, len(bucketActions))
		for b := range bucketActions {
			buckets = append(buckets, b)
		}
		sort.Strings(buckets)

		tw := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "BUCKET\tACCESS")
		for _, b := range buckets {
			actions := bucketActions[b]
			sort.Strings(actions)
			fmt.Fprintf(tw, "%s\t%s\n", b, strings.Join(actions, ", "))
		}
		return tw.Flush()
	})
}
