package shell

import (
	"context"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandS3UserList{})
}

type commandS3UserList struct {
}

func (c *commandS3UserList) Name() string {
	return "s3.user.list"
}

func (c *commandS3UserList) Help() string {
	return `list S3 IAM users

	s3.user.list

	Lists all users with their status, attached policies, and credential count.
`
}

func (c *commandS3UserList) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3UserList) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	return pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		listResp, err := client.ListUsers(ctx, &iam_pb.ListUsersRequest{})
		if err != nil {
			return err
		}

		if len(listResp.Usernames) == 0 {
			fmt.Fprintln(writer, "No users found.")
			return nil
		}

		tw := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "NAME\tSTATUS\tPOLICIES\tKEYS")

		for _, username := range listResp.Usernames {
			resp, err := client.GetUser(ctx, &iam_pb.GetUserRequest{Username: username})
			if err != nil {
				fmt.Fprintf(tw, "%s\t<error>\t\t\n", username)
				continue
			}
			id := resp.Identity
			status := "enabled"
			if id.Disabled {
				status = "disabled"
			}
			policies := "-"
			if len(id.PolicyNames) > 0 {
				policies = joinMax(id.PolicyNames, 3)
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\n", id.Name, status, policies, len(id.Credentials))
		}
		return tw.Flush()
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}

// joinMax joins up to max strings with ", " and appends "..." if truncated.
func joinMax(items []string, max int) string {
	if len(items) <= max {
		result := ""
		for i, s := range items {
			if i > 0 {
				result += ", "
			}
			result += s
		}
		return result
	}
	result := ""
	for i := 0; i < max; i++ {
		if i > 0 {
			result += ", "
		}
		result += items[i]
	}
	return result + "..."
}
