package shell

import (
	"context"
	"encoding/json"
	"io"
	"strings"
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

	Output: JSON array of users with status, policies, and credential count.
`
}

func (c *commandS3UserList) HasTag(CommandTag) bool {
	return false
}

type s3UserListEntry struct {
	Name     string   `json:"name"`
	Status   string   `json:"status"`
	Policies []string `json:"policies"`
	Keys     int      `json:"keys"`
}

func (c *commandS3UserList) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	return pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		resp, err := client.GetConfiguration(ctx, &iam_pb.GetConfigurationRequest{})
		if err != nil {
			return err
		}

		var result []s3UserListEntry
		for _, id := range resp.Configuration.GetIdentities() {
			status := "enabled"
			if id.Disabled {
				status = "disabled"
			}
			policies := id.PolicyNames
			if policies == nil {
				policies = []string{}
			}
			result = append(result, s3UserListEntry{
				Name:     id.Name,
				Status:   status,
				Policies: policies,
				Keys:     len(id.Credentials),
			})
		}
		if result == nil {
			result = []s3UserListEntry{}
		}
		return json.NewEncoder(writer).Encode(result)
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}

// joinMax joins up to max strings with ", " and appends "..." if truncated.
func joinMax(items []string, max int) string {
	if len(items) <= max {
		return strings.Join(items, ", ")
	}
	return strings.Join(items[:max], ", ") + "..."
}
