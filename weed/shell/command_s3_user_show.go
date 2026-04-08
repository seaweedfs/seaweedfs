package shell

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandS3UserShow{})
}

type commandS3UserShow struct {
}

func (c *commandS3UserShow) Name() string {
	return "s3.user.show"
}

func (c *commandS3UserShow) Help() string {
	return `show details of an S3 IAM user

	s3.user.show -name <username>

	Output: JSON object with user details.
`
}

func (c *commandS3UserShow) HasTag(CommandTag) bool {
	return false
}

type s3CredentialInfo struct {
	AccessKey string `json:"access_key"`
	Status    string `json:"status"`
}

type s3AccountInfo struct {
	ID          string `json:"id,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
	Email       string `json:"email,omitempty"`
}

type s3UserShowResult struct {
	Name            string            `json:"name"`
	Status          string            `json:"status"`
	Source          string            `json:"source"`
	Account         *s3AccountInfo    `json:"account,omitempty"`
	Policies        []string          `json:"policies"`
	Actions         []string          `json:"actions,omitempty"`
	Credentials     []s3CredentialInfo `json:"credentials"`
	ServiceAccounts []string          `json:"service_accounts,omitempty"`
}

func (c *commandS3UserShow) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	name := f.String("name", "", "user name")
	if err := f.Parse(args); err != nil {
		return err
	}

	if *name == "" {
		return fmt.Errorf("-name is required")
	}

	return pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		resp, err := client.GetUser(ctx, &iam_pb.GetUserRequest{Username: *name})
		if err != nil {
			return err
		}
		id := resp.Identity
		if id == nil {
			return fmt.Errorf("user %q returned empty identity", *name)
		}

		status := "enabled"
		if id.Disabled {
			status = "disabled"
		}
		source := "dynamic"
		if id.IsStatic {
			source = "static"
		}

		result := s3UserShowResult{
			Name:   id.Name,
			Status: status,
			Source: source,
		}

		if id.Account != nil && (id.Account.Id != "" || id.Account.DisplayName != "" || id.Account.EmailAddress != "") {
			result.Account = &s3AccountInfo{
				ID:          id.Account.Id,
				DisplayName: id.Account.DisplayName,
				Email:       id.Account.EmailAddress,
			}
		}

		result.Policies = id.PolicyNames
		if result.Policies == nil {
			result.Policies = []string{}
		}

		if len(id.Actions) > 0 {
			result.Actions = id.Actions
		}

		result.Credentials = make([]s3CredentialInfo, 0, len(id.Credentials))
		for _, cred := range id.Credentials {
			st := cred.Status
			if st == "" {
				st = "Active"
			}
			result.Credentials = append(result.Credentials, s3CredentialInfo{
				AccessKey: cred.AccessKey,
				Status:    st,
			})
		}

		if len(id.ServiceAccountIds) > 0 {
			result.ServiceAccounts = id.ServiceAccountIds
		}

		return json.NewEncoder(writer).Encode(result)
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}
