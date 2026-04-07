package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
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
`
}

func (c *commandS3UserShow) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3UserShow) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	name := f.String("name", "", "user name")
	if err := f.Parse(args); err != nil {
		return nil
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

		status := "enabled"
		if id.Disabled {
			status = "disabled"
		}
		source := "dynamic"
		if id.IsStatic {
			source = "static"
		}

		fmt.Fprintf(writer, "Name:     %s\n", id.Name)
		fmt.Fprintf(writer, "Status:   %s\n", status)
		fmt.Fprintf(writer, "Source:   %s\n", source)

		if id.Account != nil {
			if id.Account.Id != "" {
				fmt.Fprintf(writer, "Account:  %s", id.Account.Id)
				if id.Account.DisplayName != "" {
					fmt.Fprintf(writer, " (%s)", id.Account.DisplayName)
				}
				fmt.Fprintln(writer)
			}
			if id.Account.EmailAddress != "" {
				fmt.Fprintf(writer, "Email:    %s\n", id.Account.EmailAddress)
			}
		}

		if len(id.PolicyNames) > 0 {
			fmt.Fprintf(writer, "Policies: %s\n", strings.Join(id.PolicyNames, ", "))
		} else {
			fmt.Fprintln(writer, "Policies: (none)")
		}

		if len(id.Actions) > 0 {
			fmt.Fprintf(writer, "Actions:  %s\n", strings.Join(id.Actions, ", "))
		}

		fmt.Fprintln(writer)
		if len(id.Credentials) > 0 {
			fmt.Fprintln(writer, "Credentials:")
			for _, cred := range id.Credentials {
				st := cred.Status
				if st == "" {
					st = "Active"
				}
				fmt.Fprintf(writer, "  %s  %s\n", cred.AccessKey, st)
			}
		} else {
			fmt.Fprintln(writer, "Credentials: (none)")
		}

		if len(id.ServiceAccountIds) > 0 {
			fmt.Fprintf(writer, "\nService Accounts: %s\n", strings.Join(id.ServiceAccountIds, ", "))
		}

		return nil
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}
