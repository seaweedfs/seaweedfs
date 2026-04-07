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
	Commands = append(Commands, &commandS3ConfigShow{})
}

type commandS3ConfigShow struct {
}

func (c *commandS3ConfigShow) Name() string {
	return "s3.config.show"
}

func (c *commandS3ConfigShow) Help() string {
	return `show a summary of the current S3 IAM configuration

	s3.config.show

	Displays counts and a brief listing of users, policies, service accounts,
	and groups. Use s3.iam.export for the full JSON dump.
`
}

func (c *commandS3ConfigShow) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3ConfigShow) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	return pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		resp, err := client.GetConfiguration(ctx, &iam_pb.GetConfigurationRequest{})
		if err != nil {
			return err
		}
		cfg := resp.Configuration

		fmt.Fprintf(writer, "S3 IAM Configuration Summary\n")
		fmt.Fprintf(writer, "============================\n\n")

		// Users
		fmt.Fprintf(writer, "Users: %d\n", len(cfg.Identities))
		if len(cfg.Identities) > 0 {
			tw := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
			fmt.Fprintln(tw, "  NAME\tSTATUS\tSOURCE\tKEYS\tPOLICIES")
			for _, id := range cfg.Identities {
				status := "enabled"
				if id.Disabled {
					status = "disabled"
				}
				source := "dynamic"
				if id.IsStatic {
					source = "static"
				}
				policies := "-"
				if len(id.PolicyNames) > 0 {
					policies = joinMax(id.PolicyNames, 3)
				}
				fmt.Fprintf(tw, "  %s\t%s\t%s\t%d\t%s\n",
					id.Name, status, source, len(id.Credentials), policies)
			}
			tw.Flush()
		}
		fmt.Fprintln(writer)

		// Policies
		fmt.Fprintf(writer, "Policies: %d\n", len(cfg.Policies))
		if len(cfg.Policies) > 0 {
			for _, p := range cfg.Policies {
				fmt.Fprintf(writer, "  %s\n", p.Name)
			}
		}
		fmt.Fprintln(writer)

		// Service Accounts
		fmt.Fprintf(writer, "Service Accounts: %d\n", len(cfg.ServiceAccounts))
		if len(cfg.ServiceAccounts) > 0 {
			for _, sa := range cfg.ServiceAccounts {
				status := "enabled"
				if sa.Disabled {
					status = "disabled"
				}
				fmt.Fprintf(writer, "  %s (parent: %s, %s)\n", sa.Id, sa.ParentUser, status)
			}
		}
		fmt.Fprintln(writer)

		// Groups
		fmt.Fprintf(writer, "Groups: %d\n", len(cfg.Groups))
		if len(cfg.Groups) > 0 {
			for _, g := range cfg.Groups {
				fmt.Fprintf(writer, "  %s (%d members)\n", g.Name, len(g.Members))
			}
		}

		return nil
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}
