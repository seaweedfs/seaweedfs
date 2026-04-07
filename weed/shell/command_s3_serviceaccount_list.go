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
	Commands = append(Commands, &commandS3ServiceAccountList{})
}

type commandS3ServiceAccountList struct {
}

func (c *commandS3ServiceAccountList) Name() string {
	return "s3.serviceaccount.list"
}

func (c *commandS3ServiceAccountList) Help() string {
	return `list service accounts

	s3.serviceaccount.list
	s3.serviceaccount.list -user <parent_user>

	Lists all service accounts, optionally filtered by parent user.
`
}

func (c *commandS3ServiceAccountList) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3ServiceAccountList) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	user := f.String("user", "", "filter by parent user (optional)")
	if err := f.Parse(args); err != nil {
		return nil
	}

	return pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		resp, err := client.ListServiceAccounts(ctx, &iam_pb.ListServiceAccountsRequest{})
		if err != nil {
			return err
		}

		var filtered []*iam_pb.ServiceAccount
		for _, sa := range resp.ServiceAccounts {
			if *user == "" || sa.ParentUser == *user {
				filtered = append(filtered, sa)
			}
		}

		if len(filtered) == 0 {
			fmt.Fprintln(writer, "No service accounts found.")
			return nil
		}

		tw := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tPARENT\tSTATUS\tDESCRIPTION")
		for _, sa := range filtered {
			st := "enabled"
			if sa.Disabled {
				st = "disabled"
			}
			desc := sa.Description
			if len(desc) > 40 {
				desc = desc[:37] + "..."
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", sa.Id, sa.ParentUser, st, desc)
		}
		return tw.Flush()
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}
