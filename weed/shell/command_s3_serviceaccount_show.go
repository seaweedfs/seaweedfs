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
	Commands = append(Commands, &commandS3ServiceAccountShow{})
}

type commandS3ServiceAccountShow struct {
}

func (c *commandS3ServiceAccountShow) Name() string {
	return "s3.serviceaccount.show"
}

func (c *commandS3ServiceAccountShow) Help() string {
	return `show details of a service account

	s3.serviceaccount.show -id <service_account_id>
`
}

func (c *commandS3ServiceAccountShow) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3ServiceAccountShow) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	id := f.String("id", "", "service account ID")
	if err := f.Parse(args); err != nil {
		return err
	}

	if *id == "" {
		return fmt.Errorf("-id is required")
	}

	return pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		resp, err := client.GetServiceAccount(ctx, &iam_pb.GetServiceAccountRequest{Id: *id})
		if err != nil {
			return err
		}
		sa := resp.ServiceAccount

		status := "enabled"
		if sa.Disabled {
			status = "disabled"
		}

		fmt.Fprintf(writer, "ID:          %s\n", sa.Id)
		fmt.Fprintf(writer, "Parent:      %s\n", sa.ParentUser)
		fmt.Fprintf(writer, "Status:      %s\n", status)
		if sa.Description != "" {
			fmt.Fprintf(writer, "Description: %s\n", sa.Description)
		}
		if sa.Credential != nil {
			st := sa.Credential.Status
			if st == "" {
				st = "Active"
			}
			fmt.Fprintf(writer, "Access Key:  %s (%s)\n", sa.Credential.AccessKey, st)
		}
		if len(sa.Actions) > 0 {
			fmt.Fprintf(writer, "Actions:     %s\n", strings.Join(sa.Actions, ", "))
		}
		if sa.Expiration > 0 {
			fmt.Fprintf(writer, "Expires:     %s\n", time.Unix(sa.Expiration, 0).Format(time.RFC3339))
		}
		if sa.CreatedAt > 0 {
			fmt.Fprintf(writer, "Created:     %s\n", time.Unix(sa.CreatedAt, 0).Format(time.RFC3339))
		}
		if sa.CreatedBy != "" {
			fmt.Fprintf(writer, "Created By:  %s\n", sa.CreatedBy)
		}

		return nil
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}
