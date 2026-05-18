package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func init() {
	Commands = append(Commands, &commandS3ServiceAccountDelete{})
}

type commandS3ServiceAccountDelete struct {
}

func (c *commandS3ServiceAccountDelete) Name() string {
	return "s3.serviceaccount.delete"
}

func (c *commandS3ServiceAccountDelete) Help() string {
	return `delete a service account

	s3.serviceaccount.delete -id <service_account_id>
`
}

func (c *commandS3ServiceAccountDelete) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3ServiceAccountDelete) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	id := f.String("id", "", "service account ID")
	if err := f.Parse(args); err != nil {
		return err
	}

	if *id == "" {
		return fmt.Errorf("-id is required")
	}

	err := commandEnv.withIamClient(func(ctx context.Context, client iam_pb.SeaweedIdentityAccessManagementClient) error {
		_, err := client.DeleteServiceAccount(ctx, &iam_pb.DeleteServiceAccountRequest{Id: *id})
		return err
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(writer, "Deleted service account %q\n", *id)
	return nil
}
