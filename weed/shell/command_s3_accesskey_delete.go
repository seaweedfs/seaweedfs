package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func init() {
	Commands = append(Commands, &commandS3AccessKeyDelete{})
}

type commandS3AccessKeyDelete struct {
}

func (c *commandS3AccessKeyDelete) Name() string {
	return "s3.accesskey.delete"
}

func (c *commandS3AccessKeyDelete) Help() string {
	return `delete an access key from an S3 IAM user

	s3.accesskey.delete -user <username> -access_key <key>
`
}

func (c *commandS3AccessKeyDelete) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3AccessKeyDelete) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	user := f.String("user", "", "user name")
	accessKey := f.String("access_key", "", "access key to delete")
	if err := f.Parse(args); err != nil {
		return err
	}

	if *user == "" {
		return fmt.Errorf("-user is required")
	}
	if *accessKey == "" {
		return fmt.Errorf("-access_key is required")
	}

	err := commandEnv.withIamClient(func(ctx context.Context, client iam_pb.SeaweedIdentityAccessManagementClient) error {
		_, err := client.DeleteAccessKey(ctx, &iam_pb.DeleteAccessKeyRequest{
			Username:  *user,
			AccessKey: *accessKey,
		})
		return err
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(writer, "Deleted access key %s from user %q\n", *accessKey, *user)
	return nil
}
