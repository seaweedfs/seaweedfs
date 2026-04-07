package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandS3AccessKeyRotate{})
}

type commandS3AccessKeyRotate struct {
}

func (c *commandS3AccessKeyRotate) Name() string {
	return "s3.accesskey.rotate"
}

func (c *commandS3AccessKeyRotate) Help() string {
	return `rotate an access key for an S3 IAM user

	s3.accesskey.rotate -user <username> -access_key <old_key>

	Creates a new credential pair and deletes the old one. There is a brief
	window where both keys are valid.
`
}

func (c *commandS3AccessKeyRotate) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3AccessKeyRotate) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	user := f.String("user", "", "user name")
	oldKey := f.String("access_key", "", "access key to rotate")
	if err := f.Parse(args); err != nil {
		return err
	}

	if *user == "" {
		return fmt.Errorf("-user is required")
	}
	if *oldKey == "" {
		return fmt.Errorf("-access_key is required")
	}

	newAK, err := iam.GenerateRandomString(iam.AccessKeyIdLength, iam.CharsetUpper)
	if err != nil {
		return fmt.Errorf("generate access key: %v", err)
	}
	newSK, err := iam.GenerateSecretAccessKey()
	if err != nil {
		return fmt.Errorf("generate secret key: %v", err)
	}

	err = pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Create new key first so there's no gap without credentials
		_, err := client.CreateAccessKey(ctx, &iam_pb.CreateAccessKeyRequest{
			Username: *user,
			Credential: &iam_pb.Credential{
				AccessKey: newAK,
				SecretKey: newSK,
				Status:    iam.AccessKeyStatusActive,
			},
		})
		if err != nil {
			return fmt.Errorf("create new key: %v", err)
		}

		// Delete old key
		_, err = client.DeleteAccessKey(ctx, &iam_pb.DeleteAccessKeyRequest{
			Username:  *user,
			AccessKey: *oldKey,
		})
		if err != nil {
			return fmt.Errorf("delete old key (new key %s was already created): %v", newAK, err)
		}

		return nil
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
	if err != nil {
		return err
	}

	fmt.Fprintf(writer, "Rotated access key for user %q\n", *user)
	fmt.Fprintf(writer, "Old Key:    %s (deleted)\n", *oldKey)
	fmt.Fprintf(writer, "Access Key: %s\n", newAK)
	fmt.Fprintf(writer, "Secret Key: %s\n", newSK)
	fmt.Fprintln(writer)
	fmt.Fprintln(writer, "Save these credentials - the secret key cannot be retrieved later.")
	return nil
}
