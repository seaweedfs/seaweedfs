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
	Commands = append(Commands, &commandS3AccessKeyCreate{})
}

type commandS3AccessKeyCreate struct {
}

func (c *commandS3AccessKeyCreate) Name() string {
	return "s3.accesskey.create"
}

func (c *commandS3AccessKeyCreate) Help() string {
	return `create an additional access key for an S3 IAM user

	s3.accesskey.create -user <username>
	s3.accesskey.create -user <username> -access_key <key> -secret_key <secret>

	Generates a new credential pair for an existing user. If -access_key and
	-secret_key are omitted, they are generated automatically.
`
}

func (c *commandS3AccessKeyCreate) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3AccessKeyCreate) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	user := f.String("user", "", "user name")
	accessKey := f.String("access_key", "", "access key (generated if omitted)")
	secretKey := f.String("secret_key", "", "secret key (generated if omitted)")
	if err := f.Parse(args); err != nil {
		return err
	}

	if *user == "" {
		return fmt.Errorf("-user is required")
	}

	ak := *accessKey
	sk := *secretKey

	if ak == "" && sk == "" {
		var err error
		ak, err = iam.GenerateRandomString(iam.AccessKeyIdLength, iam.CharsetUpper)
		if err != nil {
			return fmt.Errorf("generate access key: %v", err)
		}
		sk, err = iam.GenerateSecretAccessKey()
		if err != nil {
			return fmt.Errorf("generate secret key: %v", err)
		}
	} else if ak == "" || sk == "" {
		return fmt.Errorf("both -access_key and -secret_key must be provided together, or omit both to auto-generate")
	}

	err := pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := client.CreateAccessKey(ctx, &iam_pb.CreateAccessKeyRequest{
			Username: *user,
			Credential: &iam_pb.Credential{
				AccessKey: ak,
				SecretKey: sk,
				Status:    iam.AccessKeyStatusActive,
			},
		})
		return err
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
	if err != nil {
		return err
	}

	fmt.Fprintf(writer, "Created access key for user %q\n", *user)
	fmt.Fprintf(writer, "Access Key: %s\n", ak)
	fmt.Fprintf(writer, "Secret Key: %s\n", sk)
	fmt.Fprintln(writer)
	fmt.Fprintln(writer, "Save these credentials - the secret key cannot be retrieved later.")
	return nil
}
