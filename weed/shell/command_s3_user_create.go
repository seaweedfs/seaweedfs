package shell

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandS3UserCreate{})
}

type commandS3UserCreate struct {
}

func (c *commandS3UserCreate) Name() string {
	return "s3.user.create"
}

func (c *commandS3UserCreate) Help() string {
	return `create an S3 IAM user

	s3.user.create -name <username>
	s3.user.create -name <username> -access_key <key> -secret_key <secret>

	Creates a new user with a credential pair. If -access_key and -secret_key
	are omitted, they are generated automatically.

	After creating a user, attach policies with s3.policy.attach.

	Output: JSON to stdout. Secret key is printed to stderr only.
`
}

func (c *commandS3UserCreate) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3UserCreate) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	name := f.String("name", "", "user name")
	accessKey := f.String("access_key", "", "access key (generated if omitted)")
	secretKey := f.String("secret_key", "", "secret key (generated if omitted)")
	if err := f.Parse(args); err != nil {
		return err
	}

	if *name == "" {
		return fmt.Errorf("-name is required")
	}

	ak := *accessKey
	sk := *secretKey
	generated := false

	if ak == "" && sk == "" {
		generated = true
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

	identity := &iam_pb.Identity{
		Name: *name,
		Credentials: []*iam_pb.Credential{
			{
				AccessKey: ak,
				SecretKey: sk,
				Status:    iam.AccessKeyStatusActive,
			},
		},
	}

	err := pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := client.CreateUser(ctx, &iam_pb.CreateUserRequest{Identity: identity})
		return err
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
	if err != nil {
		return err
	}

	if generated {
		fmt.Fprintf(os.Stderr, "Secret Key: %s\n", sk)
		fmt.Fprintf(os.Stderr, "Save this secret key - it cannot be retrieved later.\n")
	}

	return json.NewEncoder(writer).Encode(map[string]string{
		"name":       *name,
		"access_key": ak,
	})
}
