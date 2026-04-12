package shell

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandS3ServiceAccountCreate{})
}

type commandS3ServiceAccountCreate struct {
}

func (c *commandS3ServiceAccountCreate) Name() string {
	return "s3.serviceaccount.create"
}

func (c *commandS3ServiceAccountCreate) Help() string {
	return `create a service account for an S3 IAM user

	s3.serviceaccount.create -user <parent_user> -description "my app"
	s3.serviceaccount.create -user <parent_user> -actions Read,List -expiry 24h

	Service accounts are linked to a parent user and can have restricted
	permissions (a subset of the parent's actions).
`
}

func (c *commandS3ServiceAccountCreate) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3ServiceAccountCreate) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	user := f.String("user", "", "parent user name")
	description := f.String("description", "", "optional description")
	actions := f.String("actions", "", "comma-separated actions (subset of parent)")
	expiry := f.Duration("expiry", 0, "expiration duration (e.g. 24h, 0 = no expiration)")
	if err := f.Parse(args); err != nil {
		return err
	}

	if *user == "" {
		return fmt.Errorf("-user is required")
	}

	ak, err := iam.GenerateRandomString(iam.AccessKeyIdLength, iam.CharsetUpper)
	if err != nil {
		return fmt.Errorf("generate access key: %v", err)
	}
	sk, err := iam.GenerateSecretAccessKey()
	if err != nil {
		return fmt.Errorf("generate secret key: %v", err)
	}

	// Generate a unique service account ID matching the format
	// required by credential.ValidateServiceAccountId: sa:<parent>:<uuid>.
	var idBytes [4]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		return fmt.Errorf("generate service account id: %v", err)
	}
	uuid := fmt.Sprintf("%012d", uint32(idBytes[0])<<24|uint32(idBytes[1])<<16|uint32(idBytes[2])<<8|uint32(idBytes[3]))
	saId := fmt.Sprintf("sa:%s:%s", *user, uuid)

	sa := &iam_pb.ServiceAccount{
		Id:          saId,
		ParentUser:  *user,
		Description: *description,
		Credential: &iam_pb.Credential{
			AccessKey: ak,
			SecretKey: sk,
			Status:    iam.AccessKeyStatusActive,
		},
		CreatedAt: time.Now().Unix(),
	}

	validActions := map[string]string{
		"read": "Read", "write": "Write", "list": "List",
		"tagging": "Tagging", "admin": "Admin",
	}
	if *actions != "" {
		seen := make(map[string]struct{})
		for _, a := range strings.Split(*actions, ",") {
			a = strings.TrimSpace(a)
			if a != "" {
				canonical, ok := validActions[strings.ToLower(a)]
				if !ok {
					return fmt.Errorf("invalid action %q: supported actions are Read, Write, List, Tagging, Admin", a)
				}
				if _, dup := seen[canonical]; dup {
					continue
				}
				seen[canonical] = struct{}{}
				sa.Actions = append(sa.Actions, canonical)
			}
		}
	}

	if *expiry < 0 {
		return fmt.Errorf("-expiry must be >= 0")
	}
	if *expiry > 0 {
		sa.Expiration = time.Now().Add(*expiry).Unix()
	}

	err = pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := client.CreateServiceAccount(ctx, &iam_pb.CreateServiceAccountRequest{
			ServiceAccount: sa,
		})
		return err
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
	if err != nil {
		return err
	}

	fmt.Fprintf(writer, "Created service account for user %q\n", *user)
	fmt.Fprintln(writer, "Note: use s3.serviceaccount.list to find the server-assigned ID.")
	fmt.Fprintf(writer, "Access Key: %s\n", ak)
	fmt.Fprintf(writer, "Secret Key: %s\n", sk)
	if *description != "" {
		fmt.Fprintf(writer, "Desc:       %s\n", *description)
	}
	if *expiry > 0 {
		fmt.Fprintf(writer, "Expires:    %s\n", time.Unix(sa.Expiration, 0).Format(time.RFC3339))
	}
	fmt.Fprintln(writer)
	fmt.Fprintln(writer, "Save these credentials - the secret key cannot be retrieved later.")
	return nil
}
