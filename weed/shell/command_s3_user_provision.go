package shell

import (
	"context"
	"encoding/json"
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
	Commands = append(Commands, &commandS3UserProvision{})
}

type commandS3UserProvision struct {
}

func (c *commandS3UserProvision) Name() string {
	return "s3.user.provision"
}

func (c *commandS3UserProvision) Help() string {
	return `create a user with a bucket policy in one step

	s3.user.provision -name <username> -bucket <bucket_name> -role readwrite
	s3.user.provision -name <username> -bucket <bucket_name> -role readonly

	Convenience wrapper that performs these steps:
	  1. Creates an IAM policy for the bucket and role
	  2. Creates the user with auto-generated credentials
	  3. Attaches the policy to the user

	Roles:
	  readonly   - s3:GetObject, s3:ListBucket
	  readwrite  - s3:GetObject, s3:PutObject, s3:DeleteObject, s3:ListBucket
	  admin      - s3:* (full access to the bucket)
`
}

func (c *commandS3UserProvision) HasTag(CommandTag) bool {
	return false
}

var rolePolicies = map[string][]string{
	"readonly":  {"s3:GetObject"},
	"readwrite": {"s3:GetObject", "s3:PutObject", "s3:DeleteObject"},
	"admin":     {"s3:*"},
}

func (c *commandS3UserProvision) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	name := f.String("name", "", "user name")
	bucket := f.String("bucket", "", "bucket name")
	role := f.String("role", "", "role: readonly, readwrite, or admin")
	if err := f.Parse(args); err != nil {
		return err
	}

	if *name == "" {
		return fmt.Errorf("-name is required")
	}
	if *bucket == "" {
		return fmt.Errorf("-bucket is required")
	}
	if strings.ContainsAny(*bucket, "*?") {
		return fmt.Errorf("-bucket must be a literal bucket name, not a wildcard pattern")
	}
	if *role == "" {
		return fmt.Errorf("-role is required (readonly, readwrite, admin)")
	}

	actions, ok := rolePolicies[*role]
	if !ok {
		return fmt.Errorf("unknown role %q: must be readonly, readwrite, or admin", *role)
	}

	policyName := fmt.Sprintf("%s-%s-%s", *bucket, *name, *role)

	// Build the policy document
	bucketActions := []string{"s3:ListBucket"}
	if *role == "admin" {
		bucketActions = []string{"s3:*"}
	}
	policyDoc := map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{
				"Effect":   "Allow",
				"Action":   actions,
				"Resource": []string{fmt.Sprintf("arn:aws:s3:::%s/*", *bucket)},
			},
			{
				"Effect":   "Allow",
				"Action":   bucketActions,
				"Resource": []string{fmt.Sprintf("arn:aws:s3:::%s", *bucket)},
			},
		},
	}
	policyJSON, err := json.Marshal(policyDoc)
	if err != nil {
		return fmt.Errorf("marshal policy: %v", err)
	}

	// Generate credentials
	ak, err := iam.GenerateRandomString(iam.AccessKeyIdLength, iam.CharsetUpper)
	if err != nil {
		return fmt.Errorf("generate access key: %v", err)
	}
	sk, err := iam.GenerateSecretAccessKey()
	if err != nil {
		return fmt.Errorf("generate secret key: %v", err)
	}

	err = pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Step 0: Check if user already exists
		if resp, getErr := client.GetUser(ctx, &iam_pb.GetUserRequest{Username: *name}); getErr == nil && resp.Identity != nil {
			return fmt.Errorf("user %q already exists", *name)
		}

		// Step 1: Create policy
		_, err := client.PutPolicy(ctx, &iam_pb.PutPolicyRequest{
			Name:    policyName,
			Content: string(policyJSON),
		})
		if err != nil {
			return fmt.Errorf("create policy: %v", err)
		}
		fmt.Fprintf(writer, "Created policy %q\n", policyName)

		// Step 2: Create user
		identity := &iam_pb.Identity{
			Name: *name,
			Credentials: []*iam_pb.Credential{
				{
					AccessKey: ak,
					SecretKey: sk,
					Status:    iam.AccessKeyStatusActive,
				},
			},
			PolicyNames: []string{policyName},
		}
		_, err = client.CreateUser(ctx, &iam_pb.CreateUserRequest{Identity: identity})
		if err != nil {
			return fmt.Errorf("create user: %v", err)
		}
		fmt.Fprintf(writer, "Created user %q with policy %q attached\n", *name, policyName)

		return nil
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
	if err != nil {
		return err
	}

	fmt.Fprintln(writer)
	fmt.Fprintf(writer, "Access Key: %s\n", ak)
	fmt.Fprintf(writer, "Secret Key: %s\n", sk)
	fmt.Fprintln(writer)
	fmt.Fprintln(writer, "Save these credentials - the secret key cannot be retrieved later.")
	return nil
}
