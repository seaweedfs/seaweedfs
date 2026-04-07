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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const anonymousUserName = "anonymous"

func init() {
	Commands = append(Commands, &commandS3AnonymousSet{})
}

type commandS3AnonymousSet struct {
}

func (c *commandS3AnonymousSet) Name() string {
	return "s3.anonymous.set"
}

func (c *commandS3AnonymousSet) Help() string {
	return `set anonymous (public) access on a bucket

	s3.anonymous.set -bucket <bucket_name> -access Read,List
	s3.anonymous.set -bucket <bucket_name> -access none

	Supported actions: Read, Write, List, Tagging, Admin
	Use "none" to remove all anonymous access for the bucket.

	This manages the special "anonymous" user's actions. It does not
	use IAM policies — it sets legacy per-bucket actions directly.
`
}

func (c *commandS3AnonymousSet) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3AnonymousSet) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucket := f.String("bucket", "", "bucket name")
	access := f.String("access", "", "comma-separated actions: Read,Write,List,Tagging,Admin or none")
	if err := f.Parse(args); err != nil {
		return err
	}

	if *bucket == "" {
		return fmt.Errorf("-bucket is required")
	}
	if *access == "" {
		return fmt.Errorf("-access is required")
	}

	return pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Get or create anonymous user
		identity, isNew, err := getOrCreateAnonymousUser(ctx, client)
		if err != nil {
			return err
		}

		// Remove existing actions for this bucket
		var kept []string
		for _, a := range identity.Actions {
			parts := strings.SplitN(a, ":", 2)
			if len(parts) != 2 || parts[1] != *bucket {
				kept = append(kept, a)
			}
		}

		// Add new actions unless "none"
		canonicalActions := map[string]string{
			"read": "Read", "write": "Write", "list": "List",
			"tagging": "Tagging", "admin": "Admin",
		}
		if strings.ToLower(strings.TrimSpace(*access)) != "none" {
			seen := make(map[string]struct{})
			for _, action := range strings.Split(*access, ",") {
				action = strings.TrimSpace(action)
				if action != "" {
					canonical, ok := canonicalActions[strings.ToLower(action)]
					if !ok {
						return fmt.Errorf("invalid action %q: supported actions are Read, Write, List, Tagging, Admin", action)
					}
					if _, dup := seen[canonical]; dup {
						continue
					}
					seen[canonical] = struct{}{}
					kept = append(kept, canonical+":"+*bucket)
				}
			}
		}

		identity.Actions = kept

		if isNew {
			_, err = client.CreateUser(ctx, &iam_pb.CreateUserRequest{Identity: identity})
		} else {
			_, err = client.UpdateUser(ctx, &iam_pb.UpdateUserRequest{
				Username: anonymousUserName,
				Identity: identity,
			})
		}
		if err != nil {
			return err
		}

		fmt.Fprintf(writer, "Set anonymous access on bucket %q to: %s\n", *bucket, *access)
		return nil
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}

func getOrCreateAnonymousUser(ctx context.Context, client iam_pb.SeaweedIdentityAccessManagementClient) (*iam_pb.Identity, bool, error) {
	resp, err := client.GetUser(ctx, &iam_pb.GetUserRequest{Username: anonymousUserName})
	if err == nil {
		if resp.Identity == nil {
			return nil, false, fmt.Errorf("anonymous user returned nil identity")
		}
		return resp.Identity, false, nil
	}

	st, ok := status.FromError(err)
	if ok && st != nil && st.Code() == codes.NotFound {
		return &iam_pb.Identity{
			Name:    anonymousUserName,
			Actions: []string{},
		}, true, nil
	}

	return nil, false, fmt.Errorf("failed to get anonymous user: %w", err)
}
