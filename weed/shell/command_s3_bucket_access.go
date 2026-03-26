package shell

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// validAccessActions lists the actions accepted by the -access flag.
var validAccessActions = map[string]bool{
	"Read":    true,
	"Write":   true,
	"List":    true,
	"Tagging": true,
	"Admin":   true,
}

func init() {
	Commands = append(Commands, &commandS3BucketAccess{})
}

type commandS3BucketAccess struct {
}

func (c *commandS3BucketAccess) Name() string {
	return "s3.bucket.access"
}

func (c *commandS3BucketAccess) Help() string {
	return `view or set per-bucket access for a user

	Example:
		# View current access for a user on a bucket
		s3.bucket.access -name <bucket_name> -user <username>

		# Grant anonymous read and list access
		s3.bucket.access -name <bucket_name> -user anonymous -access Read,List

		# Grant full anonymous access
		s3.bucket.access -name <bucket_name> -user anonymous -access Read,Write,List

		# Remove all access for a user on a bucket
		s3.bucket.access -name <bucket_name> -user <username> -access none

	Supported action names (comma-separated):
		Read, Write, List, Tagging, Admin

	The user is auto-created if it does not exist. Actions are scoped to
	the specified bucket (stored as "Action:bucket" in the identity).
`
}

func (c *commandS3BucketAccess) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3BucketAccess) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucketName := bucketCommand.String("name", "", "bucket name")
	userName := bucketCommand.String("user", "", "user name")
	access := bucketCommand.String("access", "", "comma-separated actions: Read,Write,List,Tagging,Admin or none")
	if err = bucketCommand.Parse(args); err != nil {
		return nil
	}

	if *bucketName == "" {
		return fmt.Errorf("empty bucket name")
	}
	if *userName == "" {
		return fmt.Errorf("empty user name")
	}

	accessStr := strings.TrimSpace(*access)

	// Validate actions
	if accessStr != "" && strings.ToLower(accessStr) != "none" {
		for _, a := range strings.Split(accessStr, ",") {
			a = strings.TrimSpace(a)
			if !validAccessActions[a] {
				return fmt.Errorf("invalid action %q: must be Read, Write, List, Tagging, Admin, or none", a)
			}
		}
	}

	err = pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)

		// Get or create user
		identity, isNewUser, getErr := getOrCreateIdentity(client, *userName)
		if getErr != nil {
			return getErr
		}

		// View mode: show current bucket-scoped actions
		if accessStr == "" {
			return displayBucketAccess(writer, *bucketName, *userName, identity)
		}

		// Set mode: update actions
		updateBucketActions(identity, *bucketName, accessStr)

		// Show the resulting identity
		var buf bytes.Buffer
		filer.ProtoToText(&buf, identity)
		fmt.Fprint(writer, buf.String())
		fmt.Fprintln(writer)

		// Save
		if isNewUser {
			if _, err := client.CreateUser(context.Background(), &iam_pb.CreateUserRequest{Identity: identity}); err != nil {
				return fmt.Errorf("failed to create user %s: %w", *userName, err)
			}
			fmt.Fprintf(writer, "Created user %q and set access on bucket %s.\n", *userName, *bucketName)
		} else {
			if _, err := client.UpdateUser(context.Background(), &iam_pb.UpdateUserRequest{Username: *userName, Identity: identity}); err != nil {
				return fmt.Errorf("failed to update user %s: %w", *userName, err)
			}
			fmt.Fprintf(writer, "Updated access for user %q on bucket %s.\n", *userName, *bucketName)
		}
		return nil
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)

	return err
}

func getOrCreateIdentity(client iam_pb.SeaweedIdentityAccessManagementClient, userName string) (*iam_pb.Identity, bool, error) {
	resp, getErr := client.GetUser(context.Background(), &iam_pb.GetUserRequest{
		Username: userName,
	})
	if getErr == nil && resp.Identity != nil {
		return resp.Identity, false, nil
	}

	st, ok := status.FromError(getErr)
	if ok && st.Code() == codes.NotFound {
		return &iam_pb.Identity{
			Name:        userName,
			Credentials: []*iam_pb.Credential{},
			Actions:     []string{},
			PolicyNames: []string{},
		}, true, nil
	}

	return nil, false, fmt.Errorf("failed to get user %s: %v", userName, getErr)
}

func displayBucketAccess(writer io.Writer, bucketName, userName string, identity *iam_pb.Identity) error {
	suffix := ":" + bucketName
	var actions []string
	for _, a := range identity.Actions {
		if strings.HasSuffix(a, suffix) {
			actions = append(actions, strings.TrimSuffix(a, suffix))
		}
	}
	fmt.Fprintf(writer, "Bucket: %s\n", bucketName)
	fmt.Fprintf(writer, "User:   %s\n", userName)
	if len(actions) == 0 {
		fmt.Fprintln(writer, "Access: none")
	} else {
		sort.Strings(actions)
		fmt.Fprintf(writer, "Access: %s\n", strings.Join(actions, ","))
	}
	return nil
}

// updateBucketActions removes existing actions for the bucket and adds the new ones.
func updateBucketActions(identity *iam_pb.Identity, bucketName, accessStr string) {
	suffix := ":" + bucketName

	// Remove existing actions for this bucket
	var kept []string
	for _, a := range identity.Actions {
		if !strings.HasSuffix(a, suffix) {
			kept = append(kept, a)
		}
	}

	// Add new actions (unless "none")
	if strings.ToLower(accessStr) != "none" {
		for _, action := range strings.Split(accessStr, ",") {
			action = strings.TrimSpace(action)
			if action != "" {
				kept = append(kept, action+suffix)
			}
		}
	}

	identity.Actions = kept
}
