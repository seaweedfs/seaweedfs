package shell

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func init() {
	Commands = append(Commands, &commandS3Configure{})
}

type commandS3Configure struct {
}

func (c *commandS3Configure) Name() string {
	return "s3.configure"
}

func (c *commandS3Configure) Help() string {
	return `configure and apply s3 options for each bucket

	# see the current configuration file content
	s3.configure

	# create a new identity with account information
	s3.configure -user=username -actions=Read,Write,List,Tagging -buckets=bucket-name -policies=policy1,policy2 -access_key=key -secret_key=secret -account_id=id -account_display_name=name -account_email=email@example.com -apply
	`
}

func (c *commandS3Configure) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3Configure) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	s3ConfigureCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	actions := s3ConfigureCommand.String("actions", "", "comma separated actions names: Read,Write,List,Tagging,Admin")
	user := s3ConfigureCommand.String("user", "", "user name")
	buckets := s3ConfigureCommand.String("buckets", "", "bucket name")
	accessKey := s3ConfigureCommand.String("access_key", "", "specify the access key")
	secretKey := s3ConfigureCommand.String("secret_key", "", "specify the secret key")
	accountId := s3ConfigureCommand.String("account_id", "", "specify the account id")
	accountDisplayName := s3ConfigureCommand.String("account_display_name", "", "specify the account display name")
	accountEmail := s3ConfigureCommand.String("account_email", "", "specify the account email address")
	policies := s3ConfigureCommand.String("policies", "", "comma separated policy names")
	isDelete := s3ConfigureCommand.Bool("delete", false, "delete users, actions, access keys or policies")
	apply := s3ConfigureCommand.Bool("apply", false, "update and apply s3 configuration")
	if err = s3ConfigureCommand.Parse(args); err != nil {
		return nil
	}

	// Case 1: List configuration (no user specified)
	if *user == "" {
		return c.listConfiguration(commandEnv, writer)
	}

	// Case 2: Modify specific user
	var identity *iam_pb.Identity
	var isNewUser bool

	err = pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)

		// Try to get existing user
		resp, getErr := client.GetUser(context.Background(), &iam_pb.GetUserRequest{
			Username: *user,
		})

		if getErr == nil {
			identity = resp.Identity
			if identity == nil {
				// Should not happen if err is nil, but handle defensively
				isNewUser = true
				identity = &iam_pb.Identity{Name: *user}
			}
		} else {
			st, ok := status.FromError(getErr)
			if ok && st.Code() == codes.NotFound {
				isNewUser = true
				identity = &iam_pb.Identity{
					Name:        *user,
					Credentials: []*iam_pb.Credential{},
					Actions:     []string{},
					PolicyNames: []string{},
				}
			} else {
				return fmt.Errorf("failed to get user %s: %v", *user, getErr)
			}
		}

		// Apply changes to identity object
		if err := c.applyChanges(identity, isNewUser, actions, buckets, accessKey, secretKey, policies, isDelete, accountId, accountDisplayName, accountEmail); err != nil {
			return err
		}

		// Print changes (Simulation)
		var buf bytes.Buffer
		filer.ProtoToText(&buf, identity)
		fmt.Fprint(writer, buf.String())
		fmt.Fprintln(writer)

		if !*apply {
			infoAboutSimulationMode(writer, *apply, "-apply")
			return nil
		}

		// Apply changes
		if *isDelete && *actions == "" && *accessKey == "" && *buckets == "" && *policies == "" {
			// Delete User
			_, err := client.DeleteUser(context.Background(), &iam_pb.DeleteUserRequest{Username: *user})
			return err
		} else {
			// Create or Update User
			if isNewUser {
				_, err := client.CreateUser(context.Background(), &iam_pb.CreateUserRequest{Identity: identity})
				return err
			} else {
				_, err := client.UpdateUser(context.Background(), &iam_pb.UpdateUserRequest{Username: *user, Identity: identity})
				return err
			}
		}
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)

	return err
}

func (c *commandS3Configure) listConfiguration(commandEnv *CommandEnv, writer io.Writer) error {
	return pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		resp, err := client.GetConfiguration(context.Background(), &iam_pb.GetConfigurationRequest{})
		if err != nil {
			return err
		}
		var buf bytes.Buffer
		filer.ProtoToText(&buf, resp.Configuration)
		fmt.Fprint(writer, buf.String())
		fmt.Fprintln(writer)
		return nil
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}

func (c *commandS3Configure) applyChanges(identity *iam_pb.Identity, isNewUser bool, actions, buckets, accessKey, secretKey, policies *string, isDelete *bool, accountId, accountDisplayName, accountEmail *string) error {

	// Helper to update account info
	if *accountId != "" || *accountDisplayName != "" || *accountEmail != "" {
		if identity.Account == nil {
			identity.Account = &iam_pb.Account{}
		}
		if *accountId != "" {
			identity.Account.Id = *accountId
		}
		if *accountDisplayName != "" {
			identity.Account.DisplayName = *accountDisplayName
		}
		if *accountEmail != "" {
			identity.Account.EmailAddress = *accountEmail
		}
	}

	// Prepare lists
	var cmdActions []string
	if *actions != "" {
		for _, action := range strings.Split(*actions, ",") {
			if *buckets == "" {
				cmdActions = append(cmdActions, action)
			} else {
				for _, bucket := range strings.Split(*buckets, ",") {
					cmdActions = append(cmdActions, fmt.Sprintf("%s:%s", action, bucket))
				}
			}
		}
	}

	var cmdPolicies []string
	if *policies != "" {
		for _, policy := range strings.Split(*policies, ",") {
			if policy != "" {
				cmdPolicies = append(cmdPolicies, policy)
			}
		}
	}

	if *isDelete {
		// DELETE LOGIC

		// Remove Actions
		if len(cmdActions) > 0 {
			identity.Actions = removeFromSlice(identity.Actions, cmdActions)
		}

		// Remove Credentials
		if *accessKey != "" {
			var keepCredentials []*iam_pb.Credential
			for _, cred := range identity.Credentials {
				if cred.AccessKey != *accessKey {
					keepCredentials = append(keepCredentials, cred)
				}
			}
			identity.Credentials = keepCredentials
		}

		// Remove Policies
		if len(cmdPolicies) > 0 {
			identity.PolicyNames = removeFromSlice(identity.PolicyNames, cmdPolicies)
		}

	} else {
		// ADD/UPDATE LOGIC

		// Add Actions
		identity.Actions = addUniqueToSlice(identity.Actions, cmdActions)

		// Add/Update Credentials
		if *accessKey != "" && identity.Name != "anonymous" {
			found := false
			for _, cred := range identity.Credentials {
				if cred.AccessKey == *accessKey {
					found = true
					if *secretKey != "" {
						cred.SecretKey = *secretKey
					}
					break
				}
			}
			if !found {
				identity.Credentials = append(identity.Credentials, &iam_pb.Credential{
					AccessKey: *accessKey,
					SecretKey: *secretKey,
				})
			}
		}

		// Add Policies
		identity.PolicyNames = addUniqueToSlice(identity.PolicyNames, cmdPolicies)
	}

	return nil
}

// Helper to remove items from a slice
func removeFromSlice(current []string, toRemove []string) []string {
	removeSet := make(map[string]struct{}, len(toRemove))
	for _, item := range toRemove {
		removeSet[item] = struct{}{}
	}
	var result []string
	for _, item := range current {
		if _, found := removeSet[item]; !found {
			result = append(result, item)
		}
	}
	return result
}

// Helper to add unique items to a slice
func addUniqueToSlice(current []string, toAdd []string) []string {
	existingSet := make(map[string]struct{}, len(current))
	for _, item := range current {
		existingSet[item] = struct{}{}
	}
	for _, item := range toAdd {
		if _, found := existingSet[item]; !found {
			current = append(current, item)
		}
	}
	return current
}
