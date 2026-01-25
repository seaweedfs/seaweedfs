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

	if *user == "" {
		// Just show the current configuration
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

	// Helper to extract actions and policies
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

	infoAboutSimulationMode(writer, *apply, "-apply")

	return pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx := context.Background()

		// 1. Fetch current user
		resp, err := client.GetUser(ctx, &iam_pb.GetUserRequest{Username: *user})
		var existingIdentity *iam_pb.Identity
		if err == nil {
			existingIdentity = resp.Identity
		}

		if *isDelete {
			if existingIdentity == nil {
				return fmt.Errorf("user %s not found", *user)
			}

			// If only user is specified, delete the whole user
			if *actions == "" && *accessKey == "" && *buckets == "" && *policies == "" {
				if *apply {
					_, err := client.DeleteUser(ctx, &iam_pb.DeleteUserRequest{Username: *user})
					return err
				}
				fmt.Fprintf(writer, "will delete user %s\n", *user)
				return nil
			}

			// Delete specific components
			updatedIdentity := existingIdentity
			changed := false

			// Delete actions
			if len(cmdActions) > 0 {
				var newActions []string
				for _, current := range updatedIdentity.Actions {
					toDelete := false
					for _, cmdAction := range cmdActions {
						if current == cmdAction {
							toDelete = true
							break
						}
					}
					if !toDelete {
						newActions = append(newActions, current)
					} else {
						changed = true
					}
				}
				updatedIdentity.Actions = newActions
			}

			// Delete policies
			if len(cmdPolicies) > 0 {
				var newPolicies []string
				for _, current := range updatedIdentity.PolicyNames {
					toDelete := false
					for _, cmdPolicy := range cmdPolicies {
						if current == cmdPolicy {
							toDelete = true
							break
						}
					}
					if !toDelete {
						newPolicies = append(newPolicies, current)
					} else {
						changed = true
					}
				}
				updatedIdentity.PolicyNames = newPolicies
			}

			// Delete access key
			if *accessKey != "" {
				if *apply {
					if _, err := client.DeleteAccessKey(ctx, &iam_pb.DeleteAccessKeyRequest{
						Username:  *user,
						AccessKey: *accessKey,
					}); err != nil {
						return err
					}
				} else {
					fmt.Fprintf(writer, "will delete access key %s for user %s\n", *accessKey, *user)
				}
			}

			if changed && *apply {
				_, err := client.UpdateUser(ctx, &iam_pb.UpdateUserRequest{
					Username: *user,
					Identity: updatedIdentity,
				})
				return err
			}
			if changed {
				fmt.Fprintf(writer, "will update user %s (removed requested actions/policies)\n", *user)
			}
			return nil
		}

		// Update or Create
		var identity *iam_pb.Identity
		if existingIdentity != nil {
			identity = existingIdentity
		} else {
			identity = &iam_pb.Identity{
				Name: *user,
			}
		}

		// Update actions
		for _, cmdAction := range cmdActions {
			found := false
			for _, action := range identity.Actions {
				if cmdAction == action {
					found = true
					break
				}
			}
			if !found {
				identity.Actions = append(identity.Actions, cmdAction)
			}
		}

		// Update policies
		for _, cmdPolicy := range cmdPolicies {
			found := false
			for _, policy := range identity.PolicyNames {
				if cmdPolicy == policy {
					found = true
					break
				}
			}
			if !found {
				identity.PolicyNames = append(identity.PolicyNames, cmdPolicy)
			}
		}

		// Update account info
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

		// Apply Identity changes
		if *apply {
			if existingIdentity != nil {
				if _, err := client.UpdateUser(ctx, &iam_pb.UpdateUserRequest{
					Username: *user,
					Identity: identity,
				}); err != nil {
					return err
				}
			} else {
				if _, err := client.CreateUser(ctx, &iam_pb.CreateUserRequest{
					Identity: identity,
				}); err != nil {
					return err
				}
			}

			// Handle credentials
			if *accessKey != "" && *user != "anonymous" {
				if _, err := client.CreateAccessKey(ctx, &iam_pb.CreateAccessKeyRequest{
					Username: *user,
					Credential: &iam_pb.Credential{
						AccessKey: *accessKey,
						SecretKey: *secretKey,
					},
				}); err != nil {
					return err
				}
			}
		} else {
			if existingIdentity != nil {
				fmt.Fprintf(writer, "will update user %s\n", *user)
			} else {
				fmt.Fprintf(writer, "will create user %s\n", *user)
			}
			if *accessKey != "" && *user != "anonymous" {
				fmt.Fprintf(writer, "will create/update access key %s for user %s\n", *accessKey, *user)
			}
		}

		return nil
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}
