package shell

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandS3GroupAddUser{})
}

type commandS3GroupAddUser struct {
}

func (c *commandS3GroupAddUser) Name() string {
	return "s3.group.add-user"
}

func (c *commandS3GroupAddUser) Help() string {
	return `add a user to an S3 IAM group

	s3.group.add-user -group <groupname> -user <username>
`
}

func (c *commandS3GroupAddUser) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3GroupAddUser) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	group := f.String("group", "", "group name")
	user := f.String("user", "", "user name")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *group == "" {
		return fmt.Errorf("-group is required")
	}
	if *user == "" {
		return fmt.Errorf("-user is required")
	}

	return pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		resp, err := client.GetConfiguration(ctx, &iam_pb.GetConfigurationRequest{})
		if err != nil {
			return err
		}
		cfg := resp.Configuration

		// Verify user exists
		userFound := false
		for _, id := range cfg.Identities {
			if id.Name == *user {
				userFound = true
				break
			}
		}
		if !userFound {
			return fmt.Errorf("user %s not found", *user)
		}

		for _, g := range cfg.Groups {
			if g.Name == *group {
				// Check if already a member
				for _, m := range g.Members {
					if m == *user {
						return fmt.Errorf("user %s is already a member of group %s", *user, *group)
					}
				}
				g.Members = append(g.Members, *user)
				if _, err := client.PutConfiguration(ctx, &iam_pb.PutConfigurationRequest{Configuration: cfg}); err != nil {
					return err
				}
				return json.NewEncoder(writer).Encode(map[string]string{"group": *group, "user": *user})
			}
		}
		return fmt.Errorf("group %s not found", *group)
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}
