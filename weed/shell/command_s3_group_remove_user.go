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
	Commands = append(Commands, &commandS3GroupRemoveUser{})
}

type commandS3GroupRemoveUser struct {
}

func (c *commandS3GroupRemoveUser) Name() string {
	return "s3.group.remove-user"
}

func (c *commandS3GroupRemoveUser) Help() string {
	return `remove a user from an S3 IAM group

	s3.group.remove-user -group <groupname> -user <username>
`
}

func (c *commandS3GroupRemoveUser) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3GroupRemoveUser) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
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
		cfg := resp.GetConfiguration()
		if cfg == nil {
			return fmt.Errorf("no IAM configuration found")
		}

		for _, g := range cfg.Groups {
			if g.Name == *group {
				for i, m := range g.Members {
					if m == *user {
						g.Members = append(g.Members[:i], g.Members[i+1:]...)
						if _, err := client.PutConfiguration(ctx, &iam_pb.PutConfigurationRequest{Configuration: cfg}); err != nil {
							return err
						}
						return json.NewEncoder(writer).Encode(map[string]string{"group": *group, "removed": *user})
					}
				}
				return fmt.Errorf("user %s is not a member of group %s", *user, *group)
			}
		}
		return fmt.Errorf("group %s not found", *group)
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}
