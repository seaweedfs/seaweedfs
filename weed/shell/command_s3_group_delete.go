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
	Commands = append(Commands, &commandS3GroupDelete{})
}

type commandS3GroupDelete struct {
}

func (c *commandS3GroupDelete) Name() string {
	return "s3.group.delete"
}

func (c *commandS3GroupDelete) Help() string {
	return `delete an S3 IAM group

	s3.group.delete -name <groupname>

	The group must have no members and no attached policies.
`
}

func (c *commandS3GroupDelete) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3GroupDelete) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	name := f.String("name", "", "group name")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return fmt.Errorf("-name is required")
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

		for i, g := range cfg.Groups {
			if g.Name == *name {
				if len(g.Members) > 0 {
					return fmt.Errorf("cannot delete group %s: has %d member(s)", *name, len(g.Members))
				}
				if len(g.PolicyNames) > 0 {
					return fmt.Errorf("cannot delete group %s: has %d attached policy(ies)", *name, len(g.PolicyNames))
				}
				cfg.Groups = append(cfg.Groups[:i], cfg.Groups[i+1:]...)
				if _, err := client.PutConfiguration(ctx, &iam_pb.PutConfigurationRequest{Configuration: cfg}); err != nil {
					return err
				}
				return json.NewEncoder(writer).Encode(map[string]string{"deleted": *name})
			}
		}
		return fmt.Errorf("group %s not found", *name)
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}
