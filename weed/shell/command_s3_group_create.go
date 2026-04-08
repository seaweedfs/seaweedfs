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
	Commands = append(Commands, &commandS3GroupCreate{})
}

type commandS3GroupCreate struct {
}

func (c *commandS3GroupCreate) Name() string {
	return "s3.group.create"
}

func (c *commandS3GroupCreate) Help() string {
	return `create an S3 IAM group

	s3.group.create -name <groupname>

	Creates a new empty group. Add users with s3.group.add-user and
	attach policies with s3.policy.attach or the IAM API.
`
}

func (c *commandS3GroupCreate) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3GroupCreate) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
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
		cfg := resp.GetConfiguration()
		if cfg == nil {
			return fmt.Errorf("no IAM configuration found")
		}

		for _, g := range cfg.Groups {
			if g.Name == *name {
				return fmt.Errorf("group %s already exists", *name)
			}
		}

		cfg.Groups = append(cfg.Groups, &iam_pb.Group{Name: *name})

		if _, err := client.PutConfiguration(ctx, &iam_pb.PutConfigurationRequest{Configuration: cfg}); err != nil {
			return err
		}

		return json.NewEncoder(writer).Encode(map[string]string{"group": *name})
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}
