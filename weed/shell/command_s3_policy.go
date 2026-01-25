package shell

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandS3Policy{})
}

type commandS3Policy struct {
}

func (c *commandS3Policy) Name() string {
	return "s3.policy"
}

func (c *commandS3Policy) Help() string {
	return `manage s3 policies

	# create or update a policy
	s3.policy -put -name=mypolicy -file=policy.json

	# list all policies
	s3.policy -list

	# get a policy
	s3.policy -get -name=mypolicy

	# delete a policy
	s3.policy -delete -name=mypolicy
	`
}

func (c *commandS3Policy) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3Policy) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	s3PolicyCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	put := s3PolicyCommand.Bool("put", false, "create or update a policy")
	get := s3PolicyCommand.Bool("get", false, "get a policy")
	list := s3PolicyCommand.Bool("list", false, "list all policies")
	del := s3PolicyCommand.Bool("delete", false, "delete a policy")
	name := s3PolicyCommand.String("name", "", "policy name")
	file := s3PolicyCommand.String("file", "", "policy file (json)")

	if err = s3PolicyCommand.Parse(args); err != nil {
		return nil
	}

	if !*put && !*get && !*list && !*del {
		return fmt.Errorf("one of -put, -get, -list, -delete must be specified")
	}

	return pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)

		if *put {
			if *name == "" {
				return fmt.Errorf("-name is required")
			}
			if *file == "" {
				return fmt.Errorf("-file is required")
			}
			data, err := ioutil.ReadFile(*file)
			if err != nil {
				return fmt.Errorf("failed to read policy file: %v", err)
			}

			// Validate JSON
			var policy policy_engine.PolicyDocument
			if err := json.Unmarshal(data, &policy); err != nil {
				return fmt.Errorf("invalid policy json: %v", err)
			}

			_, err = client.PutPolicy(context.Background(), &iam_pb.PutPolicyRequest{
				Name:   *name,
				Policy: string(data),
			})
			return err
		}

		if *get {
			if *name == "" {
				return fmt.Errorf("-name is required")
			}
			resp, err := client.GetPolicy(context.Background(), &iam_pb.GetPolicyRequest{
				Name: *name,
			})
			if err != nil {
				return err
			}
			if resp.Policy == "" {
				return fmt.Errorf("policy not found")
			}
			fmt.Fprintf(writer, "%s\n", resp.Policy)
			return nil
		}

		if *list {
			resp, err := client.ListPolicies(context.Background(), &iam_pb.ListPoliciesRequest{})
			if err != nil {
				return err
			}
			for _, policy := range resp.Policies {
				fmt.Fprintf(writer, "Name: %s\n", policy.Name)
				fmt.Fprintf(writer, "Content: %s\n", policy.Content)
				fmt.Fprintf(writer, "---\n")
			}
			return nil
		}

		if *del {
			if *name == "" {
				return fmt.Errorf("-name is required")
			}
			_, err := client.DeletePolicy(context.Background(), &iam_pb.DeletePolicyRequest{
				Name: *name,
			})
			return err
		}

		return nil
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)

}
