package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandS3IAMImport{})
}

type commandS3IAMImport struct {
}

func (c *commandS3IAMImport) Name() string {
	return "s3.iam.import"
}

func (c *commandS3IAMImport) Help() string {
	return `import S3 IAM configuration from a JSON file

	s3.iam.import -file backup.json -force

	Replaces the entire IAM configuration (users, credentials, policies,
	service accounts, groups) with the contents of the file.

	Requires -force to confirm, since this overwrites the current configuration.
`
}

func (c *commandS3IAMImport) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3IAMImport) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	file := f.String("file", "", "input JSON file")
	force := f.Bool("force", false, "confirm overwrite of the entire IAM configuration")
	if err := f.Parse(args); err != nil {
		return err
	}

	if *file == "" {
		return fmt.Errorf("-file is required")
	}
	if !*force {
		return fmt.Errorf("this overwrites the entire IAM configuration; use -force to confirm")
	}

	data, err := os.ReadFile(*file)
	if err != nil {
		return fmt.Errorf("read file: %v", err)
	}

	config := &iam_pb.S3ApiConfiguration{}
	if err := filer.ParseS3ConfigurationFromBytes(data, config); err != nil {
		return fmt.Errorf("parse configuration: %v", err)
	}

	err = pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := client.PutConfiguration(ctx, &iam_pb.PutConfigurationRequest{
			Configuration: config,
		})
		return err
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
	if err != nil {
		return err
	}

	fmt.Fprintf(writer, "Imported IAM configuration from %s\n", *file)
	fmt.Fprintf(writer, "  Users:            %d\n", len(config.Identities))
	fmt.Fprintf(writer, "  Policies:         %d\n", len(config.Policies))
	fmt.Fprintf(writer, "  Service Accounts: %d\n", len(config.ServiceAccounts))
	fmt.Fprintf(writer, "  Groups:           %d\n", len(config.Groups))
	return nil
}
