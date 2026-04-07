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
	Commands = append(Commands, &commandS3IAMExport{})
}

type commandS3IAMExport struct {
}

func (c *commandS3IAMExport) Name() string {
	return "s3.iam.export"
}

func (c *commandS3IAMExport) Help() string {
	return `export the full S3 IAM configuration as JSON

	s3.iam.export
	s3.iam.export -file backup.json

	Exports all users, credentials, policies, service accounts, and groups.
	Without -file, prints to stdout.
`
}

func (c *commandS3IAMExport) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3IAMExport) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	file := f.String("file", "", "output file path (stdout if omitted)")
	if err := f.Parse(args); err != nil {
		return nil
	}

	return pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
		client := iam_pb.NewSeaweedIdentityAccessManagementClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		resp, err := client.GetConfiguration(ctx, &iam_pb.GetConfigurationRequest{})
		if err != nil {
			return err
		}

		var out io.Writer = writer
		if *file != "" {
			fp, err := os.Create(*file)
			if err != nil {
				return fmt.Errorf("create file: %v", err)
			}
			defer fp.Close()
			out = fp
		}

		if err := filer.ProtoToText(out, resp.Configuration); err != nil {
			return err
		}
		fmt.Fprintln(out)

		if *file != "" {
			fmt.Fprintf(writer, "Exported IAM configuration to %s\n", *file)
		}
		return nil
	}, commandEnv.option.FilerAddress.ToGrpcAddress(), false, commandEnv.option.GrpcDialOption)
}
