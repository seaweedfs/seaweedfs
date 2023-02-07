package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func init() {
	Commands = append(Commands, &commandS3BucketCreate{})
}

type commandS3BucketCreate struct {
}

func (c *commandS3BucketCreate) Name() string {
	return "s3.bucket.create"
}

func (c *commandS3BucketCreate) Help() string {
	return `create a bucket with a given name

	Example:
		s3.bucket.create -name <bucket_name>
`
}

func (c *commandS3BucketCreate) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucketName := bucketCommand.String("name", "", "bucket name")
	if err = bucketCommand.Parse(args); err != nil {
		return nil
	}

	if *bucketName == "" {
		return fmt.Errorf("empty bucket name")
	}

	err = verifyBucketName(*bucketName)
	if err != nil {
		return err
	}

	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get filer configuration: %v", err)
		}
		filerBucketsPath := resp.DirBuckets

		println("create bucket under", filerBucketsPath)

		entry := &filer_pb.Entry{
			Name:        *bucketName,
			IsDirectory: true,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				Crtime:   time.Now().Unix(),
				FileMode: uint32(0777 | os.ModeDir),
			},
		}

		if err := filer_pb.CreateEntry(client, &filer_pb.CreateEntryRequest{
			Directory: filerBucketsPath,
			Entry:     entry,
		}); err != nil {
			return err
		}

		println("created bucket", *bucketName)

		return nil

	})

	return err

}

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
func verifyBucketName(name string) (err error) {
	if len(name) < 3 || len(name) > 63 {
		return fmt.Errorf("s3 bucket name must between [3, 63] characters")
	}
	for idx, ch := range name {
		if !(unicode.IsLower(ch) || ch == '.' || ch == '-' || unicode.IsNumber(ch)) {
			return fmt.Errorf("s3 bucket name can only contain lower case characters, numbers, dots, and hyphens")
		}
		if idx > 0 && (ch == '.' && name[idx] == '.') {
			return fmt.Errorf("s3 bucket name cannot contain repeated dots")
		}
		//TODO buckets with s3 transfer accleration cannot have . in name
	}
	if name[0] == '.' || name[0] == '-' {
		return fmt.Errorf("name must start with number or lower case character")
	}
	if name[len(name)-1] == '.' || name[len(name)-1] == '-' {
		return fmt.Errorf("name must end with number or lower case character")
	}
	if strings.HasPrefix(name, "xn--") {
		return fmt.Errorf("prefix xn-- is a reserved and not allowed in bucket prefix")
	}
	if strings.HasSuffix(name, "-s3alias") {
		return fmt.Errorf("suffix -s3alias is a reserved and not allowed in bucket suffix")
	}
	if net.ParseIP(name) != nil {
		return fmt.Errorf("bucket name cannot be ip addresses")
	}
	return nil
}