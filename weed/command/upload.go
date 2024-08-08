package command

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	upload UploadOptions
)

type UploadOptions struct {
	master       *string
	dir          *string
	include      *string
	replication  *string
	collection   *string
	dataCenter   *string
	ttl          *string
	diskType     *string
	maxMB        *int
	usePublicUrl *bool
}

func init() {
	cmdUpload.Run = runUpload // break init cycle
	cmdUpload.IsDebug = cmdUpload.Flag.Bool("debug", false, "verbose debug information")
	upload.master = cmdUpload.Flag.String("master", "localhost:9333", "SeaweedFS master location")
	upload.dir = cmdUpload.Flag.String("dir", "", "Upload the whole folder recursively if specified.")
	upload.include = cmdUpload.Flag.String("include", "", "pattens of files to upload, e.g., *.pdf, *.html, ab?d.txt, works together with -dir")
	upload.replication = cmdUpload.Flag.String("replication", "", "replication type")
	upload.collection = cmdUpload.Flag.String("collection", "", "optional collection name")
	upload.dataCenter = cmdUpload.Flag.String("dataCenter", "", "optional data center name")
	upload.diskType = cmdUpload.Flag.String("disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	upload.ttl = cmdUpload.Flag.String("ttl", "", "time to live, e.g.: 1m, 1h, 1d, 1M, 1y")
	upload.maxMB = cmdUpload.Flag.Int("maxMB", 4, "split files larger than the limit")
	upload.usePublicUrl = cmdUpload.Flag.Bool("usePublicUrl", false, "upload to public url from volume server")
}

var cmdUpload = &Command{
	UsageLine: "upload -master=localhost:9333 file1 [file2 file3]\n         weed upload -master=localhost:9333 -dir=one_directory -include=*.pdf",
	Short:     "upload one or a list of files",
	Long: `upload one or a list of files, or batch upload one whole folder recursively.

  If uploading a list of files:
  It uses consecutive file keys for the list of files.
  e.g. If the file1 uses key k, file2 can be read via k_1

  If uploading a whole folder recursively:
  All files under the folder and subfolders will be uploaded, each with its own file key.
  Optional parameter "-include" allows you to specify the file name patterns.

  If "maxMB" is set to a positive number, files larger than it would be split into chunks and uploaded separately.
  The list of file ids of those chunks would be stored in an additional chunk, and this additional chunk's file id would be returned.

  `,
}

func runUpload(cmd *Command, args []string) bool {

	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	defaultReplication, err := readMasterConfiguration(grpcDialOption, pb.ServerAddress(*upload.master))
	if err != nil {
		fmt.Printf("upload: %v", err)
		return false
	}
	if *upload.replication == "" {
		*upload.replication = defaultReplication
	}

	if len(args) == 0 {
		if *upload.dir == "" {
			return false
		}
		err = filepath.Walk(util.ResolvePath(*upload.dir), func(path string, info os.FileInfo, err error) error {
			if err == nil {
				if !info.IsDir() {
					if *upload.include != "" {
						if ok, _ := filepath.Match(*upload.include, filepath.Base(path)); !ok {
							return nil
						}
					}
					parts, e := operation.NewFileParts([]string{path})
					if e != nil {
						return e
					}
					results, e := operation.SubmitFiles(func(_ context.Context) pb.ServerAddress { return pb.ServerAddress(*upload.master) }, grpcDialOption, parts, *upload.replication, *upload.collection, *upload.dataCenter, *upload.ttl, *upload.diskType, *upload.maxMB, *upload.usePublicUrl)
					bytes, _ := json.Marshal(results)
					fmt.Println(string(bytes))
					if e != nil {
						return e
					}
				}
			} else {
				fmt.Println(err)
			}
			return err
		})
		if err != nil {
			fmt.Println(err.Error())
			return false
		}
	} else {
		parts, e := operation.NewFileParts(args)
		if e != nil {
			fmt.Println(e.Error())
			return false
		}
		results, err := operation.SubmitFiles(func(_ context.Context) pb.ServerAddress { return pb.ServerAddress(*upload.master) }, grpcDialOption, parts, *upload.replication, *upload.collection, *upload.dataCenter, *upload.ttl, *upload.diskType, *upload.maxMB, *upload.usePublicUrl)
		if err != nil {
			fmt.Println(err.Error())
			return false
		}
		bytes, _ := json.Marshal(results)
		fmt.Println(string(bytes))
	}
	return true
}

func readMasterConfiguration(grpcDialOption grpc.DialOption, masterAddress pb.ServerAddress) (replication string, err error) {
	err = pb.WithMasterClient(false, masterAddress, grpcDialOption, false, func(client master_pb.SeaweedClient) error {
		resp, err := client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get master %s configuration: %v", masterAddress, err)
		}
		replication = resp.DefaultReplication
		return nil
	})
	return
}
