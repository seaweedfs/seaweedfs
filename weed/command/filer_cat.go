package command

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"net/url"
	"os"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	filerCat FilerCatOptions
)

type FilerCatOptions struct {
	grpcDialOption grpc.DialOption
	filerAddress   pb.ServerAddress
	filerClient    filer_pb.SeaweedFilerClient
	output         *string
}

func (fco *FilerCatOptions) GetLookupFileIdFunction() wdclient.LookupFileIdFunctionType {
	return func(fileId string) (targetUrls []string, err error) {
		vid := filer.VolumeId(fileId)
		resp, err := fco.filerClient.LookupVolume(context.Background(), &filer_pb.LookupVolumeRequest{
			VolumeIds: []string{vid},
		})
		if err != nil {
			return nil, err
		}
		locations := resp.LocationsMap[vid]
		for _, loc := range locations.Locations {
			targetUrls = append(targetUrls, fmt.Sprintf("http://%s/%s", loc.Url, fileId))
		}
		return
	}
}

func init() {
	cmdFilerCat.Run = runFilerCat // break init cycle
	filerCat.output = cmdFilerCat.Flag.String("o", "", "write to file instead of stdout")
}

var cmdFilerCat = &Command{
	UsageLine: "filer.cat [-o <file>] http://localhost:8888/path/to/file",
	Short:     "copy one file to local",
	Long: `read one file to stdout or write to a file

`,
}

func runFilerCat(cmd *Command, args []string) bool {

	util.LoadSecurityConfiguration()

	if len(args) == 0 {
		return false
	}
	filerSource := args[len(args)-1]

	filerUrl, err := url.Parse(filerSource)
	if err != nil {
		fmt.Printf("The last argument should be a URL on filer: %v\n", err)
		return false
	}
	urlPath := filerUrl.Path
	if strings.HasSuffix(urlPath, "/") {
		fmt.Printf("The last argument should be a file: %v\n", err)
		return false
	}

	filerCat.filerAddress = pb.ServerAddress(filerUrl.Host)
	filerCat.grpcDialOption = security.LoadClientTLS(util.GetViper(), "grpc.client")

	dir, name := util.FullPath(urlPath).DirAndName()

	writer := os.Stdout
	if *filerCat.output != "" {

		fmt.Printf("saving %s to %s\n", filerSource, *filerCat.output)

		f, err := os.OpenFile(*filerCat.output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			fmt.Printf("open file %s: %v\n", *filerCat.output, err)
			return false
		}
		defer f.Close()
		writer = f
	}

	pb.WithFilerClient(false, util.RandomInt32(), filerCat.filerAddress, filerCat.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Name:      name,
			Directory: dir,
		}
		respLookupEntry, err := filer_pb.LookupEntry(client, request)
		if err != nil {
			return err
		}

		if len(respLookupEntry.Entry.Content) > 0 {
			_, err = writer.Write(respLookupEntry.Entry.Content)
			return err
		}

		filerCat.filerClient = client

		return filer.StreamContent(&filerCat, writer, respLookupEntry.Entry.GetChunks(), 0, int64(filer.FileSize(respLookupEntry.Entry)))

	})

	return true
}
