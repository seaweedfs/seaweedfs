package command

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	d DownloadOptions
)

type DownloadOptions struct {
	server *string
	dir    *string
}

func init() {
	cmdDownload.Run = runDownload // break init cycle
	d.server = cmdDownload.Flag.String("server", "localhost:9333", "SeaweedFS master location")
	d.dir = cmdDownload.Flag.String("dir", ".", "Download the whole folder recursively if specified.")
}

var cmdDownload = &Command{
	UsageLine: "download -server=localhost:9333 -dir=one_directory fid1 [fid2 fid3 ...]",
	Short:     "download files by file id",
	Long: `download files by file id.

  Usually you just need to use curl to lookup the file's volume server, and then download them directly.
  This download tool combine the two steps into one.

  What's more, if you use "weed upload -maxMB=..." option to upload a big file divided into chunks, you can
  use this tool to download the chunks and merge them automatically.

  `,
}

func runDownload(cmd *Command, args []string) bool {
	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	for _, fid := range args {
		if e := downloadToFile(func(_ context.Context) pb.ServerAddress { return pb.ServerAddress(*d.server) }, grpcDialOption, fid, util.ResolvePath(*d.dir)); e != nil {
			fmt.Println("Download Error: ", fid, e)
		}
	}
	return true
}

func downloadToFile(masterFn operation.GetMasterFn, grpcDialOption grpc.DialOption, fileId, saveDir string) error {
	fileUrl, jwt, lookupError := operation.LookupFileId(masterFn, grpcDialOption, fileId)
	if lookupError != nil {
		return lookupError
	}
	filename, _, rc, err := util_http.DownloadFile(fileUrl, jwt)
	if err != nil {
		return err
	}
	defer util_http.CloseResponse(rc)
	if filename == "" {
		filename = fileId
	}
	isFileList := false
	if strings.HasSuffix(filename, "-list") {
		// old command compatible
		isFileList = true
		filename = filename[0 : len(filename)-len("-list")]
	}
	f, err := os.OpenFile(path.Join(saveDir, filename), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer f.Close()
	if isFileList {
		content, err := io.ReadAll(rc.Body)
		if err != nil {
			return err
		}
		fids := strings.Split(string(content), "\n")
		for _, partId := range fids {
			var n int
			_, part, err := fetchContent(masterFn, grpcDialOption, partId)
			if err == nil {
				n, err = f.Write(part)
			}
			if err == nil && n < len(part) {
				err = io.ErrShortWrite
			}
			if err != nil {
				return err
			}
		}
	} else {
		if _, err = io.Copy(f, rc.Body); err != nil {
			return err
		}

	}
	return nil
}

func fetchContent(masterFn operation.GetMasterFn, grpcDialOption grpc.DialOption, fileId string) (filename string, content []byte, e error) {
	fileUrl, jwt, lookupError := operation.LookupFileId(masterFn, grpcDialOption, fileId)
	if lookupError != nil {
		return "", nil, lookupError
	}
	var rc *http.Response
	if filename, _, rc, e = util_http.DownloadFile(fileUrl, jwt); e != nil {
		return "", nil, e
	}
	defer util_http.CloseResponse(rc)
	content, e = io.ReadAll(rc.Body)
	return
}

func WriteFile(filename string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	f.Close()
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	}
	return err
}
