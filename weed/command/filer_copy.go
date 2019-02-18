package command

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"context"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"io"
	"net/http"
	"strconv"
	"time"
)

var (
	copy CopyOptions
)

type CopyOptions struct {
	filerGrpcPort  *int
	master         *string
	include        *string
	replication    *string
	collection     *string
	ttl            *string
	maxMB          *int
	grpcDialOption grpc.DialOption
}

func init() {
	cmdCopy.Run = runCopy // break init cycle
	cmdCopy.IsDebug = cmdCopy.Flag.Bool("debug", false, "verbose debug information")
	copy.master = cmdCopy.Flag.String("master", "localhost:9333", "SeaweedFS master location")
	copy.include = cmdCopy.Flag.String("include", "", "pattens of files to copy, e.g., *.pdf, *.html, ab?d.txt, works together with -dir")
	copy.replication = cmdCopy.Flag.String("replication", "", "replication type")
	copy.collection = cmdCopy.Flag.String("collection", "", "optional collection name")
	copy.ttl = cmdCopy.Flag.String("ttl", "", "time to live, e.g.: 1m, 1h, 1d, 1M, 1y")
	copy.maxMB = cmdCopy.Flag.Int("maxMB", 0, "split files larger than the limit")
	copy.filerGrpcPort = cmdCopy.Flag.Int("filer.port.grpc", 0, "filer grpc server listen port, default to filer port + 10000")
}

var cmdCopy = &Command{
	UsageLine: "filer.copy file_or_dir1 [file_or_dir2 file_or_dir3] http://localhost:8888/path/to/a/folder/",
	Short:     "copy one or a list of files to a filer folder",
	Long: `copy one or a list of files, or batch copy one whole folder recursively, to a filer folder

  It can copy one or a list of files or folders.

  If copying a whole folder recursively:
  All files under the folder and subfolders will be copyed.
  Optional parameter "-include" allows you to specify the file name patterns.

  If "maxMB" is set to a positive number, files larger than it would be split into chunks.

  `,
}

func runCopy(cmd *Command, args []string) bool {

	weed_server.LoadConfiguration("security", false)

	if len(args) <= 1 {
		return false
	}
	filerDestination := args[len(args)-1]
	fileOrDirs := args[0 : len(args)-1]

	filerUrl, err := url.Parse(filerDestination)
	if err != nil {
		fmt.Printf("The last argument should be a URL on filer: %v\n", err)
		return false
	}
	urlPath := filerUrl.Path
	if !strings.HasSuffix(urlPath, "/") {
		fmt.Printf("The last argument should be a folder and end with \"/\": %v\n", err)
		return false
	}

	if filerUrl.Port() == "" {
		fmt.Printf("The filer port should be specified.\n")
		return false
	}

	filerPort, parseErr := strconv.ParseUint(filerUrl.Port(), 10, 64)
	if parseErr != nil {
		fmt.Printf("The filer port parse error: %v\n", parseErr)
		return false
	}

	filerGrpcPort := filerPort + 10000
	if *copy.filerGrpcPort != 0 {
		filerGrpcPort = uint64(*copy.filerGrpcPort)
	}

	filerGrpcAddress := fmt.Sprintf("%s:%d", filerUrl.Hostname(), filerGrpcPort)
	copy.grpcDialOption = security.LoadClientTLS(viper.Sub("grpc"), "client")

	for _, fileOrDir := range fileOrDirs {
		if !doEachCopy(fileOrDir, filerUrl.Host, filerGrpcAddress, copy.grpcDialOption, urlPath) {
			return false
		}
	}
	return true
}

func doEachCopy(fileOrDir string, filerAddress, filerGrpcAddress string, grpcDialOption grpc.DialOption, path string) bool {
	f, err := os.Open(fileOrDir)
	if err != nil {
		fmt.Printf("Failed to open file %s: %v\n", fileOrDir, err)
		return false
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		fmt.Printf("Failed to get stat for file %s: %v\n", fileOrDir, err)
		return false
	}

	mode := fi.Mode()
	if mode.IsDir() {
		files, _ := ioutil.ReadDir(fileOrDir)
		for _, subFileOrDir := range files {
			if !doEachCopy(fileOrDir+"/"+subFileOrDir.Name(), filerAddress, filerGrpcAddress, grpcDialOption, path+fi.Name()+"/") {
				return false
			}
		}
		return true
	}

	// this is a regular file
	if *copy.include != "" {
		if ok, _ := filepath.Match(*copy.include, filepath.Base(fileOrDir)); !ok {
			return true
		}
	}

	// find the chunk count
	chunkSize := int64(*copy.maxMB * 1024 * 1024)
	chunkCount := 1
	if chunkSize > 0 && fi.Size() > chunkSize {
		chunkCount = int(fi.Size()/chunkSize) + 1
	}

	if chunkCount == 1 {
		return uploadFileAsOne(filerAddress, filerGrpcAddress, grpcDialOption, path, f, fi)
	}

	return uploadFileInChunks(filerAddress, filerGrpcAddress, grpcDialOption, path, f, fi, chunkCount, chunkSize)
}

func uploadFileAsOne(filerAddress, filerGrpcAddress string, grpcDialOption grpc.DialOption, urlFolder string, f *os.File, fi os.FileInfo) bool {

	// upload the file content
	fileName := filepath.Base(f.Name())
	mimeType := detectMimeType(f)

	var chunks []*filer_pb.FileChunk

	if fi.Size() > 0 {

		// assign a volume
		assignResult, err := operation.Assign(*copy.master, grpcDialOption, &operation.VolumeAssignRequest{
			Count:       1,
			Replication: *copy.replication,
			Collection:  *copy.collection,
			Ttl:         *copy.ttl,
		})
		if err != nil {
			fmt.Printf("Failed to assign from %s: %v\n", *copy.master, err)
		}

		targetUrl := "http://" + assignResult.Url + "/" + assignResult.Fid

		uploadResult, err := operation.Upload(targetUrl, fileName, f, false, mimeType, nil, assignResult.Auth)
		if err != nil {
			fmt.Printf("upload data %v to %s: %v\n", fileName, targetUrl, err)
			return false
		}
		if uploadResult.Error != "" {
			fmt.Printf("upload %v to %s result: %v\n", fileName, targetUrl, uploadResult.Error)
			return false
		}
		fmt.Printf("uploaded %s to %s\n", fileName, targetUrl)

		chunks = append(chunks, &filer_pb.FileChunk{
			FileId: assignResult.Fid,
			Offset: 0,
			Size:   uint64(uploadResult.Size),
			Mtime:  time.Now().UnixNano(),
			ETag:   uploadResult.ETag,
		})

		fmt.Printf("copied %s => http://%s%s%s\n", fileName, filerAddress, urlFolder, fileName)
	}

	if err := withFilerClient(filerGrpcAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.CreateEntryRequest{
			Directory: urlFolder,
			Entry: &filer_pb.Entry{
				Name: fileName,
				Attributes: &filer_pb.FuseAttributes{
					Crtime:      time.Now().Unix(),
					Mtime:       time.Now().Unix(),
					Gid:         uint32(os.Getgid()),
					Uid:         uint32(os.Getuid()),
					FileSize:    uint64(fi.Size()),
					FileMode:    uint32(fi.Mode()),
					Mime:        mimeType,
					Replication: *copy.replication,
					Collection:  *copy.collection,
					TtlSec:      int32(util.ParseInt(*copy.ttl, 0)),
				},
				Chunks: chunks,
			},
		}

		if _, err := client.CreateEntry(context.Background(), request); err != nil {
			return fmt.Errorf("update fh: %v", err)
		}
		return nil
	}); err != nil {
		fmt.Printf("upload data %v to http://%s%s%s: %v\n", fileName, filerAddress, urlFolder, fileName, err)
		return false
	}

	return true
}

func uploadFileInChunks(filerAddress, filerGrpcAddress string, grpcDialOption grpc.DialOption, urlFolder string, f *os.File, fi os.FileInfo, chunkCount int, chunkSize int64) bool {

	fileName := filepath.Base(f.Name())
	mimeType := detectMimeType(f)

	var chunks []*filer_pb.FileChunk

	for i := int64(0); i < int64(chunkCount); i++ {

		// assign a volume
		assignResult, err := operation.Assign(*copy.master, grpcDialOption, &operation.VolumeAssignRequest{
			Count:       1,
			Replication: *copy.replication,
			Collection:  *copy.collection,
			Ttl:         *copy.ttl,
		})
		if err != nil {
			fmt.Printf("Failed to assign from %s: %v\n", *copy.master, err)
		}

		targetUrl := "http://" + assignResult.Url + "/" + assignResult.Fid

		uploadResult, err := operation.Upload(targetUrl,
			fileName+"-"+strconv.FormatInt(i+1, 10),
			io.LimitReader(f, chunkSize),
			false, "application/octet-stream", nil, assignResult.Auth)
		if err != nil {
			fmt.Printf("upload data %v to %s: %v\n", fileName, targetUrl, err)
			return false
		}
		if uploadResult.Error != "" {
			fmt.Printf("upload %v to %s result: %v\n", fileName, targetUrl, uploadResult.Error)
			return false
		}
		chunks = append(chunks, &filer_pb.FileChunk{
			FileId: assignResult.Fid,
			Offset: i * chunkSize,
			Size:   uint64(uploadResult.Size),
			Mtime:  time.Now().UnixNano(),
			ETag:   uploadResult.ETag,
		})
		fmt.Printf("uploaded %s-%d to %s [%d,%d)\n", fileName, i+1, targetUrl, i*chunkSize, i*chunkSize+int64(uploadResult.Size))
	}

	if err := withFilerClient(filerGrpcAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.CreateEntryRequest{
			Directory: urlFolder,
			Entry: &filer_pb.Entry{
				Name: fileName,
				Attributes: &filer_pb.FuseAttributes{
					Crtime:      time.Now().Unix(),
					Mtime:       time.Now().Unix(),
					Gid:         uint32(os.Getgid()),
					Uid:         uint32(os.Getuid()),
					FileSize:    uint64(fi.Size()),
					FileMode:    uint32(fi.Mode()),
					Mime:        mimeType,
					Replication: *copy.replication,
					Collection:  *copy.collection,
					TtlSec:      int32(util.ParseInt(*copy.ttl, 0)),
				},
				Chunks: chunks,
			},
		}

		if _, err := client.CreateEntry(context.Background(), request); err != nil {
			return fmt.Errorf("update fh: %v", err)
		}
		return nil
	}); err != nil {
		fmt.Printf("upload data %v to http://%s%s%s: %v\n", fileName, filerAddress, urlFolder, fileName, err)
		return false
	}

	fmt.Printf("copied %s => http://%s%s%s\n", fileName, filerAddress, urlFolder, fileName)

	return true
}

func detectMimeType(f *os.File) string {
	head := make([]byte, 512)
	f.Seek(0, io.SeekStart)
	n, err := f.Read(head)
	if err == io.EOF {
		return ""
	}
	if err != nil {
		fmt.Printf("read head of %v: %v\n", f.Name(), err)
		return "application/octet-stream"
	}
	f.Seek(0, io.SeekStart)
	mimeType := http.DetectContentType(head[:n])
	return mimeType
}

func withFilerClient(filerAddress string, grpcDialOption grpc.DialOption, fn func(filer_pb.SeaweedFilerClient) error) error {

	grpcConnection, err := util.GrpcDial(filerAddress, grpcDialOption)
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", filerAddress, err)
	}
	defer grpcConnection.Close()

	client := filer_pb.NewSeaweedFilerClient(grpcConnection)

	return fn(client)
}
