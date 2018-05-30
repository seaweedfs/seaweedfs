package command

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/operation"
	filer_operation "github.com/chrislusf/seaweedfs/weed/operation/filer"
	"github.com/chrislusf/seaweedfs/weed/security"
	"path"
	"net/http"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"strconv"
	"io"
	"time"
	"google.golang.org/grpc"
	"context"
)

var (
	copy CopyOptions
)

type CopyOptions struct {
	master      *string
	include     *string
	replication *string
	collection  *string
	ttl         *string
	maxMB       *int
	secretKey   *string

	secret security.Secret
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
	copy.secretKey = cmdCopy.Flag.String("secure.secret", "", "secret to encrypt Json Web Token(JWT)")
}

var cmdCopy = &Command{
	UsageLine: "filer.copy file_or_dir1 [file_or_dir2 file_or_dir3] http://localhost:8888/path/to/a/folder/",
	Short:     "copy one or a list of files to a filer folder",
	Long: `copy one or a list of files, or batch copy one whole folder recursively, to a filer folder

  It can copy one or a list of files or folders.

  If copying a whole folder recursively:
  All files under the folder and subfolders will be copyed.
  Optional parameter "-include" allows you to specify the file name patterns.

  If any file has a ".gz" extension, the content are considered gzipped already, and will be stored as is.
  This can save volume server's gzipped processing and allow customizable gzip compression level.
  The file name will strip out ".gz" and stored. For example, "jquery.js.gz" will be stored as "jquery.js".

  If "maxMB" is set to a positive number, files larger than it would be split into chunks and copyed separatedly.
  The list of file ids of those chunks would be stored in an additional chunk, and this additional chunk's file id would be returned.

  `,
}

func runCopy(cmd *Command, args []string) bool {
	copy.secret = security.Secret(*copy.secretKey)
	if len(args) <= 1 {
		return false
	}
	filerDestination := args[len(args)-1]
	fileOrDirs := args[0: len(args)-1]

	filerUrl, err := url.Parse(filerDestination)
	if err != nil {
		fmt.Printf("The last argument should be a URL on filer: %v\n", err)
		return false
	}
	urlPath := filerUrl.Path
	if !strings.HasSuffix(urlPath, "/") {
		urlPath = urlPath + "/"
	}

	for _, fileOrDir := range fileOrDirs {
		if !doEachCopy(fileOrDir, filerUrl.Host, urlPath) {
			return false
		}
	}
	return true
}

func doEachCopy(fileOrDir string, host string, path string) bool {
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
			if !doEachCopy(fileOrDir+"/"+subFileOrDir.Name(), host, path+fi.Name()+"/") {
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

	// assign a volume
	assignResult, err := operation.Assign(*copy.master, &operation.VolumeAssignRequest{
		Count:       uint64(chunkCount),
		Replication: *copy.replication,
		Collection:  *copy.collection,
		Ttl:         *copy.ttl,
	})
	if err != nil {
		fmt.Printf("Failed to assign from %s: %v\n", *copy.master, err)
	}

	if chunkCount == 1 {
		return uploadFileAsOne(host, path, assignResult, f, fi)
	}

	return uploadFileInChunks(host, path, assignResult, f, fi, chunkCount, chunkSize)
}

func uploadFileAsOne(filerUrl string, urlFolder string, assignResult *operation.AssignResult, f *os.File, fi os.FileInfo) bool {
	// upload the file content

	mimeType := detectMimeType(f)
	isGzipped := isGzipped(f.Name())

	targetUrl := "http://" + assignResult.Url + "/" + assignResult.Fid

	uploadResult, err := operation.Upload(targetUrl, f.Name(), f, isGzipped, mimeType, nil, "")
	if err != nil {
		fmt.Printf("upload data %v to %s: %v\n", f.Name(), targetUrl, err)
		return false
	}
	if uploadResult.Error != "" {
		fmt.Printf("upload %v to %s result: %v\n", f.Name(), targetUrl, uploadResult.Error)
		return false
	}
	fmt.Printf("uploaded %s to %s\n", f.Name(), targetUrl)

	if err = filer_operation.RegisterFile(filerUrl, filepath.Join(urlFolder, f.Name()), assignResult.Fid, fi.Size(),
		os.Getuid(), os.Getgid(), copy.secret); err != nil {
		fmt.Printf("Failed to register file %s on %s: %v\n", f.Name(), filerUrl, err)
		return false
	}

	fmt.Printf("copied %s => http://%s%s%s\n", f.Name(), filerUrl, urlFolder, f.Name())
	return true
}

func uploadFileInChunks(filerUrl string, urlFolder string, assignResult *operation.AssignResult, f *os.File, fi os.FileInfo, chunkCount int, chunkSize int64) bool {

	var chunks []*filer_pb.FileChunk

	for i := int64(0); i < int64(chunkCount); i++ {
		fileId := assignResult.Fid
		if i > 0 {
			fileId += "_" + strconv.FormatInt(i, 10)
		}

		targetUrl := "http://" + assignResult.Url + "/" + fileId

		uploadResult, err := operation.Upload(targetUrl,
			f.Name()+"-"+strconv.FormatInt(i+1, 10),
			io.LimitReader(f, chunkSize),
			false, "application/octet-stream", nil, "")
		if err != nil {
			fmt.Printf("upload data %v to %s: %v\n", f.Name(), targetUrl, err)
			return false
		}
		if uploadResult.Error != "" {
			fmt.Printf("upload %v to %s result: %v\n", f.Name(), targetUrl, uploadResult.Error)
			return false
		}
		chunks = append(chunks, &filer_pb.FileChunk{
			FileId: fileId,
			Offset: i * chunkSize,
			Size:   uint64(uploadResult.Size),
			Mtime:  time.Now().UnixNano(),
		})
		fmt.Printf("uploaded %s split %d => %s\n", f.Name(), i, targetUrl)
	}

	if err := withFilerClient(filerUrl, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.CreateEntryRequest{
			Directory: urlFolder,
			Entry: &filer_pb.Entry{
				Name: f.Name(),
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   time.Now().Unix(),
					Mtime:    time.Now().Unix(),
					Gid:      uint32(os.Getgid()),
					Uid:      uint32(os.Getuid()),
					FileSize: uint64(fi.Size()),
					FileMode: uint32(fi.Mode()),
				},
				Chunks: chunks,
			},
		}

		fmt.Printf("%s%s set chunks: %v", urlFolder, f.Name(), len(chunks))
		for i, chunk := range chunks {
			fmt.Printf("%s%s chunks %d: %v [%d,%d)\n", urlFolder, f.Name(), i, chunk.FileId, chunk.Offset, chunk.Offset+int64(chunk.Size))
		}
		if _, err := client.CreateEntry(context.Background(), request); err != nil {
			return fmt.Errorf("update fh: %v", err)
		}
		return nil
	}); err != nil {
		fmt.Printf("upload data %v to http://%s%s%s: %v\n", f.Name(), filerUrl, urlFolder, f.Name(), err)
		return false
	}

	return true
}

func isGzipped(filename string) bool {
	return strings.ToLower(path.Ext(filename)) == ".gz"
}

func detectMimeType(f *os.File) string {
	head := make([]byte, 512)
	f.Seek(0, 0)
	n, err := f.Read(head)
	if err != nil {
		fmt.Printf("read head of %v: %v\n", f.Name(), err)
		return "application/octet-stream"
	}
	f.Seek(0, 0)
	mimeType := http.DetectContentType(head[:n])
	return mimeType
}

func withFilerClient(filerAddress string, fn func(filer_pb.SeaweedFilerClient) error) error {

	grpcConnection, err := grpc.Dial(filerAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", filerAddress, err)
	}
	defer grpcConnection.Close()

	client := filer_pb.NewSeaweedFilerClient(grpcConnection)

	return fn(client)
}
