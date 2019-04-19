package command

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	copy      CopyOptions
	waitGroup sync.WaitGroup
)

type CopyOptions struct {
	filerGrpcPort    *int
	master           *string
	include          *string
	replication      *string
	collection       *string
	ttl              *string
	maxMB            *int
	grpcDialOption   grpc.DialOption
	masterClient     *wdclient.MasterClient
	concurrency      *int
	compressionLevel *int
}

func init() {
	cmdCopy.Run = runCopy // break init cycle
	cmdCopy.IsDebug = cmdCopy.Flag.Bool("debug", false, "verbose debug information")
	copy.master = cmdCopy.Flag.String("master", "localhost:9333", "SeaweedFS master location")
	copy.include = cmdCopy.Flag.String("include", "", "pattens of files to copy, e.g., *.pdf, *.html, ab?d.txt, works together with -dir")
	copy.replication = cmdCopy.Flag.String("replication", "", "replication type")
	copy.collection = cmdCopy.Flag.String("collection", "", "optional collection name")
	copy.ttl = cmdCopy.Flag.String("ttl", "", "time to live, e.g.: 1m, 1h, 1d, 1M, 1y")
	copy.maxMB = cmdCopy.Flag.Int("maxMB", 32, "split files larger than the limit")
	copy.filerGrpcPort = cmdCopy.Flag.Int("filer.port.grpc", 0, "filer grpc server listen port, default to filer port + 10000")
	copy.concurrency = cmdCopy.Flag.Int("c", 8, "concurrent file copy goroutines")
	copy.compressionLevel = cmdCopy.Flag.Int("compressionLevel", 9, "local file compression level 1 ~ 9")
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

	copy.masterClient = wdclient.NewMasterClient(context.Background(), copy.grpcDialOption, "client", strings.Split(*copy.master, ","))
	go copy.masterClient.KeepConnectedToMaster()
	copy.masterClient.WaitUntilConnected()

	if *cmdCopy.IsDebug {
		util.SetupProfiling("filer.copy.cpu.pprof", "filer.copy.mem.pprof")
	}

	fileCopyTaskChan := make(chan FileCopyTask, *copy.concurrency)

	ctx := context.Background()

	go func() {
		defer close(fileCopyTaskChan)
		for _, fileOrDir := range fileOrDirs {
			if err := genFileCopyTask(fileOrDir, urlPath, fileCopyTaskChan); err != nil {
				fmt.Fprintf(os.Stderr, "gen file list error: %v\n", err)
				break
			}
		}
	}()
	for i := 0; i < *copy.concurrency; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			worker := FileCopyWorker{
				options:          &copy,
				filerHost:        filerUrl.Host,
				filerGrpcAddress: filerGrpcAddress,
			}
			if err := worker.copyFiles(ctx, fileCopyTaskChan); err != nil {
				fmt.Fprintf(os.Stderr, "copy file error: %v\n", err)
				return
			}
		}()
	}
	waitGroup.Wait()

	return true
}

func genFileCopyTask(fileOrDir string, destPath string, fileCopyTaskChan chan FileCopyTask) error {

	fi, err := os.Stat(fileOrDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get stat for file %s: %v\n", fileOrDir, err)
		return nil
	}

	mode := fi.Mode()
	if mode.IsDir() {
		files, _ := ioutil.ReadDir(fileOrDir)
		for _, subFileOrDir := range files {
			if err = genFileCopyTask(fileOrDir+"/"+subFileOrDir.Name(), destPath+fi.Name()+"/", fileCopyTaskChan); err != nil {
				return err
			}
		}
		return nil
	}

	uid, gid := util.GetFileUidGid(fi)

	fileCopyTaskChan <- FileCopyTask{
		sourceLocation:     fileOrDir,
		destinationUrlPath: destPath,
		fileSize:           fi.Size(),
		fileMode:           fi.Mode(),
		uid:                uid,
		gid:                gid,
	}

	return nil
}

type FileCopyWorker struct {
	options          *CopyOptions
	filerHost        string
	filerGrpcAddress string
}

func (worker *FileCopyWorker) copyFiles(ctx context.Context, fileCopyTaskChan chan FileCopyTask) error {
	for task := range fileCopyTaskChan {
		if err := worker.doEachCopy(ctx, task); err != nil {
			return err
		}
	}
	return nil
}

type FileCopyTask struct {
	sourceLocation     string
	destinationUrlPath string
	fileSize           int64
	fileMode           os.FileMode
	uid                uint32
	gid                uint32
}

func (worker *FileCopyWorker) doEachCopy(ctx context.Context, task FileCopyTask) error {

	f, err := os.Open(task.sourceLocation)
	if err != nil {
		fmt.Printf("Failed to open file %s: %v\n", task.sourceLocation, err)
		if _, ok := err.(*os.PathError); ok {
			fmt.Printf("skipping %s\n", task.sourceLocation)
			return nil
		}
		return err
	}
	defer f.Close()

	// this is a regular file
	if *worker.options.include != "" {
		if ok, _ := filepath.Match(*worker.options.include, filepath.Base(task.sourceLocation)); !ok {
			return nil
		}
	}

	// find the chunk count
	chunkSize := int64(*worker.options.maxMB * 1024 * 1024)
	chunkCount := 1
	if chunkSize > 0 && task.fileSize > chunkSize {
		chunkCount = int(task.fileSize/chunkSize) + 1
	}

	if chunkCount == 1 {
		return worker.uploadFileAsOne(ctx, task, f)
	}

	return worker.uploadFileInChunks(ctx, task, f, chunkCount, chunkSize)
}

func (worker *FileCopyWorker) uploadFileAsOne(ctx context.Context, task FileCopyTask, f *os.File) error {

	// upload the file content
	fileName := filepath.Base(f.Name())
	mimeType := detectMimeType(f)

	var chunks []*filer_pb.FileChunk

	if task.fileSize > 0 {

		// assign a volume
		assignResult, err := operation.Assign(worker.options.masterClient.GetMaster(), worker.options.grpcDialOption, &operation.VolumeAssignRequest{
			Count:       1,
			Replication: *worker.options.replication,
			Collection:  *worker.options.collection,
			Ttl:         *worker.options.ttl,
		})
		if err != nil {
			fmt.Printf("Failed to assign from %s: %v\n", *worker.options.master, err)
		}

		targetUrl := "http://" + assignResult.Url + "/" + assignResult.Fid

		uploadResult, err := operation.UploadWithLocalCompressionLevel(targetUrl, fileName, f, false, mimeType, nil, assignResult.Auth, *worker.options.compressionLevel)
		if err != nil {
			return fmt.Errorf("upload data %v to %s: %v\n", fileName, targetUrl, err)
		}
		if uploadResult.Error != "" {
			return fmt.Errorf("upload %v to %s result: %v\n", fileName, targetUrl, uploadResult.Error)
		}
		fmt.Printf("uploaded %s to %s\n", fileName, targetUrl)

		chunks = append(chunks, &filer_pb.FileChunk{
			FileId: assignResult.Fid,
			Offset: 0,
			Size:   uint64(uploadResult.Size),
			Mtime:  time.Now().UnixNano(),
			ETag:   uploadResult.ETag,
		})

		fmt.Printf("copied %s => http://%s%s%s\n", fileName, worker.filerHost, task.destinationUrlPath, fileName)
	}

	if err := withFilerClient(ctx, worker.filerGrpcAddress, worker.options.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.CreateEntryRequest{
			Directory: task.destinationUrlPath,
			Entry: &filer_pb.Entry{
				Name: fileName,
				Attributes: &filer_pb.FuseAttributes{
					Crtime:      time.Now().Unix(),
					Mtime:       time.Now().Unix(),
					Gid:         task.gid,
					Uid:         task.uid,
					FileSize:    uint64(task.fileSize),
					FileMode:    uint32(task.fileMode),
					Mime:        mimeType,
					Replication: *worker.options.replication,
					Collection:  *worker.options.collection,
					TtlSec:      int32(util.ParseInt(*worker.options.ttl, 0)),
				},
				Chunks: chunks,
			},
		}

		if _, err := client.CreateEntry(ctx, request); err != nil {
			return fmt.Errorf("update fh: %v", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("upload data %v to http://%s%s%s: %v\n", fileName, worker.filerHost, task.destinationUrlPath, fileName, err)
	}

	return nil
}

func (worker *FileCopyWorker) uploadFileInChunks(ctx context.Context, task FileCopyTask, f *os.File, chunkCount int, chunkSize int64) error {

	fileName := filepath.Base(f.Name())
	mimeType := detectMimeType(f)

	var chunks []*filer_pb.FileChunk

	for i := int64(0); i < int64(chunkCount); i++ {

		// assign a volume
		assignResult, err := operation.Assign(worker.options.masterClient.GetMaster(), worker.options.grpcDialOption, &operation.VolumeAssignRequest{
			Count:       1,
			Replication: *worker.options.replication,
			Collection:  *worker.options.collection,
			Ttl:         *worker.options.ttl,
		})
		if err != nil {
			fmt.Printf("Failed to assign from %s: %v\n", *worker.options.master, err)
		}

		targetUrl := "http://" + assignResult.Url + "/" + assignResult.Fid

		uploadResult, err := operation.Upload(targetUrl,
			fileName+"-"+strconv.FormatInt(i+1, 10),
			io.LimitReader(f, chunkSize),
			false, "application/octet-stream", nil, assignResult.Auth)
		if err != nil {
			return fmt.Errorf("upload data %v to %s: %v\n", fileName, targetUrl, err)
		}
		if uploadResult.Error != "" {
			return fmt.Errorf("upload %v to %s result: %v\n", fileName, targetUrl, uploadResult.Error)
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

	if err := withFilerClient(ctx, worker.filerGrpcAddress, worker.options.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.CreateEntryRequest{
			Directory: task.destinationUrlPath,
			Entry: &filer_pb.Entry{
				Name: fileName,
				Attributes: &filer_pb.FuseAttributes{
					Crtime:      time.Now().Unix(),
					Mtime:       time.Now().Unix(),
					Gid:         task.gid,
					Uid:         task.uid,
					FileSize:    uint64(task.fileSize),
					FileMode:    uint32(task.fileMode),
					Mime:        mimeType,
					Replication: *worker.options.replication,
					Collection:  *worker.options.collection,
					TtlSec:      int32(util.ParseInt(*worker.options.ttl, 0)),
				},
				Chunks: chunks,
			},
		}

		if _, err := client.CreateEntry(ctx, request); err != nil {
			return fmt.Errorf("update fh: %v", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("upload data %v to http://%s%s%s: %v\n", fileName, worker.filerHost, task.destinationUrlPath, fileName, err)
	}

	fmt.Printf("copied %s => http://%s%s%s\n", fileName, worker.filerHost, task.destinationUrlPath, fileName)

	return nil
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

func withFilerClient(ctx context.Context, filerAddress string, grpcDialOption grpc.DialOption, fn func(filer_pb.SeaweedFilerClient) error) error {

	return util.WithCachedGrpcClient(ctx, func(clientConn *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(clientConn)
		return fn(client)
	}, filerAddress, grpcDialOption)

}
