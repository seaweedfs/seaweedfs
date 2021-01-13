package command

import (
	"context"
	"fmt"
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

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/util/grace"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
)

var (
	copy      CopyOptions
	waitGroup sync.WaitGroup
)

type CopyOptions struct {
	include           *string
	replication       *string
	collection        *string
	ttl               *string
	maxMB             *int
	masterClient      *wdclient.MasterClient
	concurrenctFiles  *int
	concurrenctChunks *int
	grpcDialOption    grpc.DialOption
	masters           []string
	cipher            bool
	ttlSec            int32
}

func init() {
	cmdCopy.Run = runCopy // break init cycle
	cmdCopy.IsDebug = cmdCopy.Flag.Bool("debug", false, "verbose debug information")
	copy.include = cmdCopy.Flag.String("include", "", "pattens of files to copy, e.g., *.pdf, *.html, ab?d.txt, works together with -dir")
	copy.replication = cmdCopy.Flag.String("replication", "", "replication type")
	copy.collection = cmdCopy.Flag.String("collection", "", "optional collection name")
	copy.ttl = cmdCopy.Flag.String("ttl", "", "time to live, e.g.: 1m, 1h, 1d, 1M, 1y")
	copy.maxMB = cmdCopy.Flag.Int("maxMB", 32, "split files larger than the limit")
	copy.concurrenctFiles = cmdCopy.Flag.Int("c", 8, "concurrent file copy goroutines")
	copy.concurrenctChunks = cmdCopy.Flag.Int("concurrentChunks", 8, "concurrent chunk copy goroutines for each file")
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

	util.LoadConfiguration("security", false)

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
		fmt.Printf("The last argument should be a folder and end with \"/\"\n")
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
	filerGrpcAddress := fmt.Sprintf("%s:%d", filerUrl.Hostname(), filerGrpcPort)
	copy.grpcDialOption = security.LoadClientTLS(util.GetViper(), "grpc.client")

	masters, collection, replication, dirBuckets, maxMB, cipher, err := readFilerConfiguration(copy.grpcDialOption, filerGrpcAddress)
	if err != nil {
		fmt.Printf("read from filer %s: %v\n", filerGrpcAddress, err)
		return false
	}
	if strings.HasPrefix(urlPath, dirBuckets+"/") {
		restPath := urlPath[len(dirBuckets)+1:]
		if strings.Index(restPath, "/") > 0 {
			expectedBucket := restPath[:strings.Index(restPath, "/")]
			if *copy.collection == "" {
				*copy.collection = expectedBucket
			} else if *copy.collection != expectedBucket {
				fmt.Printf("destination %s uses collection \"%s\": unexpected collection \"%v\"\n", urlPath, expectedBucket, *copy.collection)
				return true
			}
		}
	}
	if *copy.collection == "" {
		*copy.collection = collection
	}
	if *copy.replication == "" {
		*copy.replication = replication
	}
	if *copy.maxMB == 0 {
		*copy.maxMB = int(maxMB)
	}
	copy.masters = masters
	copy.cipher = cipher

	ttl, err := needle.ReadTTL(*copy.ttl)
	if err != nil {
		fmt.Printf("parsing ttl %s: %v\n", *copy.ttl, err)
		return false
	}
	copy.ttlSec = int32(ttl.Minutes()) * 60

	if *cmdCopy.IsDebug {
		grace.SetupProfiling("filer.copy.cpu.pprof", "filer.copy.mem.pprof")
	}

	fileCopyTaskChan := make(chan FileCopyTask, *copy.concurrenctFiles)

	go func() {
		defer close(fileCopyTaskChan)
		for _, fileOrDir := range fileOrDirs {
			if err := genFileCopyTask(fileOrDir, urlPath, fileCopyTaskChan); err != nil {
				fmt.Fprintf(os.Stderr, "genFileCopyTask : %v\n", err)
				break
			}
		}
	}()
	for i := 0; i < *copy.concurrenctFiles; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			worker := FileCopyWorker{
				options:          &copy,
				filerHost:        filerUrl.Host,
				filerGrpcAddress: filerGrpcAddress,
			}
			if err := worker.copyFiles(fileCopyTaskChan); err != nil {
				fmt.Fprintf(os.Stderr, "copy file error: %v\n", err)
				return
			}
		}()
	}
	waitGroup.Wait()

	return true
}

func readFilerConfiguration(grpcDialOption grpc.DialOption, filerGrpcAddress string) (masters []string, collection, replication string, dirBuckets string, maxMB uint32, cipher bool, err error) {
	err = pb.WithGrpcFilerClient(filerGrpcAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get filer %s configuration: %v", filerGrpcAddress, err)
		}
		masters, collection, replication, maxMB = resp.Masters, resp.Collection, resp.Replication, resp.MaxMb
		dirBuckets = resp.DirBuckets
		cipher = resp.Cipher
		return nil
	})
	return
}

func genFileCopyTask(fileOrDir string, destPath string, fileCopyTaskChan chan FileCopyTask) error {

	fi, err := os.Stat(fileOrDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: read file %s: %v\n", fileOrDir, err)
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

func (worker *FileCopyWorker) copyFiles(fileCopyTaskChan chan FileCopyTask) error {
	for task := range fileCopyTaskChan {
		if err := worker.doEachCopy(task); err != nil {
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

func (worker *FileCopyWorker) doEachCopy(task FileCopyTask) error {

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
		return worker.uploadFileAsOne(task, f)
	}

	return worker.uploadFileInChunks(task, f, chunkCount, chunkSize)
}

func (worker *FileCopyWorker) uploadFileAsOne(task FileCopyTask, f *os.File) error {

	// upload the file content
	fileName := filepath.Base(f.Name())
	mimeType := detectMimeType(f)
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}

	var chunks []*filer_pb.FileChunk
	var assignResult *filer_pb.AssignVolumeResponse
	var assignError error

	if task.fileSize > 0 {

		// assign a volume
		err := pb.WithGrpcFilerClient(worker.filerGrpcAddress, worker.options.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {

			request := &filer_pb.AssignVolumeRequest{
				Count:       1,
				Replication: *worker.options.replication,
				Collection:  *worker.options.collection,
				TtlSec:      worker.options.ttlSec,
				Path:        task.destinationUrlPath,
			}

			assignResult, assignError = client.AssignVolume(context.Background(), request)
			if assignError != nil {
				return fmt.Errorf("assign volume failure %v: %v", request, assignError)
			}
			if assignResult.Error != "" {
				return fmt.Errorf("assign volume failure %v: %v", request, assignResult.Error)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("Failed to assign from %v: %v\n", worker.options.masters, err)
		}

		targetUrl := "http://" + assignResult.Url + "/" + assignResult.FileId

		uploadResult, err := operation.UploadData(targetUrl, fileName, worker.options.cipher, data, false, mimeType, nil, security.EncodedJwt(assignResult.Auth))
		if err != nil {
			return fmt.Errorf("upload data %v to %s: %v\n", fileName, targetUrl, err)
		}
		if uploadResult.Error != "" {
			return fmt.Errorf("upload %v to %s result: %v\n", fileName, targetUrl, uploadResult.Error)
		}
		fmt.Printf("uploaded %s to %s\n", fileName, targetUrl)

		chunks = append(chunks, uploadResult.ToPbFileChunk(assignResult.FileId, 0))

		fmt.Printf("copied %s => http://%s%s%s\n", fileName, worker.filerHost, task.destinationUrlPath, fileName)
	}

	if err := pb.WithGrpcFilerClient(worker.filerGrpcAddress, worker.options.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
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
					TtlSec:      worker.options.ttlSec,
				},
				Chunks: chunks,
			},
		}

		if err := filer_pb.CreateEntry(client, request); err != nil {
			return fmt.Errorf("update fh: %v", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("upload data %v to http://%s%s%s: %v\n", fileName, worker.filerHost, task.destinationUrlPath, fileName, err)
	}

	return nil
}

func (worker *FileCopyWorker) uploadFileInChunks(task FileCopyTask, f *os.File, chunkCount int, chunkSize int64) error {

	fileName := filepath.Base(f.Name())
	mimeType := detectMimeType(f)

	chunksChan := make(chan *filer_pb.FileChunk, chunkCount)

	concurrentChunks := make(chan struct{}, *worker.options.concurrenctChunks)
	var wg sync.WaitGroup
	var uploadError error
	var collection, replication string

	fmt.Printf("uploading %s in %d chunks ...\n", fileName, chunkCount)
	for i := int64(0); i < int64(chunkCount) && uploadError == nil; i++ {
		wg.Add(1)
		concurrentChunks <- struct{}{}
		go func(i int64) {
			defer func() {
				wg.Done()
				<-concurrentChunks
			}()
			// assign a volume
			var assignResult *filer_pb.AssignVolumeResponse
			var assignError error
			err := pb.WithGrpcFilerClient(worker.filerGrpcAddress, worker.options.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
				request := &filer_pb.AssignVolumeRequest{
					Count:       1,
					Replication: *worker.options.replication,
					Collection:  *worker.options.collection,
					TtlSec:      worker.options.ttlSec,
					Path:        task.destinationUrlPath + fileName,
				}

				assignResult, assignError = client.AssignVolume(context.Background(), request)
				if assignError != nil {
					return fmt.Errorf("assign volume failure %v: %v", request, assignError)
				}
				if assignResult.Error != "" {
					return fmt.Errorf("assign volume failure %v: %v", request, assignResult.Error)
				}
				return nil
			})
			if err != nil {
				fmt.Printf("Failed to assign from %v: %v\n", worker.options.masters, err)
			}
			if err != nil {
				fmt.Printf("Failed to assign from %v: %v\n", worker.options.masters, err)
			}

			targetUrl := "http://" + assignResult.Url + "/" + assignResult.FileId
			if collection == "" {
				collection = assignResult.Collection
			}
			if replication == "" {
				replication = assignResult.Replication
			}

			uploadResult, err, _ := operation.Upload(targetUrl, fileName+"-"+strconv.FormatInt(i+1, 10), worker.options.cipher, io.NewSectionReader(f, i*chunkSize, chunkSize), false, "", nil, security.EncodedJwt(assignResult.Auth))
			if err != nil {
				uploadError = fmt.Errorf("upload data %v to %s: %v\n", fileName, targetUrl, err)
				return
			}
			if uploadResult.Error != "" {
				uploadError = fmt.Errorf("upload %v to %s result: %v\n", fileName, targetUrl, uploadResult.Error)
				return
			}
			chunksChan <- uploadResult.ToPbFileChunk(assignResult.FileId, i*chunkSize)

			fmt.Printf("uploaded %s-%d to %s [%d,%d)\n", fileName, i+1, targetUrl, i*chunkSize, i*chunkSize+int64(uploadResult.Size))
		}(i)
	}
	wg.Wait()
	close(chunksChan)

	var chunks []*filer_pb.FileChunk
	for chunk := range chunksChan {
		chunks = append(chunks, chunk)
	}

	if uploadError != nil {
		var fileIds []string
		for _, chunk := range chunks {
			fileIds = append(fileIds, chunk.FileId)
		}
		operation.DeleteFiles(copy.masters[0], false, worker.options.grpcDialOption, fileIds)
		return uploadError
	}

	if err := pb.WithGrpcFilerClient(worker.filerGrpcAddress, worker.options.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
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
					Replication: replication,
					Collection:  collection,
					TtlSec:      worker.options.ttlSec,
				},
				Chunks: chunks,
			},
		}

		if err := filer_pb.CreateEntry(client, request); err != nil {
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
		return ""
	}
	f.Seek(0, io.SeekStart)
	mimeType := http.DetectContentType(head[:n])
	if mimeType == "application/octet-stream" {
		return ""
	}
	return mimeType
}
