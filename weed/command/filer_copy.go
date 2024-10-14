package command

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
)

var (
	copy      CopyOptions
	waitGroup sync.WaitGroup
)

type CopyOptions struct {
	include            *string
	replication        *string
	collection         *string
	ttl                *string
	diskType           *string
	maxMB              *int
	concurrentFiles    *int
	concurrentChunks   *int
	grpcDialOption     grpc.DialOption
	masters            []string
	cipher             bool
	ttlSec             int32
	checkSize          *bool
	verbose            *bool
	volumeServerAccess *string
}

func init() {
	cmdFilerCopy.Run = runCopy // break init cycle
	cmdFilerCopy.IsDebug = cmdFilerCopy.Flag.Bool("debug", false, "verbose debug information")
	copy.include = cmdFilerCopy.Flag.String("include", "", "pattens of files to copy, e.g., *.pdf, *.html, ab?d.txt, works together with -dir")
	copy.replication = cmdFilerCopy.Flag.String("replication", "", "replication type")
	copy.collection = cmdFilerCopy.Flag.String("collection", "", "optional collection name")
	copy.ttl = cmdFilerCopy.Flag.String("ttl", "", "time to live, e.g.: 1m, 1h, 1d, 1M, 1y")
	copy.diskType = cmdFilerCopy.Flag.String("disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	copy.maxMB = cmdFilerCopy.Flag.Int("maxMB", 4, "split files larger than the limit")
	copy.concurrentFiles = cmdFilerCopy.Flag.Int("c", 8, "concurrent file copy goroutines")
	copy.concurrentChunks = cmdFilerCopy.Flag.Int("concurrentChunks", 8, "concurrent chunk copy goroutines for each file")
	copy.checkSize = cmdFilerCopy.Flag.Bool("check.size", false, "copy when the target file size is different from the source file")
	copy.verbose = cmdFilerCopy.Flag.Bool("verbose", false, "print out details during copying")
	copy.volumeServerAccess = cmdFilerCopy.Flag.String("volumeServerAccess", "direct", "access volume servers by [direct|publicUrl]")
}

var cmdFilerCopy = &Command{
	UsageLine: "filer.copy file_or_dir1 [file_or_dir2 file_or_dir3] http://localhost:8888/path/to/a/folder/",
	Short:     "copy one or a list of files to a filer folder",
	Long: `copy one or a list of files, or batch copy one whole folder recursively, to a filer folder

  It can copy one or a list of files or folders.

  If copying a whole folder recursively:
  All files under the folder and sub folders will be copied.
  Optional parameter "-include" allows you to specify the file name patterns.

  If "maxMB" is set to a positive number, files larger than it would be split into chunks.

`,
}

func runCopy(cmd *Command, args []string) bool {

	util.LoadSecurityConfiguration()

	if len(args) <= 1 {
		return false
	}
	filerDestination := args[len(args)-1]
	fileOrDirs := args[0 : len(args)-1]

	filerAddress, urlPath, err := pb.ParseUrl(filerDestination)
	if err != nil {
		fmt.Printf("The last argument should be a URL on filer: %v\n", err)
		return false
	}
	if !strings.HasSuffix(urlPath, "/") {
		fmt.Printf("The last argument should be a folder and end with \"/\"\n")
		return false
	}

	copy.grpcDialOption = security.LoadClientTLS(util.GetViper(), "grpc.client")

	masters, collection, replication, dirBuckets, maxMB, cipher, err := readFilerConfiguration(copy.grpcDialOption, filerAddress)
	if err != nil {
		fmt.Printf("read from filer %s: %v\n", filerAddress, err)
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

	if *cmdFilerCopy.IsDebug {
		grace.SetupProfiling("filer.copy.cpu.pprof", "filer.copy.mem.pprof")
	}

	fileCopyTaskChan := make(chan FileCopyTask, *copy.concurrentFiles)

	go func() {
		defer close(fileCopyTaskChan)
		for _, fileOrDir := range fileOrDirs {
			if err := genFileCopyTask(fileOrDir, urlPath, fileCopyTaskChan); err != nil {
				fmt.Fprintf(os.Stderr, "genFileCopyTask : %v\n", err)
				break
			}
		}
	}()
	for i := 0; i < *copy.concurrentFiles; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			worker := FileCopyWorker{
				options:      &copy,
				filerAddress: filerAddress,
				signature:    util.RandomInt32(),
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

func readFilerConfiguration(grpcDialOption grpc.DialOption, filerGrpcAddress pb.ServerAddress) (masters []string, collection, replication string, dirBuckets string, maxMB uint32, cipher bool, err error) {
	err = pb.WithGrpcFilerClient(false, 0, filerGrpcAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
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
	uid, gid := util.GetFileUidGid(fi)
	fileSize := fi.Size()
	if mode.IsDir() {
		fileSize = 0
	}

	fileCopyTaskChan <- FileCopyTask{
		sourceLocation:     fileOrDir,
		destinationUrlPath: destPath,
		fileSize:           fileSize,
		fileMode:           fi.Mode(),
		uid:                uid,
		gid:                gid,
	}

	if mode.IsDir() {
		files, _ := os.ReadDir(fileOrDir)
		for _, subFileOrDir := range files {
			cleanedDestDirectory := destPath + fi.Name()
			if err = genFileCopyTask(fileOrDir+"/"+subFileOrDir.Name(), cleanedDestDirectory+"/", fileCopyTaskChan); err != nil {
				return err
			}
		}
	}

	return nil
}

type FileCopyWorker struct {
	options      *CopyOptions
	filerAddress pb.ServerAddress
	signature    int32
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

	if shouldCopy, err := worker.checkExistingFileFirst(task, f); err != nil {
		return fmt.Errorf("check existing file: %v", err)
	} else if !shouldCopy {
		if *worker.options.verbose {
			fmt.Printf("skipping copied file: %v\n", f.Name())
		}
		return nil
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

func (worker *FileCopyWorker) checkExistingFileFirst(task FileCopyTask, f *os.File) (shouldCopy bool, err error) {

	shouldCopy = true

	if !*worker.options.checkSize {
		return
	}

	fileStat, err := f.Stat()
	if err != nil {
		shouldCopy = false
		return
	}

	err = pb.WithGrpcFilerClient(false, worker.signature, worker.filerAddress, worker.options.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: task.destinationUrlPath,
			Name:      filepath.Base(f.Name()),
		}

		resp, lookupErr := client.LookupDirectoryEntry(context.Background(), request)
		if lookupErr != nil {
			// mostly not found error
			return nil
		}

		if fileStat.Size() == int64(filer.FileSize(resp.Entry)) {
			shouldCopy = false
		}

		return nil
	})
	return
}

func (worker *FileCopyWorker) uploadFileAsOne(task FileCopyTask, f *os.File) error {

	// upload the file content
	fileName := filepath.Base(f.Name())
	var mimeType string

	var chunks []*filer_pb.FileChunk

	if task.fileMode&os.ModeDir == 0 && task.fileSize > 0 {

		mimeType = detectMimeType(f)
		data, err := io.ReadAll(f)
		if err != nil {
			return err
		}

		uploader, uploaderErr := operation.NewUploader()
		if uploaderErr != nil {
			return uploaderErr
		}

		finalFileId, uploadResult, flushErr, _ := uploader.UploadWithRetry(
			worker,
			&filer_pb.AssignVolumeRequest{
				Count:       1,
				Replication: *worker.options.replication,
				Collection:  *worker.options.collection,
				TtlSec:      worker.options.ttlSec,
				DiskType:    *worker.options.diskType,
				Path:        task.destinationUrlPath,
			},
			&operation.UploadOption{
				Filename:          fileName,
				Cipher:            worker.options.cipher,
				IsInputCompressed: false,
				MimeType:          mimeType,
				PairMap:           nil,
			},
			func(host, fileId string) string {
				return fmt.Sprintf("http://%s/%s", host, fileId)
			},
			util.NewBytesReader(data),
		)
		if flushErr != nil {
			return flushErr
		}
		chunks = append(chunks, uploadResult.ToPbFileChunk(finalFileId, 0, time.Now().UnixNano()))
	}

	if err := pb.WithGrpcFilerClient(false, worker.signature, worker.filerAddress, worker.options.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.CreateEntryRequest{
			Directory: task.destinationUrlPath,
			Entry: &filer_pb.Entry{
				Name: fileName,
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   time.Now().Unix(),
					Mtime:    time.Now().Unix(),
					Gid:      task.gid,
					Uid:      task.uid,
					FileSize: uint64(task.fileSize),
					FileMode: uint32(task.fileMode),
					Mime:     mimeType,
					TtlSec:   worker.options.ttlSec,
				},
				Chunks: chunks,
			},
		}

		if err := filer_pb.CreateEntry(client, request); err != nil {
			return fmt.Errorf("update fh: %v", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("upload data %v to http://%s%s%s: %v\n", fileName, worker.filerAddress.ToHttpAddress(), task.destinationUrlPath, fileName, err)
	}

	return nil
}

func (worker *FileCopyWorker) uploadFileInChunks(task FileCopyTask, f *os.File, chunkCount int, chunkSize int64) error {

	fileName := filepath.Base(f.Name())
	mimeType := detectMimeType(f)

	chunksChan := make(chan *filer_pb.FileChunk, chunkCount)

	concurrentChunks := make(chan struct{}, *worker.options.concurrentChunks)
	var wg sync.WaitGroup
	var uploadError error

	fmt.Printf("uploading %s in %d chunks ...\n", fileName, chunkCount)
	for i := int64(0); i < int64(chunkCount) && uploadError == nil; i++ {
		wg.Add(1)
		concurrentChunks <- struct{}{}
		go func(i int64) {
			defer func() {
				wg.Done()
				<-concurrentChunks
			}()

			uploader, err := operation.NewUploader()
			if err != nil {
				uploadError = fmt.Errorf("upload data %v: %v\n", fileName, err)
				return
			}

			fileId, uploadResult, err, _ := uploader.UploadWithRetry(
				worker,
				&filer_pb.AssignVolumeRequest{
					Count:       1,
					Replication: *worker.options.replication,
					Collection:  *worker.options.collection,
					TtlSec:      worker.options.ttlSec,
					DiskType:    *worker.options.diskType,
					Path:        task.destinationUrlPath + fileName,
				},
				&operation.UploadOption{
					Filename:          fileName + "-" + strconv.FormatInt(i+1, 10),
					Cipher:            worker.options.cipher,
					IsInputCompressed: false,
					MimeType:          "",
					PairMap:           nil,
				},
				func(host, fileId string) string {
					return fmt.Sprintf("http://%s/%s", host, fileId)
				},
				io.NewSectionReader(f, i*chunkSize, chunkSize),
			)

			if err != nil {
				uploadError = fmt.Errorf("upload data %v: %v\n", fileName, err)
				return
			}
			if uploadResult.Error != "" {
				uploadError = fmt.Errorf("upload %v result: %v\n", fileName, uploadResult.Error)
				return
			}
			chunksChan <- uploadResult.ToPbFileChunk(fileId, i*chunkSize, time.Now().UnixNano())

			fmt.Printf("uploaded %s-%d [%d,%d)\n", fileName, i+1, i*chunkSize, i*chunkSize+int64(uploadResult.Size))
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
		operation.DeleteFileIds(func(_ context.Context) pb.ServerAddress {
			return pb.ServerAddress(copy.masters[0])
		}, false, worker.options.grpcDialOption, fileIds)
		return uploadError
	}

	manifestedChunks, manifestErr := filer.MaybeManifestize(worker.saveDataAsChunk, chunks)
	if manifestErr != nil {
		return fmt.Errorf("create manifest: %v", manifestErr)
	}

	if err := pb.WithGrpcFilerClient(false, worker.signature, worker.filerAddress, worker.options.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.CreateEntryRequest{
			Directory: task.destinationUrlPath,
			Entry: &filer_pb.Entry{
				Name: fileName,
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   time.Now().Unix(),
					Mtime:    time.Now().Unix(),
					Gid:      task.gid,
					Uid:      task.uid,
					FileSize: uint64(task.fileSize),
					FileMode: uint32(task.fileMode),
					Mime:     mimeType,
					TtlSec:   worker.options.ttlSec,
				},
				Chunks: manifestedChunks,
			},
		}

		if err := filer_pb.CreateEntry(client, request); err != nil {
			return fmt.Errorf("update fh: %v", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("upload data %v to http://%s%s%s: %v\n", fileName, worker.filerAddress.ToHttpAddress(), task.destinationUrlPath, fileName, err)
	}

	fmt.Printf("copied %s => http://%s%s%s\n", f.Name(), worker.filerAddress.ToHttpAddress(), task.destinationUrlPath, fileName)

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

func (worker *FileCopyWorker) saveDataAsChunk(reader io.Reader, name string, offset int64, tsNs int64) (chunk *filer_pb.FileChunk, err error) {
	uploader, uploaderErr := operation.NewUploader()
	if uploaderErr != nil {
		return nil, fmt.Errorf("upload data: %v", uploaderErr)
	}

	finalFileId, uploadResult, flushErr, _ := uploader.UploadWithRetry(
		worker,
		&filer_pb.AssignVolumeRequest{
			Count:       1,
			Replication: *worker.options.replication,
			Collection:  *worker.options.collection,
			TtlSec:      worker.options.ttlSec,
			DiskType:    *worker.options.diskType,
			Path:        name,
		},
		&operation.UploadOption{
			Filename:          name,
			Cipher:            worker.options.cipher,
			IsInputCompressed: false,
			MimeType:          "",
			PairMap:           nil,
		},
		func(host, fileId string) string {
			return fmt.Sprintf("http://%s/%s", host, fileId)
		},
		reader,
	)

	if flushErr != nil {
		return nil, fmt.Errorf("upload data: %v", flushErr)
	}
	if uploadResult.Error != "" {
		return nil, fmt.Errorf("upload result: %v", uploadResult.Error)
	}
	return uploadResult.ToPbFileChunk(finalFileId, offset, tsNs), nil
}

var _ = filer_pb.FilerClient(&FileCopyWorker{})

func (worker *FileCopyWorker) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) (err error) {

	filerGrpcAddress := worker.filerAddress.ToGrpcAddress()
	err = pb.WithGrpcClient(streamingMode, worker.signature, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, filerGrpcAddress, false, worker.options.grpcDialOption)

	return
}

func (worker *FileCopyWorker) AdjustedUrl(location *filer_pb.Location) string {
	if *worker.options.volumeServerAccess == "publicUrl" {
		return location.PublicUrl
	}
	return location.Url
}

func (worker *FileCopyWorker) GetDataCenter() string {
	return ""
}
