package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandFsMetaSave{})
}

type commandFsMetaSave struct {
}

func (c *commandFsMetaSave) Name() string {
	return "fs.meta.save"
}

func (c *commandFsMetaSave) Help() string {
	return `save all directory and file meta data to a local file for metadata backup.

	fs.meta.save /               # save from the root
	fs.meta.save -v -o t.meta /  # save from the root, output to t.meta file.
	fs.meta.save /path/to/save   # save from the directory /path/to/save
	fs.meta.save .               # save from current directory
	fs.meta.save                 # save from current directory

	The meta data will be saved into a local <filer_host>-<port>-<time>.meta file.
	These meta data can be later loaded by fs.meta.load command, 

	This assumes there are no deletions, so this is different from taking a snapshot.

`
}

func (c *commandFsMetaSave) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsMetaSaveCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	verbose := fsMetaSaveCommand.Bool("v", false, "print out each processed files")
	outputFileName := fsMetaSaveCommand.String("o", "", "output the meta data to this file")
	if err = fsMetaSaveCommand.Parse(args); err != nil {
		return nil
	}

	filerServer, filerPort, path, parseErr := commandEnv.parseUrl(findInputDirectory(fsMetaSaveCommand.Args()))
	if parseErr != nil {
		return parseErr
	}

	ctx := context.Background()

	t := time.Now()
	fileName := *outputFileName
	if fileName == "" {
		fileName = fmt.Sprintf("%s-%d-%4d%02d%02d-%02d%02d%02d.meta",
			filerServer, filerPort, t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
	}

	dst, openErr := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if openErr != nil {
		return fmt.Errorf("failed to create file %s: %v", fileName, openErr)
	}
	defer dst.Close()

	var dirCount, fileCount uint64

	err = doTraverseBFS(ctx, writer, commandEnv.getFilerClient(filerServer, filerPort), filer2.FullPath(path), func(parentPath filer2.FullPath, entry *filer_pb.Entry) {

		protoMessage := &filer_pb.FullEntry{
			Dir:   string(parentPath),
			Entry: entry,
		}

		bytes, err := proto.Marshal(protoMessage)
		if err != nil {
			fmt.Fprintf(writer, "marshall error: %v\n", err)
			return
		}

		sizeBuf := make([]byte, 4)
		util.Uint32toBytes(sizeBuf, uint32(len(bytes)))

		dst.Write(sizeBuf)
		dst.Write(bytes)

		if entry.IsDirectory {
			atomic.AddUint64(&dirCount, 1)
		} else {
			atomic.AddUint64(&fileCount, 1)
		}

		if *verbose {
			println(parentPath.Child(entry.Name))
		}

	})

	if err == nil {
		fmt.Fprintf(writer, "\ntotal %d directories, %d files", dirCount, fileCount)
		fmt.Fprintf(writer, "\nmeta data for http://%s:%d%s is saved to %s\n", filerServer, filerPort, path, fileName)
	}

	return err

}
func doTraverseBFS(ctx context.Context, writer io.Writer, filerClient filer2.FilerClient,
	parentPath filer2.FullPath, fn func(parentPath filer2.FullPath, entry *filer_pb.Entry)) (err error) {

	K := 5

	var jobQueueWg sync.WaitGroup
	queue := util.NewQueue()
	jobQueueWg.Add(1)
	queue.Enqueue(parentPath)
	var isTerminating bool

	for i := 0; i < K; i++ {
		go func() {
			for {
				if isTerminating {
					break
				}
				t := queue.Dequeue()
				if t == nil {
					time.Sleep(329 * time.Millisecond)
					continue
				}
				dir := t.(filer2.FullPath)
				processErr := processOneDirectory(ctx, writer, filerClient, dir, queue, &jobQueueWg, fn)
				if processErr != nil {
					err = processErr
				}
				jobQueueWg.Done()
			}
		}()
	}
	jobQueueWg.Wait()
	isTerminating = true
	return
}

func processOneDirectory(ctx context.Context, writer io.Writer, filerClient filer2.FilerClient,
	parentPath filer2.FullPath, queue *util.Queue, jobQueueWg *sync.WaitGroup,
	fn func(parentPath filer2.FullPath, entry *filer_pb.Entry)) (err error) {

	return filer2.ReadDirAllEntries(ctx, filerClient, string(parentPath), "", func(entry *filer_pb.Entry, isLast bool) {

		fn(parentPath, entry)

		if entry.IsDirectory {
			subDir := fmt.Sprintf("%s/%s", parentPath, entry.Name)
			if parentPath == "/" {
				subDir = "/" + entry.Name
			}
			jobQueueWg.Add(1)
			queue.Enqueue(filer2.FullPath(subDir))
		}
	})

}
