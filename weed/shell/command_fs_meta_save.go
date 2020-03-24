package shell

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"

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

Another usage is to export all data chunk file ids used by the files.
	
	fs.meta.save -chunks <filer.chunks>

	The output chunks file will contain lines as:
		<file key> <tab> <volumeId, fileKey, cookie> <tab> <file name>

	This output chunks can be used to find out missing chunks or files.

`
}

func (c *commandFsMetaSave) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsMetaSaveCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	verbose := fsMetaSaveCommand.Bool("v", false, "print out each processed files")
	outputFileName := fsMetaSaveCommand.String("o", "", "output the meta data to this file")
	chunksFileName := fsMetaSaveCommand.String("chunks", "", "output all the chunks to this file")
	if err = fsMetaSaveCommand.Parse(args); err != nil {
		return nil
	}

	path, parseErr := commandEnv.parseUrl(findInputDirectory(fsMetaSaveCommand.Args()))
	if parseErr != nil {
		return parseErr
	}

	if *outputFileName != "" {
		fileName := *outputFileName
		if fileName == "" {
			t := time.Now()
			fileName = fmt.Sprintf("%s-%d-%4d%02d%02d-%02d%02d%02d.meta",
				commandEnv.option.FilerHost, commandEnv.option.FilerPort, t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
		}
		return doTraverseBfsAndSaving(fileName, commandEnv, writer, path, *verbose, func(dst io.Writer, outputChan chan []byte) {
			sizeBuf := make([]byte, 4)
			for b := range outputChan {
				util.Uint32toBytes(sizeBuf, uint32(len(b)))
				dst.Write(sizeBuf)
				dst.Write(b)
			}
		}, func(entry *filer_pb.FullEntry, outputChan chan []byte) (err error) {
			bytes, err := proto.Marshal(entry)
			if err != nil {
				fmt.Fprintf(writer, "marshall error: %v\n", err)
				return
			}

			outputChan <- bytes
			return nil
		})
	}

	if *chunksFileName != "" {
		return doTraverseBfsAndSaving(*chunksFileName, commandEnv, writer, path, *verbose, func(dst io.Writer, outputChan chan []byte) {
			for b := range outputChan {
				dst.Write(b)
			}
		}, func(entry *filer_pb.FullEntry, outputChan chan []byte) (err error) {
			for _, chunk := range entry.Entry.Chunks {
				dir := entry.Dir
				if dir == "/" {
					dir = ""
				}
				outputLine := fmt.Sprintf("%d\t%s\t%s/%s\n", chunk.Fid.FileKey, chunk.FileId, dir, entry.Entry.Name)
				outputChan <- []byte(outputLine)
			}
			return nil
		})
	}

	return err

}

func doTraverseBfsAndSaving(fileName string, commandEnv *CommandEnv, writer io.Writer, path string, verbose bool,
	saveFn func(dst io.Writer, outputChan chan []byte),
	genFn func(entry *filer_pb.FullEntry, outputChan chan []byte) error) error {

	dst, openErr := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if openErr != nil {
		return fmt.Errorf("failed to create file %s: %v", fileName, openErr)
	}
	defer dst.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	outputChan := make(chan []byte, 1024)
	go func() {
		saveFn(dst, outputChan)
		wg.Done()
	}()

	var dirCount, fileCount uint64

	err := doTraverseBfs(writer, commandEnv, util.FullPath(path), func(parentPath util.FullPath, entry *filer_pb.Entry) {

		protoMessage := &filer_pb.FullEntry{
			Dir:   string(parentPath),
			Entry: entry,
		}

		if err := genFn(protoMessage, outputChan); err != nil {
			fmt.Fprintf(writer, "marshall error: %v\n", err)
			return
		}

		if entry.IsDirectory {
			atomic.AddUint64(&dirCount, 1)
		} else {
			atomic.AddUint64(&fileCount, 1)
		}

		if verbose {
			println(parentPath.Child(entry.Name))
		}

	})

	close(outputChan)

	wg.Wait()

	if err == nil {
		fmt.Fprintf(writer, "total %d directories, %d files\n", dirCount, fileCount)
	}
	return err
}

func doTraverseBfs(writer io.Writer, filerClient filer_pb.FilerClient, parentPath util.FullPath, fn func(parentPath util.FullPath, entry *filer_pb.Entry)) (err error) {

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
				dir := t.(util.FullPath)
				processErr := processOneDirectory(writer, filerClient, dir, queue, &jobQueueWg, fn)
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

func processOneDirectory(writer io.Writer, filerClient filer_pb.FilerClient, parentPath util.FullPath, queue *util.Queue, jobQueueWg *sync.WaitGroup, fn func(parentPath util.FullPath, entry *filer_pb.Entry)) (err error) {

	return filer_pb.ReadDirAllEntries(filerClient, parentPath, "", func(entry *filer_pb.Entry, isLast bool) {

		fn(parentPath, entry)

		if entry.IsDirectory {
			subDir := fmt.Sprintf("%s/%s", parentPath, entry.Name)
			if parentPath == "/" {
				subDir = "/" + entry.Name
			}
			jobQueueWg.Add(1)
			queue.Enqueue(util.FullPath(subDir))
		}
	})

}
