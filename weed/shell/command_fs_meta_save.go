package shell

import (
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
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
	These meta data can be later loaded by fs.meta.load command

`
}

func (c *commandFsMetaSave) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsMetaSave) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsMetaSaveCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	verbose := fsMetaSaveCommand.Bool("v", false, "print out each processed files")
	outputFileName := fsMetaSaveCommand.String("o", "", "output the meta data to this file")
	isObfuscate := fsMetaSaveCommand.Bool("obfuscate", false, "obfuscate the file names")
	// chunksFileName := fsMetaSaveCommand.String("chunks", "", "output all the chunks to this file")
	if err = fsMetaSaveCommand.Parse(args); err != nil {
		return err
	}

	path, parseErr := commandEnv.parseUrl(findInputDirectory(fsMetaSaveCommand.Args()))
	if parseErr != nil {
		return parseErr
	}

	fileName := *outputFileName
	if fileName == "" {
		t := time.Now()
		fileName = fmt.Sprintf("%s-%4d%02d%02d-%02d%02d%02d.meta",
			commandEnv.option.FilerAddress.ToHttpAddress(), t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
	}

	dst, openErr := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if openErr != nil {
		return fmt.Errorf("failed to create file %s: %v", fileName, openErr)
	}
	defer dst.Close()

	var cipherKey util.CipherKey
	if *isObfuscate {
		cipherKey = util.GenCipherKey()
	}

	err = doTraverseBfsAndSaving(commandEnv, writer, path, *verbose, func(entry *filer_pb.FullEntry, outputChan chan interface{}) (err error) {
		if !entry.Entry.IsDirectory {
			ext := filepath.Ext(entry.Entry.Name)
			if encrypted, encErr := util.Encrypt([]byte(entry.Entry.Name), cipherKey); encErr == nil {
				entry.Entry.Name = util.Base64Encode(encrypted)[:len(entry.Entry.Name)] + ext
				entry.Entry.Name = strings.ReplaceAll(entry.Entry.Name, "/", "x")
			}
		}
		bytes, err := proto.Marshal(entry)
		if err != nil {
			fmt.Fprintf(writer, "marshall error: %v\n", err)
			return
		}

		outputChan <- bytes
		return nil
	}, func(outputChan chan interface{}) {
		sizeBuf := make([]byte, 4)
		for item := range outputChan {
			b := item.([]byte)
			util.Uint32toBytes(sizeBuf, uint32(len(b)))
			dst.Write(sizeBuf)
			dst.Write(b)
		}
	})

	if err == nil {
		fmt.Fprintf(writer, "meta data for http://%s%s is saved to %s\n", commandEnv.option.FilerAddress.ToHttpAddress(), path, fileName)
	}

	return err

}

func doTraverseBfsAndSaving(filerClient filer_pb.FilerClient, writer io.Writer, path string, verbose bool, genFn func(entry *filer_pb.FullEntry, outputChan chan interface{}) error, saveFn func(outputChan chan interface{})) error {

	var wg sync.WaitGroup
	wg.Add(1)
	outputChan := make(chan interface{}, 1024)
	go func() {
		saveFn(outputChan)
		wg.Done()
	}()

	var dirCount, fileCount uint64

	err := filer_pb.TraverseBfs(filerClient, util.FullPath(path), func(parentPath util.FullPath, entry *filer_pb.Entry) {

		if strings.HasPrefix(string(parentPath), filer.SystemLogDir) {
			return
		}

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

	if err == nil && writer != nil {
		fmt.Fprintf(writer, "total %d directories, %d files\n", dirCount, fileCount)
	}
	return err
}
