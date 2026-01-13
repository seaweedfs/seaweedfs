package shell

import (
	"compress/gzip"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"

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

	The meta data will be saved into a local <filer_host>-<port>-<time>.meta.gz file.
	These meta data can be later loaded by fs.meta.load command

`
}

func (c *commandFsMetaSave) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsMetaSave) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsMetaSaveCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	verbose := fsMetaSaveCommand.Bool("v", false, "print out each processed files")
	outputFileName := fsMetaSaveCommand.String("o", "", "output the meta data to this file. If file name ends with .gz or .gzip, it will be gzip compressed")
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
		fileName = fmt.Sprintf("%s-%4d%02d%02d-%02d%02d%02d.meta.gz",
			commandEnv.option.FilerAddress.ToHttpAddress(), t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
	}

	var dst io.Writer

	f, openErr := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if openErr != nil {
		return fmt.Errorf("failed to create file %s: %v", fileName, openErr)
	}
	defer f.Close()

	dst = f

	if strings.HasSuffix(fileName, ".gz") || strings.HasSuffix(fileName, ".gzip") {
		gw := gzip.NewWriter(dst)
		defer func() {
			err1 := gw.Close()
			if err == nil {
				err = err1
			}
		}()

		dst = gw
	}

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
	}, func(outputChan chan interface{}) error {
		sizeBuf := make([]byte, 4)
		for item := range outputChan {
			b := item.([]byte)
			util.Uint32toBytes(sizeBuf, uint32(len(b)))
			_, err := dst.Write(sizeBuf)
			if err != nil {
				return err
			}
			_, err = dst.Write(b)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err == nil {
		fmt.Fprintf(writer, "meta data for http://%s%s is saved to %s\n", commandEnv.option.FilerAddress.ToHttpAddress(), path, fileName)
	}

	return err

}

func doTraverseBfsAndSaving(filerClient filer_pb.FilerClient, writer io.Writer, path string, verbose bool, genFn func(entry *filer_pb.FullEntry, outputChan chan interface{}) error, saveFn func(outputChan chan interface{}) error) error {

	var wg sync.WaitGroup
	wg.Add(1)
	outputChan := make(chan interface{}, 1024)
	saveErrChan := make(chan error, 1)
	go func() {
		saveErrChan <- saveFn(outputChan)
		wg.Done()
	}()

	var dirCount, fileCount uint64
	var once sync.Once
	var firstErr error
	var hasErr atomic.Bool

	// also save the directory itself (path) if it exists in the filer
	if e, getErr := filer_pb.GetEntry(context.Background(), filerClient, util.FullPath(path)); getErr != nil {
		// Entry not found is expected and can be ignored; log other errors.
		if !errors.Is(getErr, filer_pb.ErrNotFound) {
			fmt.Fprintf(writer, "failed to get entry %s: %v\n", path, getErr)
		}
	} else if e != nil {
		parentDir, _ := util.FullPath(path).DirAndName()
		protoMessage := &filer_pb.FullEntry{
			Dir:   parentDir,
			Entry: e,
		}
		if hasErr.Load() {
			// fail-fast: do not continue emitting partial results after first error
			return firstErr
		}
		if genErr := genFn(protoMessage, outputChan); genErr != nil {
			once.Do(func() {
				firstErr = genErr
				hasErr.Store(true)
			})
			return genErr
		} else {
			if e.IsDirectory {
				atomic.AddUint64(&dirCount, 1)
			} else {
				atomic.AddUint64(&fileCount, 1)
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := filer_pb.TraverseBfsWithContext(ctx, filerClient, util.FullPath(path), func(parentPath util.FullPath, entry *filer_pb.Entry) error {

		if strings.HasPrefix(string(parentPath), filer.SystemLogDir) {
			return nil
		}

		protoMessage := &filer_pb.FullEntry{
			Dir:   string(parentPath),
			Entry: entry,
		}

		if hasErr.Load() {
			// fail-fast: stop traversal once an error is observed.
			return firstErr
		}
		if genErr := genFn(protoMessage, outputChan); genErr != nil {
			once.Do(func() {
				firstErr = genErr
				hasErr.Store(true)
				cancel()
			})
			return genErr
		}

		if entry.IsDirectory {
			atomic.AddUint64(&dirCount, 1)
		} else {
			atomic.AddUint64(&fileCount, 1)
		}

		if verbose {
			println(parentPath.Child(entry.Name))
		}

		return nil
	})

	close(outputChan)

	wg.Wait()
	saveErr := <-saveErrChan

	if firstErr != nil {
		return firstErr
	}
	if saveErr != nil {
		return saveErr
	}

	if err == nil && writer != nil {
		fmt.Fprintf(writer, "total %d directories, %d files\n", dirCount, fileCount)
	}
	return err
}
