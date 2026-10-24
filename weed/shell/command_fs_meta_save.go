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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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

	fileName := util.ResolvePath(*outputFileName)
	if fileName == "" {
		t := time.Now()
		fileName = fmt.Sprintf("%s-%4d%02d%02d-%02d%02d%02d.meta.gz",
			commandEnv.FilerAddress().ToHttpAddress(), t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
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

	err = doTraverseBfsAndSaving(commandEnv, writer, path, *verbose, true, func(ctx context.Context, entry *filer_pb.FullEntry, outputChan chan interface{}) (err error) {
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

		select {
		case outputChan <- bytes:
		case <-ctx.Done():
			return ctx.Err()
		}
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
		fmt.Fprintf(writer, "meta data for http://%s%s is saved to %s\n", commandEnv.FilerAddress().ToHttpAddress(), path, fileName)
	}

	return err

}

// skipSystemLog excludes the metadata-log subtree (SystemLogDir) from the
// traversal. fs.meta.save sets it so the export doesn't carry the whole change
// log; volume.fsck and fs.verify must leave it false so the live log-file
// chunks are accounted for — otherwise fsck reports them as orphans and could
// delete referenced needles.
// doTraverseAndSaving feeds every entry at and below path through genFn and
// saveFn: from one snapshot of the filer store when the filer can provide it
// (so a file renamed during the traversal is not missed), by walking otherwise.
func doTraverseAndSaving(filerClient filer_pb.FilerClient, writer io.Writer, path string, verbose bool, skipSystemLog bool, genFn func(ctx context.Context, entry *filer_pb.FullEntry, outputChan chan interface{}) error, saveFn func(outputChan chan interface{}) error) error {
	if used, err := doTraverseSnapshotAndSaving(filerClient, writer, path, verbose, skipSystemLog, genFn, saveFn); used || err != nil {
		return err
	}
	return doTraverseBfsAndSaving(filerClient, writer, path, verbose, skipSystemLog, genFn, saveFn)
}

// doTraverseSnapshotAndSaving asks the filer to traverse path itself and
// consumes the stream only when the filer serves it from a store snapshot
// (filer_pb.TraverseModeHeaderKey). Otherwise it returns used=false having
// read nothing, and the caller walks as before.
func doTraverseSnapshotAndSaving(filerClient filer_pb.FilerClient, writer io.Writer, path string, verbose bool, skipSystemLog bool, genFn func(ctx context.Context, entry *filer_pb.FullEntry, outputChan chan interface{}) error, saveFn func(outputChan chan interface{}) error) (used bool, err error) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var excludedPrefixes []string
	if skipSystemLog {
		excludedPrefixes = append(excludedPrefixes, filer.SystemLogDir)
	}

	// The walk tolerates a path that does not exist (it saves nothing); the
	// filer-side traversal reports it as an error, so let the walk have it.
	// Any other failure must stop here: a walk that could then reach the filer
	// reads renamed-away entries as absent, and fsck would purge their chunks.
	if _, _, _, getErr := filer_pb.GetEntry(ctx, filerClient, util.FullPath(path)); getErr != nil {
		if errors.Is(getErr, filer_pb.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("traverse %s: %w", path, getErr)
	}

	err = filerClient.WithFilerClient(true, func(client filer_pb.SeaweedFilerClient) error {
		ctx := metadata.AppendToOutgoingContext(ctx, filer_pb.TraverseModeHeaderKey, filer_pb.TraverseModeSnapshot)
		stream, err := client.TraverseBfsMetadata(ctx, &filer_pb.TraverseBfsMetadataRequest{
			Directory:        path,
			ExcludedPrefixes: excludedPrefixes,
		})
		if err != nil {
			return err
		}
		header, err := stream.Header()
		if err != nil {
			if status.Code(err) == codes.Unimplemented {
				return nil // a filer without the RPC at all: walk
			}
			return fmt.Errorf("traverse %s: %w", path, err)
		}
		if modes := header.Get(filer_pb.TraverseModeHeaderKey); len(modes) == 0 || modes[0] != filer_pb.TraverseModeSnapshot {
			// An older filer (no header), a store that cannot scan ("bfs"), or
			// a stream that failed before any header, which Header() reports
			// as no header and nil: only Recv tells, and a failure is no
			// licence to walk instead.
			if _, recvErr := stream.Recv(); recvErr != nil && !errors.Is(recvErr, io.EOF) && status.Code(recvErr) != codes.Unimplemented {
				return fmt.Errorf("traverse %s: %w", path, recvErr)
			}
			return nil
		}
		used = true

		var wg sync.WaitGroup
		wg.Add(1)
		outputChan := make(chan interface{}, 1024)
		saveErrChan := make(chan error, 1)
		go func() {
			saveErr := saveFn(outputChan)
			saveErrChan <- saveErr
			if saveErr != nil {
				cancel()
			}
			wg.Done()
		}()

		var dirCount, fileCount uint64
		var recvErr error
		for {
			resp, err := stream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					recvErr = fmt.Errorf("traverse %s: %w", path, err)
				}
				break
			}
			if resp.Entry == nil {
				continue
			}
			protoMessage := &filer_pb.FullEntry{
				Dir:   resp.Directory,
				Entry: resp.Entry,
			}
			if genErr := genFn(ctx, protoMessage, outputChan); genErr != nil {
				recvErr = genErr
				cancel()
				break
			}
			if resp.Entry.IsDirectory {
				dirCount++
			} else {
				fileCount++
			}
			if verbose {
				println(util.NewFullPath(resp.Directory, resp.Entry.Name))
			}
		}

		close(outputChan)
		wg.Wait()
		saveErr := <-saveErrChan

		if recvErr != nil {
			return recvErr
		}
		if saveErr != nil {
			return saveErr
		}
		if writer != nil {
			fmt.Fprintf(writer, "collected filer entries from a consistent store snapshot\n")
			fmt.Fprintf(writer, "total %d directories, %d files\n", dirCount, fileCount)
		}
		return nil
	})
	return used, err
}

func doTraverseBfsAndSaving(filerClient filer_pb.FilerClient, writer io.Writer, path string, verbose bool, skipSystemLog bool, genFn func(ctx context.Context, entry *filer_pb.FullEntry, outputChan chan interface{}) error, saveFn func(outputChan chan interface{}) error) error {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	outputChan := make(chan interface{}, 1024)
	saveErrChan := make(chan error, 1)
	go func() {
		saveErr := saveFn(outputChan)
		saveErrChan <- saveErr
		if saveErr != nil {
			cancel()
		}
		wg.Done()
	}()

	var dirCount, fileCount uint64
	var once sync.Once
	var firstErr error
	var hasErr atomic.Bool

	// also save the directory itself (path) if it exists in the filer
	if e, _, _, getErr := filer_pb.GetEntry(ctx, filerClient, util.FullPath(path)); getErr != nil {
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

		if genErr := genFn(ctx, protoMessage, outputChan); genErr != nil {
			once.Do(func() {
				firstErr = genErr
				hasErr.Store(true)
			})
			close(outputChan)
			wg.Wait()
			<-saveErrChan
			return genErr
		} else {
			if e.IsDirectory {
				atomic.AddUint64(&dirCount, 1)
			} else {
				atomic.AddUint64(&fileCount, 1)
			}
		}
	}

	defer cancel()
	systemLogChildPrefix := filer.SystemLogDir + "/"
	err := filer_pb.TraverseBfs(ctx, filerClient, util.FullPath(path), func(parentPath util.FullPath, entry *filer_pb.Entry) error {

		if skipSystemLog {
			parent := string(parentPath)
			if parent == filer.SystemLogDir || strings.HasPrefix(parent, systemLogChildPrefix) {
				return nil
			}
		}

		protoMessage := &filer_pb.FullEntry{
			Dir:   string(parentPath),
			Entry: entry,
		}

		if hasErr.Load() {
			// fail-fast: stop traversal once an error is observed.
			return firstErr
		}
		if genErr := genFn(ctx, protoMessage, outputChan); genErr != nil {
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

	if err != nil {
		return err
	}
	if saveErr != nil {
		return saveErr
	}

	if writer != nil {
		fmt.Fprintf(writer, "total %d directories, %d files\n", dirCount, fileCount)
	}
	return nil
}
