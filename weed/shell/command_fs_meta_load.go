package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandFsMetaLoad{})
}

type commandFsMetaLoad struct {
	dirPrefix *string
}

func (c *commandFsMetaLoad) Name() string {
	return "fs.meta.load"
}

func (c *commandFsMetaLoad) Help() string {
	return `load saved filer meta data to restore the directory and file structure

	fs.meta.load <filer_host>-<port>-<time>.meta
	fs.meta.load -v=false <filer_host>-<port>-<time>.meta // skip printing out the verbose output
 	fs.meta.load -concurrency=1 <filer_host>-<port>-<time>.meta // number of parallel meta load to filer
	fs.meta.load -dirPrefix=/buckets/important <filer_host>.meta // load any dirs with prefix "important"

`
}

func (c *commandFsMetaLoad) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsMetaLoad) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if len(args) == 0 {
		fmt.Fprintf(writer, "missing a metadata file\n")
		return nil
	}

	fileName := args[len(args)-1]

	metaLoadCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	c.dirPrefix = metaLoadCommand.String("dirPrefix", "", "load entries only with directories matching prefix")
	concurrency := metaLoadCommand.Int("concurrency", 1, "number of parallel meta load to filer")
	verbose := metaLoadCommand.Bool("v", true, "verbose mode")
	if err = metaLoadCommand.Parse(args[0 : len(args)-1]); err != nil {
		return nil
	}

	dst, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return nil
	}
	defer dst.Close()

	var dirCount, fileCount uint64
	lastLogTime := time.Now()

	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		sizeBuf := make([]byte, 4)
		waitChan := make(chan struct{}, *concurrency)
		defer close(waitChan)
		var wg sync.WaitGroup

		for {
			if n, err := dst.Read(sizeBuf); n != 4 {
				if err == io.EOF {
					return nil
				}
				return err
			}

			size := util.BytesToUint32(sizeBuf)

			data := make([]byte, int(size))

			if n, err := dst.Read(data); n != len(data) {
				return err
			}

			fullEntry := &filer_pb.FullEntry{}
			if err = proto.Unmarshal(data, fullEntry); err != nil {
				return err
			}

			// check collection name pattern
			entryFullName := string(util.FullPath(fullEntry.Dir).Child(fullEntry.Entry.Name))
			if *c.dirPrefix != "" {
				if !strings.HasPrefix(fullEntry.Dir, *c.dirPrefix) {
					if *verbose {
						fmt.Fprintf(writer, "not match dir prefix %s\n", entryFullName)
					}
					continue
				}
			}

			if *verbose || lastLogTime.Add(time.Second).Before(time.Now()) {
				if !*verbose {
					lastLogTime = time.Now()
				}
				fmt.Fprintf(writer, "load %s\n", entryFullName)
			}

			fullEntry.Entry.Name = strings.ReplaceAll(fullEntry.Entry.Name, "/", "x")
			if fullEntry.Entry.IsDirectory {
				wg.Wait()
				if errEntry := filer_pb.CreateEntry(context.Background(), client, &filer_pb.CreateEntryRequest{
					Directory: fullEntry.Dir,
					Entry:     fullEntry.Entry,
				}); errEntry != nil {
					return errEntry
				}
				dirCount++
			} else {
				wg.Add(1)
				waitChan <- struct{}{}
				go func(entry *filer_pb.FullEntry) {
					if errEntry := filer_pb.CreateEntry(context.Background(), client, &filer_pb.CreateEntryRequest{
						Directory: entry.Dir,
						Entry:     entry.Entry,
					}); errEntry != nil {
						err = errEntry
					}
					defer wg.Done()
					<-waitChan
				}(fullEntry)
				if err != nil {
					return err
				}
				fileCount++
			}
		}
	})

	if err == nil {
		fmt.Fprintf(writer, "\ntotal %d directories, %d files", dirCount, fileCount)
		fmt.Fprintf(writer, "\n%s is loaded.\n", fileName)
	}

	return err
}
