package command

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/golang/protobuf/jsonpb"
	jsoniter "github.com/json-iterator/go"
	elastic "github.com/olivere/elastic/v7"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func init() {
	cmdFilerMetaTail.Run = runFilerMetaTail // break init cycle
}

var cmdFilerMetaTail = &Command{
	UsageLine: "filer.meta.tail [-filer=localhost:8888] [-pathPrefix=/]",
	Short:     "see continuous changes on a filer",
	Long: `See continuous changes on a filer.

	weed filer.meta.tail -timeAgo=30h | grep truncate
	weed filer.meta.tail -timeAgo=30h | jq .
	weed filer.meta.tail -timeAgo=30h | jq .eventNotification.newEntry.name

	weed filer.meta.tail -timeAgo=30h -es=http://<elasticSearchServerHost>:<port> -es.index=seaweedfs

  `,
}

var (
	tailFiler   = cmdFilerMetaTail.Flag.String("filer", "localhost:8888", "filer hostname:port")
	tailTarget  = cmdFilerMetaTail.Flag.String("pathPrefix", "/", "path to a folder or common prefix for the folders or files on filer")
	tailStart   = cmdFilerMetaTail.Flag.Duration("timeAgo", 0, "start time before now. \"300ms\", \"1.5h\" or \"2h45m\". Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\"")
	tailPattern = cmdFilerMetaTail.Flag.String("pattern", "", "full path or just filename pattern, ex: \"/home/?opher\", \"*.pdf\", see https://golang.org/pkg/path/filepath/#Match ")
	esServers   = cmdFilerMetaTail.Flag.String("es", "", "comma-separated elastic servers http://<host:port>")
	esIndex     = cmdFilerMetaTail.Flag.String("es.index", "seaweedfs", "ES index name")
)

func runFilerMetaTail(cmd *Command, args []string) bool {

	util.LoadConfiguration("security", false)
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")
	clientId := util.RandomInt32()

	var filterFunc func(dir, fname string) bool
	if *tailPattern != "" {
		if strings.Contains(*tailPattern, "/") {
			println("watch path pattern", *tailPattern)
			filterFunc = func(dir, fname string) bool {
				matched, err := filepath.Match(*tailPattern, dir+"/"+fname)
				if err != nil {
					fmt.Printf("error: %v", err)
				}
				return matched
			}
		} else {
			println("watch file pattern", *tailPattern)
			filterFunc = func(dir, fname string) bool {
				matched, err := filepath.Match(*tailPattern, fname)
				if err != nil {
					fmt.Printf("error: %v", err)
				}
				return matched
			}
		}
	}

	shouldPrint := func(resp *filer_pb.SubscribeMetadataResponse) bool {
		if filer_pb.IsEmpty(resp) {
			return false
		}
		if filterFunc == nil {
			return true
		}
		if resp.EventNotification.OldEntry != nil && filterFunc(resp.Directory, resp.EventNotification.OldEntry.Name) {
			return true
		}
		if resp.EventNotification.NewEntry != nil && filterFunc(resp.EventNotification.NewParentPath, resp.EventNotification.NewEntry.Name) {
			return true
		}
		return false
	}

	jsonpbMarshaler := jsonpb.Marshaler{
		EmitDefaults: false,
	}
	eachEntryFunc := func(resp *filer_pb.SubscribeMetadataResponse) error {
		jsonpbMarshaler.Marshal(os.Stdout, resp)
		fmt.Fprintln(os.Stdout)
		return nil
	}
	if *esServers != "" {
		var err error
		eachEntryFunc, err = sendToElasticSearchFunc(*esServers, *esIndex)
		if err != nil {
			fmt.Printf("create elastic search client to %s: %+v\n", *esServers, err)
			return false
		}
	}

	tailErr := pb.FollowMetadata(pb.ServerAddress(*tailFiler), grpcDialOption, "tail", clientId,
		*tailTarget, nil, time.Now().Add(-*tailStart).UnixNano(), 0,
		func(resp *filer_pb.SubscribeMetadataResponse) error {
			if !shouldPrint(resp) {
				return nil
			}
			if err := eachEntryFunc(resp); err != nil {
				return err
			}
			return nil
		}, false)

	if tailErr != nil {
		fmt.Printf("tail %s: %v\n", *tailFiler, tailErr)
	}

	return true
}

type EsDocument struct {
	Dir         string `json:"dir,omitempty"`
	Name        string `json:"name,omitempty"`
	IsDirectory bool   `json:"isDir,omitempty"`
	Size        uint64 `json:"size,omitempty"`
	Uid         uint32 `json:"uid,omitempty"`
	Gid         uint32 `json:"gid,omitempty"`
	UserName    string `json:"userName,omitempty"`
	Collection  string `json:"collection,omitempty"`
	Crtime      int64  `json:"crtime,omitempty"`
	Mtime       int64  `json:"mtime,omitempty"`
	Mime        string `json:"mime,omitempty"`
}

func toEsEntry(event *filer_pb.EventNotification) (*EsDocument, string) {
	entry := event.NewEntry
	dir, name := event.NewParentPath, entry.Name
	id := util.Md5String([]byte(util.NewFullPath(dir, name)))
	esEntry := &EsDocument{
		Dir:         dir,
		Name:        name,
		IsDirectory: entry.IsDirectory,
		Size:        entry.Attributes.FileSize,
		Uid:         entry.Attributes.Uid,
		Gid:         entry.Attributes.Gid,
		UserName:    entry.Attributes.UserName,
		Collection:  entry.Attributes.Collection,
		Crtime:      entry.Attributes.Crtime,
		Mtime:       entry.Attributes.Mtime,
		Mime:        entry.Attributes.Mime,
	}
	return esEntry, id
}

func sendToElasticSearchFunc(servers string, esIndex string) (func(resp *filer_pb.SubscribeMetadataResponse) error, error) {
	options := []elastic.ClientOptionFunc{}
	options = append(options, elastic.SetURL(strings.Split(servers, ",")...))
	options = append(options, elastic.SetSniff(false))
	options = append(options, elastic.SetHealthcheck(false))
	client, err := elastic.NewClient(options...)
	if err != nil {
		return nil, err
	}
	return func(resp *filer_pb.SubscribeMetadataResponse) error {
		event := resp.EventNotification
		if event.OldEntry != nil &&
			(event.NewEntry == nil || resp.Directory != event.NewParentPath || event.OldEntry.Name != event.NewEntry.Name) {
			// delete or not update the same file
			dir, name := resp.Directory, event.OldEntry.Name
			id := util.Md5String([]byte(util.NewFullPath(dir, name)))
			println("delete", id)
			_, err := client.Delete().Index(esIndex).Id(id).Do(context.Background())
			return err
		}
		if event.NewEntry != nil {
			// add a new file or update the same file
			esEntry, id := toEsEntry(event)
			value, err := jsoniter.Marshal(esEntry)
			if err != nil {
				return err
			}
			println(string(value))
			_, err = client.Index().Index(esIndex).Id(id).BodyJson(string(value)).Do(context.Background())
			return err
		}
		return nil
	}, nil
}
