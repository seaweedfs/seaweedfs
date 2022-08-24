//go:build elastic
// +build elastic

package command

import (
	"context"
	jsoniter "github.com/json-iterator/go"
	elastic "github.com/olivere/elastic/v7"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"strings"
)

type EsDocument struct {
	Dir         string `json:"dir,omitempty"`
	Name        string `json:"name,omitempty"`
	IsDirectory bool   `json:"isDir,omitempty"`
	Size        uint64 `json:"size,omitempty"`
	Uid         uint32 `json:"uid,omitempty"`
	Gid         uint32 `json:"gid,omitempty"`
	UserName    string `json:"userName,omitempty"`
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
