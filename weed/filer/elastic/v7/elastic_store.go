package elastic

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	weed_util "github.com/chrislusf/seaweedfs/weed/util"
	jsoniter "github.com/json-iterator/go"
	elastic "github.com/olivere/elastic/v7"
)

var (
	indexType           = "_doc"
	indexPrefix         = ".seaweedfs_"
	indexKV             = ".seaweedfs_kv_entries"
	mappingWithoutQuery = ` {
		     "mappings": {
		   	"enabled": false,
		       "properties": {
		         "Value":{
		           "type": "binary"
		         }
		       }
		     }
		   }`
)

type ESEntry struct {
	ParentId string `json:"ParentId"`
	Entry    *filer.Entry
}

type ESKVEntry struct {
	Value []byte `json:"Value"`
}

func init() {
	filer.Stores = append(filer.Stores, &ElasticStore{})
}

type ElasticStore struct {
	client      *elastic.Client
	maxPageSize int
}

func (store *ElasticStore) GetName() string {
	return "elastic7"
}

func (store *ElasticStore) Initialize(configuration weed_util.Configuration, prefix string) (err error) {
	options := store.initialize(configuration, prefix)
	store.client, err = elastic.NewClient(options...)
	if err != nil {
		return fmt.Errorf("init elastic %v.", err)
	}
	if ok, err := store.client.IndexExists(indexKV).Do(context.Background()); err == nil && !ok {
		_, err = store.client.CreateIndex(indexKV).Body(mappingWithoutQuery).Do(context.Background())
		if err != nil {
			return fmt.Errorf("create index(%s) %v.", indexKV, err)
		}
	}
	return nil
}

func (store *ElasticStore) initialize(configuration weed_util.Configuration, prefix string) (options []elastic.ClientOptionFunc) {
	servers := configuration.GetStringSlice(prefix + "servers")
	options = append(options, elastic.SetURL(servers...))
	username := configuration.GetString(prefix + "username")
	password := configuration.GetString(prefix + "password")
	if username != "" && password != "" {
		options = append(options, elastic.SetBasicAuth(username, password))
	}
	options = append(options, elastic.SetSniff(configuration.GetBool(prefix+"sniff_enabled")))
	options = append(options, elastic.SetHealthcheck(configuration.GetBool(prefix+"healthcheck_enabled")))
	store.maxPageSize = configuration.GetInt(prefix + "index.max_result_window")
	if store.maxPageSize <= 0 {
		store.maxPageSize = 10000
	}
	glog.Infof("filer store elastic endpoints: %s.", servers)
	return options
}

func (store *ElasticStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (store *ElasticStore) CommitTransaction(ctx context.Context) error {
	return nil
}
func (store *ElasticStore) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *ElasticStore) ListDirectoryPrefixedEntries(ctx context.Context, fullpath weed_util.FullPath, startFileName string, inclusive bool, limit int, prefix string) (entries []*filer.Entry, err error) {
	return nil, filer.ErrUnsupportedListDirectoryPrefixed
}

func (store *ElasticStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {
	index := getIndex(entry.FullPath)
	dir, _ := entry.FullPath.DirAndName()
	id := weed_util.Md5String([]byte(entry.FullPath))
	esEntry := &ESEntry{
		ParentId: weed_util.Md5String([]byte(dir)),
		Entry:    entry,
	}
	value, err := jsoniter.Marshal(esEntry)
	if err != nil {
		glog.Errorf("insert entry(%s) %v.", string(entry.FullPath), err)
		return fmt.Errorf("insert entry %v.", err)
	}
	_, err = store.client.Index().
		Index(index).
		Type(indexType).
		Id(id).
		BodyJson(string(value)).
		Do(ctx)
	if err != nil {
		glog.Errorf("insert entry(%s) %v.", string(entry.FullPath), err)
		return fmt.Errorf("insert entry %v.", err)
	}
	return nil
}

func (store *ElasticStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {
	return store.InsertEntry(ctx, entry)
}

func (store *ElasticStore) FindEntry(ctx context.Context, fullpath weed_util.FullPath) (entry *filer.Entry, err error) {
	index := getIndex(fullpath)
	id := weed_util.Md5String([]byte(fullpath))
	searchResult, err := store.client.Get().
		Index(index).
		Type(indexType).
		Id(id).
		Do(ctx)
	if elastic.IsNotFound(err) {
		return nil, filer_pb.ErrNotFound
	}
	if searchResult != nil && searchResult.Found {
		esEntry := &ESEntry{
			ParentId: "",
			Entry:    &filer.Entry{},
		}
		err := jsoniter.Unmarshal(searchResult.Source, esEntry)
		return esEntry.Entry, err
	}
	glog.Errorf("find entry(%s),%v.", string(fullpath), err)
	return nil, filer_pb.ErrNotFound
}

func (store *ElasticStore) DeleteEntry(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	index := getIndex(fullpath)
	id := weed_util.Md5String([]byte(fullpath))
	if strings.Count(string(fullpath), "/") == 1 {
		return store.deleteIndex(ctx, index)
	}
	return store.deleteEntry(ctx, index, id)
}

func (store *ElasticStore) deleteIndex(ctx context.Context, index string) (err error) {
	deleteResult, err := store.client.DeleteIndex(index).Do(ctx)
	if elastic.IsNotFound(err) || (err == nil && deleteResult.Acknowledged) {
		return nil
	}
	glog.Errorf("delete index(%s) %v.", index, err)
	return err
}

func (store *ElasticStore) deleteEntry(ctx context.Context, index, id string) (err error) {
	deleteResult, err := store.client.Delete().
		Index(index).
		Type(indexType).
		Id(id).
		Do(ctx)
	if err == nil {
		if deleteResult.Result == "deleted" || deleteResult.Result == "not_found" {
			return nil
		}
	}
	glog.Errorf("delete entry(index:%s,_id:%s) %v.", index, id, err)
	return fmt.Errorf("delete entry %v.", err)
}

func (store *ElasticStore) DeleteFolderChildren(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	if entries, err := store.ListDirectoryEntries(ctx, fullpath, "", false, math.MaxInt32); err == nil {
		for _, entry := range entries {
			store.DeleteEntry(ctx, entry.FullPath)
		}
	}
	return nil
}

func (store *ElasticStore) ListDirectoryEntries(
	ctx context.Context, fullpath weed_util.FullPath, startFileName string, inclusive bool, limit int,
) (entries []*filer.Entry, err error) {
	if string(fullpath) == "/" {
		return store.listRootDirectoryEntries(ctx, startFileName, inclusive, limit)
	}
	return store.listDirectoryEntries(ctx, fullpath, startFileName, inclusive, limit)
}

func (store *ElasticStore) listRootDirectoryEntries(ctx context.Context, startFileName string, inclusive bool, limit int) (entries []*filer.Entry, err error) {
	indexResult, err := store.client.CatIndices().Do(ctx)
	if err != nil {
		glog.Errorf("list indices %v.", err)
		return entries, err
	}
	for _, index := range indexResult {
		if index.Index == indexKV {
			continue
		}
		if strings.HasPrefix(index.Index, indexPrefix) {
			if entry, err := store.FindEntry(ctx,
				weed_util.FullPath("/"+strings.Replace(index.Index, indexPrefix, "", 1))); err == nil {
				fileName := getFileName(entry.FullPath)
				if fileName == startFileName && !inclusive {
					continue
				}
				limit--
				if limit < 0 {
					break
				}
				entries = append(entries, entry)
			}
		}
	}
	return entries, nil
}

func (store *ElasticStore) listDirectoryEntries(
	ctx context.Context, fullpath weed_util.FullPath, startFileName string, inclusive bool, limit int,
) (entries []*filer.Entry, err error) {
	first := true
	index := getIndex(fullpath)
	nextStart := ""
	parentId := weed_util.Md5String([]byte(fullpath))
	if _, err := store.client.Refresh(index).Do(ctx); err != nil {
		if elastic.IsNotFound(err) {
			store.client.CreateIndex(index).Do(ctx)
			return entries, nil
		}
	}
	for {
		result := &elastic.SearchResult{}
		if (startFileName == "" && first) || inclusive {
			if result, err = store.search(ctx, index, parentId); err != nil {
				glog.Errorf("search (%s,%s,%t,%d) %v.", string(fullpath), startFileName, inclusive, limit, err)
				return entries, err
			}
		} else {
			fullPath := string(fullpath) + "/" + startFileName
			if !first {
				fullPath = nextStart
			}
			after := weed_util.Md5String([]byte(fullPath))
			if result, err = store.searchAfter(ctx, index, parentId, after); err != nil {
				glog.Errorf("searchAfter (%s,%s,%t,%d) %v.", string(fullpath), startFileName, inclusive, limit, err)
				return entries, err
			}
		}
		first = false
		for _, hit := range result.Hits.Hits {
			esEntry := &ESEntry{
				ParentId: "",
				Entry:    &filer.Entry{},
			}
			if err := jsoniter.Unmarshal(hit.Source, esEntry); err == nil {
				limit--
				if limit < 0 {
					return entries, nil
				}
				nextStart = string(esEntry.Entry.FullPath)
				fileName := getFileName(esEntry.Entry.FullPath)
				if fileName == startFileName && !inclusive {
					continue
				}
				entries = append(entries, esEntry.Entry)
			}
		}
		if len(result.Hits.Hits) < store.maxPageSize {
			break
		}
	}
	return entries, nil
}

func (store *ElasticStore) search(ctx context.Context, index, parentId string) (result *elastic.SearchResult, err error) {
	if count, err := store.client.Count(index).Do(ctx); err == nil && count == 0 {
		return &elastic.SearchResult{
			Hits: &elastic.SearchHits{
				Hits: make([]*elastic.SearchHit, 0)},
		}, nil
	}
	queryResult, err := store.client.Search().
		Index(index).
		Query(elastic.NewMatchQuery("ParentId", parentId)).
		Size(store.maxPageSize).
		Sort("_id", false).
		Do(ctx)
	return queryResult, err
}

func (store *ElasticStore) searchAfter(ctx context.Context, index, parentId, after string) (result *elastic.SearchResult, err error) {
	queryResult, err := store.client.Search().
		Index(index).
		Query(elastic.NewMatchQuery("ParentId", parentId)).
		SearchAfter(after).
		Size(store.maxPageSize).
		Sort("_id", false).
		Do(ctx)
	return queryResult, err

}

func (store *ElasticStore) Shutdown() {
	store.client.Stop()
}

func getIndex(fullpath weed_util.FullPath) string {
	path := strings.Split(string(fullpath), "/")
	if len(path) > 1 {
		return indexPrefix + path[1]
	}
	return ""
}

func getFileName(fullpath weed_util.FullPath) string {
	path := strings.Split(string(fullpath), "/")
	if len(path) > 1 {
		return path[len(path)-1]
	}
	return ""
}
