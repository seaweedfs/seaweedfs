package elastic

import (
	"context"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	jsoniter "github.com/json-iterator/go"
	elastic "github.com/olivere/elastic/v7"
)

func (store *ElasticStore) KvDelete(ctx context.Context, key []byte) (err error) {
	deleteResult, err := store.client.Delete().
		Index(indexKV).
		Type(indexType).
		Id(string(key)).
		Do(ctx)
	if err == nil {
		if deleteResult.Result == "deleted" || deleteResult.Result == "not_found" {
			return nil
		}
	}
	glog.Errorf("delete key(id:%s) %v.", string(key), err)
	return fmt.Errorf("delete key %v.", err)
}

func (store *ElasticStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	searchResult, err := store.client.Get().
		Index(indexKV).
		Type(indexType).
		Id(string(key)).
		Do(ctx)
	if elastic.IsNotFound(err) {
		return nil, filer_pb.ErrNotFound
	}
	if searchResult != nil && searchResult.Found {
		esEntry := &ESKVEntry{}
		if err := jsoniter.Unmarshal(searchResult.Source, esEntry); err == nil {
			return []byte(esEntry.Value), nil
		}
	}
	glog.Errorf("find key(%s),%v.", string(key), err)
	return nil, filer_pb.ErrNotFound
}

func (store *ElasticStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	esEntry := &ESKVEntry{string(value)}
	val, err := jsoniter.Marshal(esEntry)
	if err != nil {
		glog.Errorf("insert key(%s) %v.", string(key), err)
		return fmt.Errorf("insert key %v.", err)
	}
	_, err = store.client.Index().
		Index(indexKV).
		Type(indexType).
		Id(string(key)).
		BodyJson(string(val)).
		Do(ctx)
	if err != nil {
		return fmt.Errorf("kv put: %v", err)
	}
	return nil
}
