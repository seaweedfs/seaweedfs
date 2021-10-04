package redis3

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util/skiplist"
	"github.com/go-redis/redis/v8"
)

const maxNameBatchSizeLimit = 5

func insertChild(ctx context.Context, client redis.UniversalClient, key string, name string) error {
	data, err := client.Get(ctx, key).Result()
	if err != nil {
		if err != redis.Nil {
			return fmt.Errorf("read %s: %v", key, err)
		}
	}
	store := newSkipListElementStore(key, client)
	nameList := skiplist.LoadNameList([]byte(data), store, maxNameBatchSizeLimit)

	// println("add", key, name)
	if err := nameList.WriteName(name); err != nil {
		glog.Errorf("add %s %s: %v", key, name, err)
		return err
	}
	if !nameList.HasChanges() {
		return nil
	}

	if err := client.Set(ctx, key, nameList.ToBytes(), 0).Err(); err != nil {
		return err
	}

	return nil
}

func removeChild(ctx context.Context, client redis.UniversalClient, key string, name string) error {
	data, err := client.Get(ctx, key).Result()
	if err != nil {
		if err != redis.Nil {
			return fmt.Errorf("read %s: %v", key, err)
		}
	}
	store := newSkipListElementStore(key, client)
	nameList := skiplist.LoadNameList([]byte(data), store, maxNameBatchSizeLimit)

	if err := nameList.DeleteName(name); err != nil {
		return err
	}
	if !nameList.HasChanges() {
		return nil
	}

	if err := client.Set(ctx, key, nameList.ToBytes(), 0).Err(); err != nil {
		return err
	}

	return nil
}

func removeChildren(ctx context.Context, client redis.UniversalClient, key string, onDeleteFn func(name string) error) error {

	data, err := client.Get(ctx, key).Result()
	if err != nil {
		if err != redis.Nil {
			return fmt.Errorf("read %s: %v", key, err)
		}
	}
	store := newSkipListElementStore(key, client)
	nameList := skiplist.LoadNameList([]byte(data), store, maxNameBatchSizeLimit)

	if err = nameList.ListNames("", func(name string) bool {
		if err := onDeleteFn(name); err != nil {
			glog.Errorf("delete %s child %s: %v", key, name, err)
			return false
		}
		return true
	}); err != nil {
		return err
	}

	if err = nameList.RemoteAllListElement(); err != nil {
		return err
	}

	return nil

}

func listChildren(ctx context.Context, client redis.UniversalClient, key string, startFileName string, eachFn func(name string) bool) error {

	data, err := client.Get(ctx, key).Result()
	if err != nil {
		if err != redis.Nil {
			return fmt.Errorf("read %s: %v", key, err)
		}
	}
	store := newSkipListElementStore(key, client)
	nameList := skiplist.LoadNameList([]byte(data), store, maxNameBatchSizeLimit)

	if err = nameList.ListNames(startFileName, func(name string) bool {
		return eachFn(name)
	}); err != nil {
		return err
	}

	return nil

}
