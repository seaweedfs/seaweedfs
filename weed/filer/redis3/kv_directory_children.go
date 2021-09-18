package redis3

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/util/bptree"
	"github.com/go-redis/redis/v8"
	"github.com/golang/protobuf/proto"
)

func insertChild(ctx context.Context, client redis.UniversalClient, key string, name string) error {
	data, err := client.Get(ctx, key).Result()
	if err != nil {
		if err != redis.Nil {
			return fmt.Errorf("read %s: %v", key, err)
		}
	}
	rootNode := &bptree.ProtoNode{}
	if err := proto.UnmarshalMerge([]byte(data), rootNode); err != nil {
		return fmt.Errorf("decoding root for %s: %v", key, err)
	}
	tree := rootNode.ToBpTree()
	tree.Add(bptree.String(name), nil)
	return nil
}

func removeChild(ctx context.Context, client redis.UniversalClient, key string, name string) error {
	data, err := client.Get(ctx, key).Result()
	if err != nil {
		if err != redis.Nil {
			return fmt.Errorf("read %s: %v", key, err)
		}
	}
	rootNode := &bptree.ProtoNode{}
	if err := proto.UnmarshalMerge([]byte(data), rootNode); err != nil {
		return fmt.Errorf("decoding root for %s: %v", key, err)
	}
	tree := rootNode.ToBpTree()
	tree.Add(bptree.String(name), nil)
	return nil
}

func removeChildren(ctx context.Context, client redis.UniversalClient, key string, onDeleteFn func(name string) error) error {
	return nil
}

func iterateChildren(ctx context.Context, client redis.UniversalClient, key string, eachFn func(name string) error) error {
	return nil
}
