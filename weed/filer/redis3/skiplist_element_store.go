package redis3

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util/skiplist"
	"github.com/go-redis/redis/v8"
	"github.com/golang/protobuf/proto"
)

type SkipListElementStore struct {
	prefix string
	client redis.UniversalClient
}

var _ = skiplist.ListStore(&SkipListElementStore{})

func newSkipListElementStore(prefix string, client redis.UniversalClient) *SkipListElementStore {
	return &SkipListElementStore{
		prefix: prefix,
		client: client,
	}
}

func (m *SkipListElementStore) SaveElement(id int64, element *skiplist.SkipListElement) error {
	key := fmt.Sprintf("%s%d", m.prefix, id)
	data, err := proto.Marshal(element)
	if err != nil {
		glog.Errorf("marshal %s: %v", key, err)
	}
	return m.client.Set(context.Background(), key, data, 0).Err()
}

func (m *SkipListElementStore) DeleteElement(id int64) error {
	key := fmt.Sprintf("%s%d", m.prefix, id)
	return m.client.Del(context.Background(), key).Err()
}

func (m *SkipListElementStore) LoadElement(id int64) (*skiplist.SkipListElement, error) {
	key := fmt.Sprintf("%s%d", m.prefix, id)
	data, err := m.client.Get(context.Background(), key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	t := &skiplist.SkipListElement{}
	err = proto.Unmarshal([]byte(data), t)
	return t, err
}
