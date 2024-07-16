package redis3

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util/skiplist"
	"google.golang.org/protobuf/proto"
)

type SkipListElementStore struct {
	Prefix string
	client redis.UniversalClient
}

var _ = skiplist.ListStore(&SkipListElementStore{})

func newSkipListElementStore(prefix string, client redis.UniversalClient) *SkipListElementStore {
	return &SkipListElementStore{
		Prefix: prefix,
		client: client,
	}
}

func (m *SkipListElementStore) SaveElement(id int64, element *skiplist.SkipListElement) error {
	key := fmt.Sprintf("%s%d", m.Prefix, id)
	data, err := proto.Marshal(element)
	if err != nil {
		glog.Errorf("marshal %s: %v", key, err)
	}
	return m.client.Set(context.Background(), key, data, 0).Err()
}

func (m *SkipListElementStore) DeleteElement(id int64) error {
	key := fmt.Sprintf("%s%d", m.Prefix, id)
	return m.client.Del(context.Background(), key).Err()
}

func (m *SkipListElementStore) LoadElement(id int64) (*skiplist.SkipListElement, error) {
	key := fmt.Sprintf("%s%d", m.Prefix, id)
	data, err := m.client.Get(context.Background(), key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	t := &skiplist.SkipListElement{}
	err = proto.Unmarshal([]byte(data), t)
	if err == nil {
		for i := 0; i < len(t.Next); i++ {
			if t.Next[i].IsNil() {
				t.Next[i] = nil
			}
		}
		if t.Prev.IsNil() {
			t.Prev = nil
		}
	}
	return t, err
}
