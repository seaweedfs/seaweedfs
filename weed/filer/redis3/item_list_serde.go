package redis3

import (
	"github.com/redis/go-redis/v9"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util/skiplist"
	"google.golang.org/protobuf/proto"
)

func LoadItemList(data []byte, prefix string, client redis.UniversalClient, store skiplist.ListStore, batchSize int) *ItemList {

	nl := &ItemList{
		skipList:  skiplist.New(store),
		batchSize: batchSize,
		client:    client,
		prefix:    prefix,
	}

	if len(data) == 0 {
		return nl
	}

	message := &skiplist.SkipListProto{}
	if err := proto.Unmarshal(data, message); err != nil {
		glog.Errorf("loading skiplist: %v", err)
	}
	nl.skipList.MaxNewLevel = int(message.MaxNewLevel)
	nl.skipList.MaxLevel = int(message.MaxLevel)
	for i, ref := range message.StartLevels {
		nl.skipList.StartLevels[i] = &skiplist.SkipListElementReference{
			ElementPointer: ref.ElementPointer,
			Key:            ref.Key,
		}
	}
	for i, ref := range message.EndLevels {
		nl.skipList.EndLevels[i] = &skiplist.SkipListElementReference{
			ElementPointer: ref.ElementPointer,
			Key:            ref.Key,
		}
	}
	return nl
}

func (nl *ItemList) HasChanges() bool {
	return nl.skipList.HasChanges
}

func (nl *ItemList) ToBytes() []byte {
	message := &skiplist.SkipListProto{}
	message.MaxNewLevel = int32(nl.skipList.MaxNewLevel)
	message.MaxLevel = int32(nl.skipList.MaxLevel)
	for _, ref := range nl.skipList.StartLevels {
		if ref == nil {
			break
		}
		message.StartLevels = append(message.StartLevels, &skiplist.SkipListElementReference{
			ElementPointer: ref.ElementPointer,
			Key:            ref.Key,
		})
	}
	for _, ref := range nl.skipList.EndLevels {
		if ref == nil {
			break
		}
		message.EndLevels = append(message.EndLevels, &skiplist.SkipListElementReference{
			ElementPointer: ref.ElementPointer,
			Key:            ref.Key,
		})
	}
	data, err := proto.Marshal(message)
	if err != nil {
		glog.Errorf("marshal skiplist: %v", err)
	}
	return data
}
