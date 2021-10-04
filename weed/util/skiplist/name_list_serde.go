package skiplist

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/golang/protobuf/proto"
)

func LoadNameList(data []byte, store ListStore, batchSize int) *NameList {

	nl := &NameList{
		skipList:  New(store),
		batchSize: batchSize,
	}

	if len(data) == 0 {
		return nl
	}

	message := &SkipListProto{}
	if err := proto.Unmarshal(data, message); err != nil {
		glog.Errorf("loading skiplist: %v", err)
	}
	nl.skipList.maxNewLevel = int(message.MaxNewLevel)
	nl.skipList.maxLevel = int(message.MaxLevel)
	for i, ref := range message.StartLevels {
		nl.skipList.startLevels[i] = &SkipListElementReference{
			ElementPointer: ref.ElementPointer,
			Key:            ref.Key,
		}
	}
	for i, ref := range message.EndLevels {
		nl.skipList.endLevels[i] = &SkipListElementReference{
			ElementPointer: ref.ElementPointer,
			Key:            ref.Key,
		}
	}
	return nl
}

func (nl *NameList) HasChanges() bool {
	return nl.skipList.hasChanges
}

func (nl *NameList) ToBytes() []byte {
	message := &SkipListProto{}
	message.MaxNewLevel = int32(nl.skipList.maxNewLevel)
	message.MaxLevel = int32(nl.skipList.maxLevel)
	for _, ref := range nl.skipList.startLevels {
		if ref == nil {
			break
		}
		message.StartLevels = append(message.StartLevels, &SkipListElementReference{
			ElementPointer: ref.ElementPointer,
			Key:            ref.Key,
		})
	}
	for _, ref := range nl.skipList.endLevels {
		if ref == nil {
			break
		}
		message.EndLevels = append(message.EndLevels, &SkipListElementReference{
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