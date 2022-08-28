package remote_pb

import "github.com/golang/protobuf/proto"

func (fp *RemoteStorageLocation) Key() interface{} {
	key, _ := proto.Marshal(fp)
	return string(key)
}
