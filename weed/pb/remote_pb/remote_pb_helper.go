package remote_pb

import "google.golang.org/protobuf/proto"

func (fp *RemoteStorageLocation) Key() interface{} {
	key, _ := proto.Marshal(fp)
	return string(key)
}
