package operation

import (
	"encoding/json"
	"log"
	"testing"

	proto "code.google.com/p/goprotobuf/proto"
)

func TestSerialDeserial(t *testing.T) {
	volumeMessage := &VolumeInformationMessage{
		Id:               proto.Uint32(12),
		Size:             proto.Uint64(2341234),
		Collection:       proto.String("benchmark"),
		FileCount:        proto.Uint64(2341234),
		DeleteCount:      proto.Uint64(234),
		DeletedByteCount: proto.Uint64(21234),
		ReadOnly:         proto.Bool(false),
		ReplicaPlacement: proto.Uint32(210),
		Version:          proto.Uint32(2),
	}
	var volumeMessages []*VolumeInformationMessage
	volumeMessages = append(volumeMessages, volumeMessage)

	joinMessage := &JoinMessage{
		IsInit:         proto.Bool(true),
		Ip:             proto.String("127.0.3.12"),
		Port:           proto.Uint32(34546),
		PublicUrl:      proto.String("localhost:2342"),
		MaxVolumeCount: proto.Uint32(210),
		MaxFileKey:     proto.Uint64(324234423),
		DataCenter:     proto.String("dc1"),
		Rack:           proto.String("rack2"),
		Volumes:        volumeMessages,
	}

	data, err := proto.Marshal(joinMessage)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	newMessage := &JoinMessage{}
	err = proto.Unmarshal(data, newMessage)
	if err != nil {
		log.Fatal("unmarshaling error: ", err)
	}
	log.Println("The pb data size is", len(data))

	jsonData, jsonError := json.Marshal(joinMessage)
	if jsonError != nil {
		log.Fatal("json marshaling error: ", jsonError)
	}
	log.Println("The json data size is", len(jsonData), string(jsonData))

	// Now test and newTest contain the same data.
	if *joinMessage.PublicUrl != *newMessage.PublicUrl {
		log.Fatalf("data mismatch %q != %q", *joinMessage.PublicUrl, *newMessage.PublicUrl)
	}
}
