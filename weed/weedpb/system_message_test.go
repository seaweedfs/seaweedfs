package weedpb

import (
	"encoding/json"
	"log"
	"testing"

	"github.com/golang/protobuf/proto"
)

func TestSerialDeserial(t *testing.T) {
	volumeMessage := &VolumeInformationMessage{
		Id:               12,
		Size:             2341234,
		Collection:       "benchmark",
		FileCount:        2341234,
		DeleteCount:      234,
		DeletedByteCount: 21234,
		ReadOnly:         false,
		ReplicaPlacement: 210,
		Version:          2,
	}
	var volumeMessages []*VolumeInformationMessage
	volumeMessages = append(volumeMessages, volumeMessage)

	joinMessage := &JoinMessage{
		IsInit:         true,
		Ip:             "127.0.3.12",
		Port:           34546,
		PublicUrl:      "localhost:2342",
		MaxVolumeCount: 210,
		MaxFileKey:     324234423,
		DataCenter:     "dc1",
		Rack:           "rack2",
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
	if joinMessage.PublicUrl != newMessage.PublicUrl {
		log.Fatalf("data mismatch %q != %q", joinMessage.PublicUrl, newMessage.PublicUrl)
	}
}
