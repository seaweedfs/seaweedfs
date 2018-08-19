package msgqueue

import (
	"github.com/golang/protobuf/proto"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type MessageQueue interface {
	// GetName gets the name to locate the configuration in message_queue.toml file
	GetName() string
	// Initialize initializes the file store
	Initialize(configuration util.Configuration) error
	SendMessage(key string, message proto.Message) error
}
