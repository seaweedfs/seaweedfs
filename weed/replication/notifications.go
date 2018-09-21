package replication

import (
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type NotificationInput interface {
	// GetName gets the name to locate the configuration in sync.toml file
	GetName() string
	// Initialize initializes the file store
	Initialize(configuration util.Configuration) error
	ReceiveMessage() (key string, message *filer_pb.EventNotification, err error)
}

var (
	NotificationInputs []NotificationInput
)
