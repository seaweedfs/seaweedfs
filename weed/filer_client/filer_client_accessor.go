package filer_client

import (
	"fmt"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc"
)

type FilerClientAccessor struct {
	GetGrpcDialOption func() grpc.DialOption
	GetFilers         func() []pb.ServerAddress // Returns multiple filer addresses for failover
	filerIndex        int32                     // Round-robin index for multi-filer selection (internal use)
}

func (fca *FilerClientAccessor) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fca.withMultipleFilers(streamingMode, fn)
}

// withMultipleFilers tries each filer in the list until one succeeds
func (fca *FilerClientAccessor) withMultipleFilers(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	filers := fca.GetFilers()
	if len(filers) == 0 {
		return fmt.Errorf("no filer addresses available")
	}

	// Start from current index and try all filers
	startIndex := atomic.LoadInt32(&fca.filerIndex)
	n := int32(len(filers))

	var lastErr error
	for i := int32(0); i < n; i++ {
		currentIndex := (startIndex + i) % n
		filerAddress := filers[currentIndex]

		err := pb.WithFilerClient(streamingMode, 0, filerAddress, fca.GetGrpcDialOption(), fn)
		if err == nil {
			// Success - update the preferred filer index for next time
			atomic.StoreInt32(&fca.filerIndex, currentIndex)
			return nil
		}

		lastErr = err
		glog.V(1).Infof("Filer %v failed: %v, trying next", filerAddress, err)
	}

	return fmt.Errorf("all filer connections failed, last error: %v", lastErr)
}

func (fca *FilerClientAccessor) SaveTopicConfToFiler(t topic.Topic, conf *mq_pb.ConfigureTopicResponse) error {

	glog.V(0).Infof("save conf for topic %v to filer", t)

	// save the topic configuration on filer
	return fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return t.WriteConfFile(client, conf)
	})
}

func (fca *FilerClientAccessor) ReadTopicConfFromFiler(t topic.Topic) (conf *mq_pb.ConfigureTopicResponse, err error) {

	glog.V(1).Infof("load conf for topic %v from filer", t)

	if err = fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		conf, err = t.ReadConfFile(client)
		return err
	}); err != nil {
		return nil, err
	}

	return conf, nil
}

// ReadTopicConfFromFilerWithMetadata reads topic configuration along with file creation and modification times
func (fca *FilerClientAccessor) ReadTopicConfFromFilerWithMetadata(t topic.Topic) (conf *mq_pb.ConfigureTopicResponse, createdAtNs, modifiedAtNs int64, err error) {

	glog.V(1).Infof("load conf with metadata for topic %v from filer", t)

	if err = fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		conf, createdAtNs, modifiedAtNs, err = t.ReadConfFileWithMetadata(client)
		return err
	}); err != nil {
		return nil, 0, 0, err
	}

	return conf, createdAtNs, modifiedAtNs, nil
}

// NewFilerClientAccessor creates a FilerClientAccessor with one or more filers
func NewFilerClientAccessor(filerAddresses []pb.ServerAddress, grpcDialOption grpc.DialOption) *FilerClientAccessor {
	if len(filerAddresses) == 0 {
		panic("at least one filer address is required")
	}

	return &FilerClientAccessor{
		GetGrpcDialOption: func() grpc.DialOption {
			return grpcDialOption
		},
		GetFilers: func() []pb.ServerAddress {
			return filerAddresses
		},
		filerIndex: 0,
	}
}

// AddFilerAddresses adds more filer addresses to the existing list
func (fca *FilerClientAccessor) AddFilerAddresses(additionalFilers []pb.ServerAddress) {
	if len(additionalFilers) == 0 {
		return
	}

	// Get the current filers if available
	var allFilers []pb.ServerAddress
	if fca.GetFilers != nil {
		allFilers = append(allFilers, fca.GetFilers()...)
	}

	// Add the additional filers
	allFilers = append(allFilers, additionalFilers...)

	// Update the filers list
	fca.GetFilers = func() []pb.ServerAddress {
		return allFilers
	}
}
