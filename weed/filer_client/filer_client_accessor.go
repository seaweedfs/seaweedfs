package filer_client

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc"
)

// filerHealth tracks the health status of a filer
type filerHealth struct {
	address      pb.ServerAddress
	failureCount int32
	lastFailure  time.Time
	backoffUntil time.Time
}

// isHealthy returns true if the filer is not in backoff period
func (fh *filerHealth) isHealthy() bool {
	return time.Now().After(fh.backoffUntil)
}

// recordFailure updates failure count and sets backoff time using exponential backoff
func (fh *filerHealth) recordFailure() {
	count := atomic.AddInt32(&fh.failureCount, 1)
	fh.lastFailure = time.Now()

	// Exponential backoff: 1s, 2s, 4s, 8s, 16s, 32s, max 30s
	// Calculate 2^(count-1) but cap the result at 30 seconds
	backoffSeconds := 1 << (count - 1)
	if backoffSeconds > 30 {
		backoffSeconds = 30
	}
	fh.backoffUntil = time.Now().Add(time.Duration(backoffSeconds) * time.Second)

	glog.V(1).Infof("Filer %v failed %d times, backing off for %ds", fh.address, count, backoffSeconds)
}

// recordSuccess resets failure count and clears backoff
func (fh *filerHealth) recordSuccess() {
	atomic.StoreInt32(&fh.failureCount, 0)
	fh.backoffUntil = time.Time{}
}

type FilerClientAccessor struct {
	GetGrpcDialOption func() grpc.DialOption
	GetFilers         func() []pb.ServerAddress // Returns multiple filer addresses for failover

	// Health tracking for smart failover
	filerHealthMap sync.Map // map[pb.ServerAddress]*filerHealth
}

// getOrCreateFilerHealth returns the health tracker for a filer, creating one if needed
func (fca *FilerClientAccessor) getOrCreateFilerHealth(address pb.ServerAddress) *filerHealth {
	if health, ok := fca.filerHealthMap.Load(address); ok {
		return health.(*filerHealth)
	}

	newHealth := &filerHealth{
		address:      address,
		failureCount: 0,
		backoffUntil: time.Time{},
	}

	actual, _ := fca.filerHealthMap.LoadOrStore(address, newHealth)
	return actual.(*filerHealth)
}

// partitionFilers separates filers into healthy and backoff groups
func (fca *FilerClientAccessor) partitionFilers(filers []pb.ServerAddress) (healthy, backoff []pb.ServerAddress) {
	for _, filer := range filers {
		health := fca.getOrCreateFilerHealth(filer)
		if health.isHealthy() {
			healthy = append(healthy, filer)
		} else {
			backoff = append(backoff, filer)
		}
	}
	return healthy, backoff
}

// shuffleFilers randomizes the order of filers to distribute load
func (fca *FilerClientAccessor) shuffleFilers(filers []pb.ServerAddress) []pb.ServerAddress {
	if len(filers) <= 1 {
		return filers
	}

	shuffled := make([]pb.ServerAddress, len(filers))
	copy(shuffled, filers)

	// Fisher-Yates shuffle
	for i := len(shuffled) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}

	return shuffled
}

func (fca *FilerClientAccessor) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fca.withMultipleFilers(streamingMode, fn)
}

// withMultipleFilers tries each filer with smart failover and backoff logic
func (fca *FilerClientAccessor) withMultipleFilers(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	filers := fca.GetFilers()
	if len(filers) == 0 {
		return fmt.Errorf("no filer addresses available")
	}

	// Partition filers into healthy and backoff groups
	healthyFilers, backoffFilers := fca.partitionFilers(filers)

	// Shuffle healthy filers to distribute load evenly
	healthyFilers = fca.shuffleFilers(healthyFilers)

	// Try healthy filers first
	var lastErr error
	for _, filerAddress := range healthyFilers {
		health := fca.getOrCreateFilerHealth(filerAddress)

		err := pb.WithFilerClient(streamingMode, 0, filerAddress, fca.GetGrpcDialOption(), fn)
		if err == nil {
			// Success - record it and return
			health.recordSuccess()
			glog.V(2).Infof("Filer %v succeeded", filerAddress)
			return nil
		}

		// Record failure and continue to next filer
		health.recordFailure()
		lastErr = err
		glog.V(1).Infof("Healthy filer %v failed: %v, trying next", filerAddress, err)
	}

	// If all healthy filers failed, try backoff filers as last resort
	if len(backoffFilers) > 0 {
		glog.V(1).Infof("All healthy filers failed, trying %d backoff filers", len(backoffFilers))

		for _, filerAddress := range backoffFilers {
			health := fca.getOrCreateFilerHealth(filerAddress)

			err := pb.WithFilerClient(streamingMode, 0, filerAddress, fca.GetGrpcDialOption(), fn)
			if err == nil {
				// Success - record it and return
				health.recordSuccess()
				glog.V(1).Infof("Backoff filer %v recovered and succeeded", filerAddress)
				return nil
			}

			// Update failure record
			health.recordFailure()
			lastErr = err
			glog.V(1).Infof("Backoff filer %v still failing: %v", filerAddress, err)
		}
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
		filerHealthMap: sync.Map{},
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
