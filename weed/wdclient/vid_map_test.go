package wdclient

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"google.golang.org/grpc"
)

func TestLookupFileId(t *testing.T) {
	mc := NewMasterClient(grpc.EmptyDialOption{}, "", "", "", "", "", pb.ServerDiscovery{})
	length := 5

	//Construct a cache linked list of length 5
	for i := 0; i < length; i++ {
		mc.addLocation(uint32(i), Location{Url: strconv.FormatInt(int64(i), 10)})
		mc.resetVidMap()
	}
	for i := 0; i < length; i++ {
		locations, found := mc.GetLocations(uint32(i))
		if !found || len(locations) != 1 || locations[0].Url != strconv.FormatInt(int64(i), 10) {
			t.Fatalf("urls of vid=%d is not valid.", i)
		}
	}

	//When continue to add nodes to the linked list, the previous node will be deleted, and the cache of the response will be gone.
	for i := length; i < length+5; i++ {
		mc.addLocation(uint32(i), Location{Url: strconv.FormatInt(int64(i), 10)})
		mc.resetVidMap()
	}
	for i := 0; i < length; i++ {
		locations, found := mc.GetLocations(uint32(i))
		if found {
			t.Fatalf("urls of vid[%d] should not exists, but found: %v", i, locations)
		}
	}

	//The delete operation will be applied to all cache nodes
	_, found := mc.GetLocations(uint32(length))
	if !found {
		t.Fatalf("urls of vid[%d] not found", length)
	}

	//If the locations of the current node exist, return directly
	newUrl := "abc"
	mc.addLocation(uint32(length), Location{Url: newUrl})
	locations, found := mc.GetLocations(uint32(length))
	if !found || locations[0].Url != newUrl {
		t.Fatalf("urls of vid[%d] not found", length)
	}

	//After delete `abc`, cache nodes are searched
	deleteLoc := Location{Url: newUrl}
	mc.deleteLocation(uint32(length), deleteLoc)
	locations, found = mc.GetLocations(uint32(length))
	if found && locations[0].Url != strconv.FormatInt(int64(length), 10) {
		t.Fatalf("urls of vid[%d] not expected", length)
	}

	//lock: concurrent test
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				for i := 0; i < 20; i++ {
					_, _ = mc.GetLocations(uint32(i))
				}
			}
		}()
	}

	for i := 0; i < 100; i++ {
		mc.addLocation(uint32(i), Location{})
	}
	wg.Wait()
}

func TestConcurrentGetLocations(t *testing.T) {
	mc := NewMasterClient(grpc.EmptyDialOption{}, "", "", "", "", "", pb.ServerDiscovery{})
	location := Location{Url: "TestDataRacing"}
	mc.addLocation(1, location)

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					_, found := mc.GetLocations(1)
					if !found {
						cancel()
						t.Error("vid map invalid due to data racing. ")
						return
					}
				}
			}
		}()
	}

	//Simulate vidmap reset with cache when leader changes
	for i := 0; i < 100; i++ {
		mc.resetVidMap()
		mc.addLocation(1, location)
		time.Sleep(1 * time.Microsecond)
	}
	cancel()
	wg.Wait()
}
