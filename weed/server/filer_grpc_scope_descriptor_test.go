package weed_server

import (
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestScopeDescriptorEveryFilerMethodIsAllowedOrExplicitlyRefused(t *testing.T) {
	expectedUnary := map[string]reflect.Type{
		"AssignVolume":           reflect.TypeOf((*filer_pb.AssignVolumeRequest)(nil)),
		"LookupDirectoryEntry":   reflect.TypeOf((*filer_pb.LookupDirectoryEntryRequest)(nil)),
		"LookupDirectoryEntries": reflect.TypeOf((*filer_pb.LookupDirectoryEntriesRequest)(nil)),
		"CreateEntry":            reflect.TypeOf((*filer_pb.CreateEntryRequest)(nil)),
		"UpdateEntry":            reflect.TypeOf((*filer_pb.UpdateEntryRequest)(nil)),
		"TouchAccessTime":        reflect.TypeOf((*filer_pb.TouchAccessTimeRequest)(nil)),
		"AppendToEntry":          reflect.TypeOf((*filer_pb.AppendToEntryRequest)(nil)),
		"DeleteEntry":            reflect.TypeOf((*filer_pb.DeleteEntryRequest)(nil)),
		"PosixLock":              reflect.TypeOf((*filer_pb.PosixLockRequest)(nil)),
		"AtomicRenameEntry":      reflect.TypeOf((*filer_pb.AtomicRenameEntryRequest)(nil)),
		"LookupVolume":           reflect.TypeOf((*filer_pb.LookupVolumeRequest)(nil)),
		"Statistics":             reflect.TypeOf((*filer_pb.StatisticsRequest)(nil)),
		"Ping":                   reflect.TypeOf((*filer_pb.PingRequest)(nil)),
		"GetFilerConfiguration":  reflect.TypeOf((*filer_pb.GetFilerConfigurationRequest)(nil)),
	}
	explicitlyRefusedUnary := map[string]bool{
		"ObjectTransaction":               true,
		"ObjectTransactionBatch":          true,
		"CollectionList":                  true,
		"DeleteCollection":                true,
		"ListMetadataSubscribers":         true,
		"KvGet":                           true,
		"KvPut":                           true,
		"CacheRemoteObjectToLocalCluster": true,
		"DistributedLock":                 true,
		"DistributedUnlock":               true,
		"FindLockOwner":                   true,
		"TransferLocks":                   true,
		"ReplicateLock":                   true,
		"MountRegister":                   true,
		"MountList":                       true,
	}
	expectedStreams := map[string]bool{
		"ListEntries":            true,
		"StreamRenameEntry":      true,
		"StreamMutateEntry":      true,
		"SubscribeMetadata":      true,
		"SubscribeLocalMetadata": true,
	}
	explicitlyRefusedStreams := map[string]bool{
		"TraverseBfsMetadata": true,
	}

	for _, method := range filer_pb.SeaweedFiler_ServiceDesc.Methods {
		name := method.MethodName
		got, allowed := scopeUnaryMethods[name]
		want, expected := expectedUnary[name]
		refused := explicitlyRefusedUnary[name]
		if !expected && !refused {
			t.Errorf("unclassified unary method %q: add it to the scope rule table or explicit refusal list", name)
			continue
		}
		if expected && !allowed {
			t.Errorf("allowed unary method %q is missing from scopeUnaryMethods", name)
			continue
		}
		if refused && allowed {
			t.Errorf("explicitly refused unary method %q is in scopeUnaryMethods", name)
		}
		if expected && got != want {
			t.Errorf("scopeUnaryMethods[%q] = %v, want %v", name, got, want)
		}
	}
	for name := range expectedUnary {
		if _, ok := scopeUnaryMethods[name]; !ok {
			t.Errorf("scope rule table is missing descriptor method %q", name)
		}
	}
	for name := range scopeUnaryMethods {
		if _, ok := expectedUnary[name]; !ok {
			t.Errorf("scope rule table contains unknown unary method %q", name)
		}
	}

	for _, stream := range filer_pb.SeaweedFiler_ServiceDesc.Streams {
		name := stream.StreamName
		allowed := scopeStreamMethods[name]
		if expectedStreams[name] {
			if !allowed {
				t.Errorf("allowed stream method %q is missing from scopeStreamMethods", name)
			}
			continue
		}
		if !explicitlyRefusedStreams[name] {
			t.Errorf("unclassified stream method %q: add it to the scope rule table or explicit refusal list", name)
		}
		if allowed {
			t.Errorf("explicitly refused stream method %q is in scopeStreamMethods", name)
		}
	}
	for name := range scopeStreamMethods {
		if !expectedStreams[name] {
			t.Errorf("scope stream rule table contains unknown method %q", name)
		}
	}
}
