package weed_server

import (
	"sort"
	"testing"
)

// ungatedMasterRPCs are the Seaweed master RPCs that run without
// checkGrpcAdminAuth, each with the reason it stays open. As on the volume
// server the split is by caller: the whitelist holds masters, shell hosts and
// workers, while volume servers, filers, gateways and clients are not
// necessarily on it.
var ungatedMasterRPCs = map[string]string{
	"SendHeartbeat":          "volume servers register and report their volumes",
	"Assign":                 "filers, gateways and clients assign file ids",
	"StreamAssign":           "filers assign file ids over a stream",
	"LookupVolume":           "every reader resolves volume locations",
	"LookupEcVolume":         "every reader resolves EC shard locations",
	"VacuumVolume":           "filer remote-cache eviction vacuums the volumes it emptied",
	"VolumeMarkReadonly":     "volume servers report their own read-only state",
	"GetMasterConfiguration": "volume servers, filers and gateways load cluster settings at startup",
	"Ping":                   "liveness probe; targets are limited to known peers",

	"Statistics":             "read-only usage, the filer's statfs",
	"CollectionList":         "read-only, filers and S3 gateways list collections",
	"CollectionStatistics":   "read-only, S3 bucket size metrics",
	"VolumeList":             "read-only topology",
	"VolumeListStream":       "read-only topology",
	"ListClusterNodes":       "read-only cluster membership",
	"GetAdminLockStatus":     "read-only admin lock holder",
	"RaftListClusterServers": "read-only raft membership",
}

func TestMasterAdminAuthCoverage(t *testing.T) {
	declared := rpcNamesFromProto(t, "master.proto")
	if len(declared) < 25 {
		t.Fatalf("parsed only %d RPCs from the proto, expected the full service", len(declared))
	}

	gated := gatedServerMethods(t, "MasterServer")

	declaredSet := make(map[string]struct{}, len(declared))
	for _, rpc := range declared {
		declaredSet[rpc] = struct{}{}
		_, exempt := ungatedMasterRPCs[rpc]
		switch {
		case gated[rpc] && exempt:
			t.Errorf("%s calls checkGrpcAdminAuth but is also listed in ungatedMasterRPCs", rpc)
		case !gated[rpc] && !exempt:
			t.Errorf("%s does not call checkGrpcAdminAuth and is not listed in ungatedMasterRPCs; "+
				"gate it, or add it with the reason it must stay open", rpc)
		}
	}

	var stale []string
	for rpc := range ungatedMasterRPCs {
		if _, ok := declaredSet[rpc]; !ok {
			stale = append(stale, rpc)
		}
	}
	sort.Strings(stale)
	for _, rpc := range stale {
		t.Errorf("ungatedMasterRPCs lists %q, which is not an RPC in master.proto", rpc)
	}
}
