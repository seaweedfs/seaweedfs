package plugin

import (
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"google.golang.org/protobuf/proto"
)

// JobReplicaHosts are the volume-replica hosts a job will read from.
// Used so executor pick prefers a worker on one of those machines.
// Sources with ShardIds are EC-cleanup rows, not encode replicas.
func JobReplicaHosts(job *plugin_pb.JobSpec) []string {
	if job == nil {
		return nil
	}
	var addrs []string
	addrs = append(addrs, pluginworker.ReadStringListConfig(job.Parameters, "replica_servers")...)
	if src := strings.TrimSpace(pluginworker.ReadStringConfig(job.Parameters, "source_server", "")); src != "" {
		addrs = append(addrs, src)
	}
	if payload := pluginworker.ReadBytesConfig(job.Parameters, "task_params_pb"); len(payload) > 0 {
		params := &worker_pb.TaskParams{}
		if err := proto.Unmarshal(payload, params); err == nil {
			for _, s := range params.GetSources() {
				if s == nil || s.Node == "" || len(s.ShardIds) > 0 {
					continue
				}
				addrs = append(addrs, s.Node)
			}
		}
	}
	return util.HostKeysFromAddresses(addrs)
}

// partitionByPreferredHost splits candidates into (host matches, others).
// No preferred hosts, or no matches, returns (nil, all) so callers keep
// the existing slots+round-robin path.
func partitionByPreferredHost(candidates []*WorkerSession, preferredHosts []string) (pref, rest []*WorkerSession) {
	want := util.HostKeysFromAddresses(preferredHosts)
	if len(want) == 0 || len(candidates) == 0 {
		return nil, candidates
	}
	set := make(map[string]struct{}, len(want))
	for _, h := range want {
		set[h] = struct{}{}
	}
	for _, c := range candidates {
		if c == nil {
			continue
		}
		if _, ok := set[util.HostKey(c.Address)]; ok {
			pref = append(pref, c)
			continue
		}
		rest = append(rest, c)
	}
	if len(pref) == 0 {
		return nil, candidates
	}
	return pref, rest
}
