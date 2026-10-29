package dash

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

const (
	// versionCacheTimeout bounds how old the per-server version map can get.
	// Versions only change on process restart, so a longish window is safe.
	versionCacheTimeout = 60 * time.Second
	// volumeVersionConcurrency bounds the VolumeServerStatus fan-out.
	volumeVersionConcurrency = 8
	volumeVersionRPCTimeout  = 3 * time.Second
)

// ShortVersion strips the volume-size-limit prefix from a version.Version()
// string ("8000GB 4.44 abc123" -> "4.44 abc123") so version comparisons and
// displays reflect the binary, not the node's configured volume size cap.
// Nodes reporting nothing show up as "unknown", which itself is a useful
// signal: very old servers may not report a version at all.
func ShortVersion(v string) string {
	v = strings.TrimSpace(v)
	if i := strings.IndexByte(v, ' '); i > 0 && isSizeLimitToken(v[:i]) {
		v = strings.TrimSpace(v[i+1:])
	}
	if v == "" {
		return "unknown"
	}
	return v
}

// isSizeLimitToken reports whether tok looks like the volume size limit that
// prefixes version.Version() output, e.g. "30GB" or "8000GB".
func isSizeLimitToken(tok string) bool {
	for _, suffix := range []string{"TB", "GB", "MB", "KB", "B"} {
		rest, ok := strings.CutSuffix(tok, suffix)
		if !ok || rest == "" {
			continue
		}
		for _, r := range rest {
			if r != '.' && (r < '0' || r > '9') {
				return false
			}
		}
		return true
	}
	return false
}

// fillVolumeServerVersions populates VolumeServer.Version by asking each
// server for its VolumeServerStatus, since heartbeat-derived topology carries
// no version. Servers that cannot be reached get "" (rendered "unknown").
func (s *AdminServer) fillVolumeServerVersions(servers map[string]*VolumeServer, nodes []*master_pb.DataNodeInfo) {
	s.volumeVersionMu.Lock()
	defer s.volumeVersionMu.Unlock()

	if s.volumeVersionCache != nil && time.Since(s.lastVersionFetch) < versionCacheTimeout {
		for id, vs := range servers {
			vs.Version = s.volumeVersionCache[id]
		}
		return
	}

	versions := make(map[string]string, len(nodes))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, volumeVersionConcurrency)
	for _, node := range nodes {
		wg.Add(1)
		go func(node *master_pb.DataNodeInfo) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			version := s.queryVolumeServerVersion(node)
			mu.Lock()
			versions[node.Id] = version
			mu.Unlock()
		}(node)
	}
	wg.Wait()

	s.volumeVersionCache = versions
	s.lastVersionFetch = time.Now()
	for id, vs := range servers {
		vs.Version = versions[id]
	}
}

// queryVolumeServerVersion asks one volume server for its version string.
func (s *AdminServer) queryVolumeServerVersion(node *master_pb.DataNodeInfo) (version string) {
	err := s.WithVolumeServerClient(pb.NewServerAddressFromDataNode(node), func(client volume_server_pb.VolumeServerClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), volumeVersionRPCTimeout)
		defer cancel()
		resp, err := client.VolumeServerStatus(ctx, &volume_server_pb.VolumeServerStatusRequest{})
		if err != nil {
			return err
		}
		version = resp.Version
		return nil
	})
	if err != nil {
		glog.V(1).Infof("Failed to get version from volume server %s: %v", node.Id, err)
	}
	return
}

// buildClusterVersionSummary groups reported node versions per node type.
// Masters are excluded: they are only enumerable via raft membership, which
// does not carry versions.
func buildClusterVersionSummary(volumeServers []VolumeServer, filers []FilerNode, s3Nodes []S3Node, brokers []MessageBrokerNode) ClusterVersionSummary {
	summary := ClusterVersionSummary{Uniform: true}
	for _, group := range []struct {
		label    string
		versions []string
	}{
		{"Volume Servers", collectVersions(len(volumeServers), func(i int) string { return volumeServers[i].Version })},
		{"Filers", collectVersions(len(filers), func(i int) string { return filers[i].Version })},
		{"S3 Servers", collectVersions(len(s3Nodes), func(i int) string { return s3Nodes[i].Version })},
		{"Message Brokers", collectVersions(len(brokers), func(i int) string { return brokers[i].Version })},
	} {
		nt := summarizeVersions(group.label, group.versions)
		if nt.Total == 0 {
			continue
		}
		summary.NodeTypes = append(summary.NodeTypes, nt)
	}

	// Uniform means every reported node is on the same single version.
	seen := ""
	for _, nt := range summary.NodeTypes {
		for _, vc := range nt.Versions {
			if seen == "" {
				seen = vc.Version
			} else if vc.Version != seen {
				summary.Uniform = false
			}
		}
	}
	if len(summary.NodeTypes) == 0 {
		summary.Uniform = false
	}
	if summary.Uniform {
		summary.Version = seen
	}
	return summary
}

// collectVersions maps node indexes to their normalized version strings.
func collectVersions(n int, versionOf func(i int) string) []string {
	versions := make([]string, 0, n)
	for i := 0; i < n; i++ {
		versions = append(versions, ShortVersion(versionOf(i)))
	}
	return versions
}

// summarizeVersions counts nodes per distinct version.
func summarizeVersions(label string, versions []string) NodeTypeVersions {
	counts := make(map[string]int)
	for _, v := range versions {
		counts[v]++
	}
	nt := NodeTypeVersions{Type: label, Total: len(versions)}
	for v, count := range counts {
		nt.Versions = append(nt.Versions, VersionCount{Version: v, Count: count})
	}
	sort.Slice(nt.Versions, func(i, j int) bool {
		if nt.Versions[i].Count != nt.Versions[j].Count {
			return nt.Versions[i].Count > nt.Versions[j].Count
		}
		return nt.Versions[i].Version < nt.Versions[j].Version
	})
	return nt
}
