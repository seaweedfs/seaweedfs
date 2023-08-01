package weed_server

import (
	"github.com/cenkalti/backoff/v4"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"net/http"
	"time"
)

type ClusterStatusResult struct {
	IsLeader    bool             `json:"IsLeader,omitempty"`
	Leader      pb.ServerAddress `json:"Leader,omitempty"`
	Peers       []string         `json:"Peers,omitempty"`
	MaxVolumeId needle.VolumeId  `json:"MaxVolumeId,omitempty"`
}

func (s *RaftServer) StatusHandler(w http.ResponseWriter, r *http.Request) {
	ret := ClusterStatusResult{
		IsLeader:    s.topo.IsLeader(),
		Peers:       s.Peers(),
		MaxVolumeId: s.topo.GetMaxVolumeId(),
	}

	if leader, e := s.topo.Leader(); e == nil {
		ret.Leader = leader
	}
	writeJsonQuiet(w, r, http.StatusOK, ret)
}

func (s *RaftServer) HealthzHandler(w http.ResponseWriter, r *http.Request) {
	leader, err := s.topo.Leader()
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if s.serverAddr == leader {
		expBackoff := backoff.NewExponentialBackOff()
		expBackoff.InitialInterval = 20 * time.Millisecond
		expBackoff.MaxInterval = 1 * time.Second
		expBackoff.MaxElapsedTime = 5 * time.Second
		isLocked, err := backoff.RetryWithData(s.topo.IsChildLocked, expBackoff)
		if err != nil {
			glog.Errorf("HealthzHandler: %+v", err)
		}
		if isLocked {
			w.WriteHeader(http.StatusLocked)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (s *RaftServer) StatsRaftHandler(w http.ResponseWriter, r *http.Request) {
	if s.RaftHashicorp == nil {
		writeJsonQuiet(w, r, http.StatusNotFound, nil)
		return
	}
	writeJsonQuiet(w, r, http.StatusOK, s.RaftHashicorp.Stats())
}
