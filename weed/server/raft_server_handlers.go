package weed_server

import (
	"net/http"
)

type ClusterStatusResult struct {
	IsLeader bool     `json:"IsLeader,omitempty"`
	Leader   string   `json:"Leader,omitempty"`
	Peers    []string `json:"Peers,omitempty"`
}

func (s *RaftServer) StatusHandler(w http.ResponseWriter, r *http.Request) {
	ret := ClusterStatusResult{
		IsLeader: s.topo.IsLeader(),
		Peers:    s.Peers(),
	}
	if leader, e := s.topo.Leader(); e == nil {
		ret.Leader = leader
	}
	writeJsonQuiet(w, r, http.StatusOK, ret)
}
