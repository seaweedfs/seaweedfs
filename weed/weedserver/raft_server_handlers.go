package weedserver

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/chrislusf/raft"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
)

// Handles incoming RAFT joins.
func (s *RaftServer) joinHandler(w http.ResponseWriter, req *http.Request) {
	glog.V(0).Infoln("Processing incoming join. Current Leader", s.raftServer.Leader(), "Self", s.raftServer.Name(), "Peers", s.raftServer.Peers())
	command := &raft.DefaultJoinCommand{}

	commandText, _ := ioutil.ReadAll(req.Body)
	glog.V(0).Info("Command:", string(commandText))
	if err := json.NewDecoder(strings.NewReader(string(commandText))).Decode(&command); err != nil {
		glog.V(0).Infoln("Error decoding json message:", err, string(commandText))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	glog.V(0).Infoln("join command from Name", command.Name, "Connection", command.ConnectionString)

	if _, err := s.raftServer.Do(command); err != nil {
		switch err {
		case raft.NotLeaderError:
			s.redirectToLeader(w, req)
		default:
			glog.V(0).Infoln("Error processing join:", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func (s *RaftServer) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}

func (s *RaftServer) redirectToLeader(w http.ResponseWriter, req *http.Request) {
	if leader, e := s.topo.Leader(); e == nil {
		//http.StatusMovedPermanently does not cause http POST following redirection
		glog.V(0).Infoln("Redirecting to", http.StatusMovedPermanently, "http://"+leader+req.URL.Path)
		http.Redirect(w, req, "http://"+leader+req.URL.Path, http.StatusMovedPermanently)
	} else {
		glog.V(0).Infoln("Error: Leader Unknown")
		http.Error(w, "Leader unknown", http.StatusInternalServerError)
	}
}

func (s *RaftServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	ret := operation.ClusterStatusResult{
		IsLeader: s.topo.IsLeader(),
		Peers:    s.Peers(),
	}
	if leader, e := s.topo.Leader(); e == nil {
		ret.Leader = leader
	}
	writeJsonQuiet(w, r, http.StatusOK, ret)
}
