package weed_server

import (
	"code.google.com/p/weed-fs/go/glog"
	"encoding/json"
	"github.com/goraft/raft"
	"net/http"
)

// Handles incoming RAFT joins.
func (s *RaftServer) joinHandler(w http.ResponseWriter, req *http.Request) {
	glog.V(0).Infoln("Processing incoming join. Current Leader", s.raftServer.Leader(), "Self", s.raftServer.Name(), "Peers", s.raftServer.Peers())
	command := &raft.DefaultJoinCommand{}

	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		glog.V(0).Infoln("Error decoding json message:", err)
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
	if s.Leader() != "" {
		glog.V(0).Infoln("Redirecting to", "http://"+s.Leader()+req.URL.Path)
		http.Redirect(w, req, "http://"+s.Leader()+req.URL.Path, http.StatusMovedPermanently)
	} else {
		glog.V(0).Infoln("Error: Leader Unknown")
		http.Error(w, "Leader unknown", http.StatusInternalServerError)
	}
}

func (s *RaftServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Leader"] = s.Leader()
	m["Members"] = s.Members()
	writeJsonQuiet(w, r, m)
}
