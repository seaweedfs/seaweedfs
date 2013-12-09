package weed_server

import (
	"bytes"
	"code.google.com/p/weed-fs/go/glog"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/goraft/raft"
	"github.com/gorilla/mux"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type RaftServer struct {
	peers      []string // initial peers to join with
	raftServer raft.Server
	dataDir    string
	httpAddr   string
	version    string
	router     *mux.Router
}

func NewRaftServer(r *mux.Router, version string, peers []string, httpAddr string, dataDir string) *RaftServer {
	s := &RaftServer{
		version:  version,
		peers:    peers,
		httpAddr: httpAddr,
		dataDir:  dataDir,
		router:   r,
	}

	//raft.SetLogLevel(2)

	var err error
	transporter := raft.NewHTTPTransporter("/raft")
	s.raftServer, err = raft.NewServer(s.httpAddr, s.dataDir, transporter, nil, nil, "")
	if err != nil {
		glog.V(0).Infoln(err)
		return nil
	}
	transporter.Install(s.raftServer, s)
	s.raftServer.SetHeartbeatTimeout(1 * time.Second)
	s.raftServer.SetElectionTimeout(1500 * time.Millisecond)
	s.raftServer.Start()

	s.router.HandleFunc("/raft/join", s.joinHandler).Methods("POST")

	// Join to leader if specified.
	if len(s.peers) > 0 {
		glog.V(0).Infoln("Joining cluster:", strings.Join(s.peers, ","))

		if !s.raftServer.IsLogEmpty() {
			glog.V(0).Infoln("Cannot join with an existing log")
		}

		if err := s.Join(s.peers); err != nil {
			return nil
		}

		glog.V(0).Infoln("Joined cluster")

		// Initialize the server by joining itself.
	} else if s.raftServer.IsLogEmpty() {
		glog.V(0).Infoln("Initializing new cluster")

		_, err := s.raftServer.Do(&raft.DefaultJoinCommand{
			Name:             s.raftServer.Name(),
			ConnectionString: "http://" + s.httpAddr,
		})

		if err != nil {
			glog.V(0).Infoln(err)
			return nil
		}

	} else {
		glog.V(0).Infoln("Recovered from log")
	}

	return s
}

func (s *RaftServer) Leader() string {
	l := s.raftServer.Leader()

	if l == "" {
		// We are a single node cluster, we are the leader
		return s.raftServer.Name()
	}

	return l
}

func (s *RaftServer) Members() (members []string) {
	peers := s.raftServer.Peers()

	for _, p := range peers {
		members = append(members, strings.TrimPrefix(p.ConnectionString, "http://"))
	}

	return
}

// Join joins an existing cluster.
func (s *RaftServer) Join(peers []string) error {
	command := &raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: "http://" + s.httpAddr,
	}

	var b bytes.Buffer
	json.NewEncoder(&b).Encode(command)

	for _, m := range peers {
		glog.V(0).Infoln("Attempting to connect to:", m)

		resp, err := http.Post(fmt.Sprintf("http://%s/raft/join", strings.TrimSpace(m)), "application/json", &b)
		glog.V(0).Infoln("Post returned: ", err)

		if err != nil {
			if _, ok := err.(*url.Error); ok {
				// If we receive a network error try the next member
				continue
			}

			return err
		}

		resp.Body.Close()
		return nil
	}

	return errors.New("Could not connect to any cluster peers")
}
