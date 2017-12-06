package weed_server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/chrislusf/raft"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/topology"
	"github.com/gorilla/mux"
)

type RaftServer struct {
	peers      []string // initial peers to join with
	raftServer raft.Server
	dataDir    string
	httpAddr   string
	router     *mux.Router
	topo       *topology.Topology
}

func NewRaftServer(r *mux.Router, peers []string, httpAddr string, dataDir string, topo *topology.Topology, pulseSeconds int) *RaftServer {
	s := &RaftServer{
		peers:    peers,
		httpAddr: httpAddr,
		dataDir:  dataDir,
		router:   r,
		topo:     topo,
	}

	if glog.V(4) {
		raft.SetLogLevel(2)
	}

	raft.RegisterCommand(&topology.MaxVolumeIdCommand{})

	var err error
	transporter := raft.NewHTTPTransporter("/cluster", 0)
	transporter.Transport.MaxIdleConnsPerHost = 1024
	glog.V(1).Infof("Starting RaftServer with IP:%v:", httpAddr)

	// Clear old cluster configurations if peers are changed
	if oldPeers, changed := isPeersChanged(s.dataDir, httpAddr, s.peers); changed {
		glog.V(0).Infof("Peers Change: %v => %v", oldPeers, s.peers)
		os.RemoveAll(path.Join(s.dataDir, "conf"))
		os.RemoveAll(path.Join(s.dataDir, "log"))
		os.RemoveAll(path.Join(s.dataDir, "snapshot"))
	}

	s.raftServer, err = raft.NewServer(s.httpAddr, s.dataDir, transporter, nil, topo, "")
	if err != nil {
		glog.V(0).Infoln(err)
		return nil
	}
	transporter.Install(s.raftServer, s)
	s.raftServer.SetHeartbeatInterval(500 * time.Millisecond)
	s.raftServer.SetElectionTimeout(time.Duration(pulseSeconds) * 500 * time.Millisecond)
	s.raftServer.Start()

	s.router.HandleFunc("/cluster/join", s.joinHandler).Methods("POST")
	s.router.HandleFunc("/cluster/status", s.statusHandler).Methods("GET")

	if len(s.peers) > 0 {
		// Join to leader if specified.
		for {
			glog.V(0).Infoln("Joining cluster:", strings.Join(s.peers, ","))
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
			firstJoinError := s.Join(s.peers)
			if firstJoinError != nil {
				glog.V(0).Infoln("No existing server found. Starting as leader in the new cluster.")
				_, err := s.raftServer.Do(&raft.DefaultJoinCommand{
					Name:             s.raftServer.Name(),
					ConnectionString: "http://" + s.httpAddr,
				})
				if err != nil {
					glog.V(0).Infoln(err)
				} else {
					break
				}
			} else {
				break
			}
		}
	} else if s.raftServer.IsLogEmpty() {
		// Initialize the server by joining itself.
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
		glog.V(0).Infoln("Old conf,log,snapshot should have been removed.")
	}

	return s
}

func (s *RaftServer) Peers() (members []string) {
	peers := s.raftServer.Peers()

	for _, p := range peers {
		members = append(members, strings.TrimPrefix(p.ConnectionString, "http://"))
	}

	return
}

func isPeersChanged(dir string, self string, peers []string) (oldPeers []string, changed bool) {
	confPath := path.Join(dir, "conf")
	// open conf file
	b, err := ioutil.ReadFile(confPath)
	if err != nil {
		return oldPeers, true
	}
	conf := &raft.Config{}
	if err = json.Unmarshal(b, conf); err != nil {
		return oldPeers, true
	}

	for _, p := range conf.Peers {
		oldPeers = append(oldPeers, strings.TrimPrefix(p.ConnectionString, "http://"))
	}
	oldPeers = append(oldPeers, self)

	if len(peers) == 0 && len(oldPeers) <= 1 {
		return oldPeers, false
	}

	sort.Strings(peers)
	sort.Strings(oldPeers)

	return oldPeers, !reflect.DeepEqual(peers, oldPeers)

}

// Join joins an existing cluster.
func (s *RaftServer) Join(peers []string) error {
	command := &raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: "http://" + s.httpAddr,
	}

	var err error
	var b bytes.Buffer
	json.NewEncoder(&b).Encode(command)
	for _, m := range peers {
		if m == s.httpAddr {
			continue
		}
		target := fmt.Sprintf("http://%s/cluster/join", strings.TrimSpace(m))
		glog.V(0).Infoln("Attempting to connect to:", target)

		err = postFollowingOneRedirect(target, "application/json", b)

		if err != nil {
			glog.V(0).Infoln("Post returned error: ", err.Error())
			if _, ok := err.(*url.Error); ok {
				// If we receive a network error try the next member
				continue
			}
		} else {
			return nil
		}
	}

	return errors.New("Could not connect to any cluster peers")
}

// a workaround because http POST following redirection misses request body
func postFollowingOneRedirect(target string, contentType string, b bytes.Buffer) error {
	backupReader := bytes.NewReader(b.Bytes())
	resp, err := http.Post(target, contentType, &b)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	statusCode := resp.StatusCode
	data, _ := ioutil.ReadAll(resp.Body)
	reply := string(data)

	if strings.HasPrefix(reply, "\"http") {
		urlStr := reply[1: len(reply)-1]

		glog.V(0).Infoln("Post redirected to ", urlStr)
		resp2, err2 := http.Post(urlStr, contentType, backupReader)
		if err2 != nil {
			return err2
		}
		defer resp2.Body.Close()
		data, _ = ioutil.ReadAll(resp2.Body)
		statusCode = resp2.StatusCode
	}

	glog.V(0).Infoln("Post returned status: ", statusCode, string(data))
	if statusCode != http.StatusOK {
		return errors.New(string(data))
	}

	return nil
}
