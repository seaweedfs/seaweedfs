package weed_server

import (
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/filer2"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/cassandra"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/leveldb"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/memdb"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/mysql"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/postgres"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/redis"
)

type FilerServer struct {
	port               string
	master             string
	mnLock             sync.RWMutex
	collection         string
	defaultReplication string
	redirectOnRead     bool
	disableDirListing  bool
	secret             security.Secret
	filer              *filer2.Filer
	maxMB              int
	masterNodes        *storage.MasterNodes
}

func NewFilerServer(defaultMux, readonlyMux *http.ServeMux, ip string, port int, master string, collection string,
	replication string, redirectOnRead bool, disableDirListing bool,
	maxMB int,
	secret string,
) (fs *FilerServer, err error) {
	fs = &FilerServer{
		master:             master,
		collection:         collection,
		defaultReplication: replication,
		redirectOnRead:     redirectOnRead,
		disableDirListing:  disableDirListing,
		maxMB:              maxMB,
		port:               ip + ":" + strconv.Itoa(port),
	}
	fs.filer = filer2.NewFiler(master)

	fs.filer.LoadConfiguration()

	defaultMux.HandleFunc("/admin/register", fs.registerHandler)
	defaultMux.HandleFunc("/", fs.filerHandler)
	if defaultMux != readonlyMux {
		readonlyMux.HandleFunc("/", fs.readonlyFilerHandler)
	}

	go func() {
		connected := true

		fs.masterNodes = storage.NewMasterNodes(fs.master)
		glog.V(0).Infof("Filer server bootstraps with master %s", fs.getMasterNode())

		for {
			glog.V(4).Infof("Filer server sending to master %s", fs.getMasterNode())
			master, err := fs.detectHealthyMaster(fs.getMasterNode())
			if err == nil {
				if !connected {
					connected = true
					if fs.getMasterNode() != master {
						fs.setMasterNode(master)
					}
					glog.V(0).Infoln("Filer Server Connected with master at", master)
				}
			} else {
				glog.V(1).Infof("Filer Server Failed to talk with master %s: %v", fs.getMasterNode(), err)
				if connected {
					connected = false
				}
			}
			if connected {
				time.Sleep(time.Duration(float32(10*1e3)*(1+rand.Float32())) * time.Millisecond)
			} else {
				time.Sleep(time.Duration(float32(10*1e3)*0.25) * time.Millisecond)
			}
		}
	}()

	return fs, nil
}

func (fs *FilerServer) jwt(fileId string) security.EncodedJwt {
	return security.GenJwt(fs.secret, fileId)
}

func (fs *FilerServer) getMasterNode() string {
	fs.mnLock.RLock()
	defer fs.mnLock.RUnlock()
	return fs.master
}

func (fs *FilerServer) setMasterNode(masterNode string) {
	fs.mnLock.Lock()
	defer fs.mnLock.Unlock()
	fs.master = masterNode
}

func (fs *FilerServer) detectHealthyMaster(masterNode string) (master string, e error) {
	if e = checkMaster(masterNode); e != nil {
		fs.masterNodes.Reset()
		for i := 0; i <= 3; i++ {
			master, e = fs.masterNodes.FindMaster()
			if e != nil {
				continue
			} else {
				if e = checkMaster(master); e == nil {
					break
				}
			}
		}
	} else {
		master = masterNode
	}
	return
}

func checkMaster(masterNode string) error {
	statUrl := "http://" + masterNode + "/stats/health"
	glog.V(4).Infof("Connecting to %s ...", statUrl)
	_, e := util.Get(statUrl)
	return e
}
