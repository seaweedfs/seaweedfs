package weed_server

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/filer/cassandra_store"
	"github.com/chrislusf/seaweedfs/weed/filer/embedded_filer"
	"github.com/chrislusf/seaweedfs/weed/filer/flat_namespace"
	"github.com/chrislusf/seaweedfs/weed/filer/mysql_store"
	"github.com/chrislusf/seaweedfs/weed/filer/postgres_store"
	"github.com/chrislusf/seaweedfs/weed/filer/redis_store"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type filerConf struct {
	MysqlConf []mysql_store.MySqlConf `json:"mysql"`
	mysql_store.ShardingConf
	PostgresConf *postgres_store.PostgresConf `json:"postgres"`
}

func parseConfFile(confPath string) (*filerConf, error) {
	var setting filerConf
	configFile, err := os.Open(confPath)
	defer configFile.Close()
	if err != nil {
		return nil, err
	}

	jsonParser := json.NewDecoder(configFile)
	if err = jsonParser.Decode(&setting); err != nil {
		return nil, err
	}
	return &setting, nil
}

type FilerServer struct {
	port               string
	master             string
	mnLock             sync.RWMutex
	collection         string
	defaultReplication string
	redirectOnRead     bool
	disableDirListing  bool
	secret             security.Secret
	filer              filer.Filer
	maxMB              int
	masterNodes        *storage.MasterNodes
}

func NewFilerServer(defaultMux, readonlyMux *http.ServeMux, ip string, port int, master string, dir string, collection string,
	replication string, redirectOnRead bool, disableDirListing bool,
	confFile string,
	maxMB int,
	secret string,
	cassandra_server string, cassandra_keyspace string,
	redis_server string, redis_password string, redis_database int,
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

	var setting *filerConf
	if confFile != "" {
		setting, err = parseConfFile(confFile)
		if err != nil {
			return nil, err
		}
	} else {
		setting = new(filerConf)
	}

	if setting.MysqlConf != nil && len(setting.MysqlConf) != 0 {
		mysql_store := mysql_store.NewMysqlStore(setting.MysqlConf, setting.IsSharding, setting.ShardCount)
		fs.filer = flat_namespace.NewFlatNamespaceFiler(master, mysql_store)
	} else if setting.PostgresConf != nil {
		fs.filer = postgres_store.NewPostgresStore(master, *setting.PostgresConf)
	} else if cassandra_server != "" {
		cassandra_store, err := cassandra_store.NewCassandraStore(cassandra_keyspace, cassandra_server)
		if err != nil {
			glog.Fatalf("Can not connect to cassandra server %s with keyspace %s: %v", cassandra_server, cassandra_keyspace, err)
		}
		fs.filer = flat_namespace.NewFlatNamespaceFiler(master, cassandra_store)
	} else if redis_server != "" {
		redis_store := redis_store.NewRedisStore(redis_server, redis_password, redis_database)
		fs.filer = flat_namespace.NewFlatNamespaceFiler(master, redis_store)
	} else {
		if fs.filer, err = embedded_filer.NewFilerEmbedded(master, dir); err != nil {
			glog.Fatalf("Can not start filer in dir %s : %v", dir, err)
			return
		}

		defaultMux.HandleFunc("/admin/mv", fs.moveHandler)
		defaultMux.HandleFunc("/admin/register", fs.registerHandler)
		defaultMux.HandleFunc("/__api__", fs.apiHandler)
	}

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
	statUrl := "http://" + masterNode + "/stats"
	glog.V(4).Infof("Connecting to %s ...", statUrl)
	_, e := util.Get(statUrl)
	return e
}
