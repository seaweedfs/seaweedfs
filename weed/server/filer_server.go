package weed_server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/stats"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/util/grace"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/arangodb"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/cassandra"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/elastic/v7"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/etcd"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/hbase"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/leveldb"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/leveldb2"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/leveldb3"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/mongodb"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/mysql"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/mysql2"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/postgres"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/postgres2"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/redis"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/redis2"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/redis3"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/sqlite"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/ydb"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/notification"
	_ "github.com/seaweedfs/seaweedfs/weed/notification/aws_sqs"
	_ "github.com/seaweedfs/seaweedfs/weed/notification/gocdk_pub_sub"
	_ "github.com/seaweedfs/seaweedfs/weed/notification/google_pub_sub"
	_ "github.com/seaweedfs/seaweedfs/weed/notification/kafka"
	_ "github.com/seaweedfs/seaweedfs/weed/notification/log"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

type FilerOption struct {
	Masters               map[string]pb.ServerAddress
	FilerGroup            string
	Collection            string
	DefaultReplication    string
	DisableDirListing     bool
	MaxMB                 int
	DirListingLimit       int
	DataCenter            string
	Rack                  string
	DataNode              string
	DefaultLevelDbDir     string
	DisableHttp           bool
	Host                  pb.ServerAddress
	recursiveDelete       bool
	Cipher                bool
	SaveToFilerLimit      int64
	ConcurrentUploadLimit int64
	ShowUIDirectoryDelete bool
	DownloadMaxBytesPs    int64
}

type FilerServer struct {
	inFlightDataSize      int64
	inFlightDataLimitCond *sync.Cond

	filer_pb.UnimplementedSeaweedFilerServer
	option         *FilerOption
	secret         security.SigningKey
	filer          *filer.Filer
	filerGuard     *security.Guard
	grpcDialOption grpc.DialOption

	// metrics read from the master
	metricsAddress     string
	metricsIntervalSec int

	// notifying clients
	listenersLock sync.Mutex
	listenersCond *sync.Cond

	// track known metadata listeners
	knownListenersLock sync.Mutex
	knownListeners     map[int32]int32
}

func NewFilerServer(defaultMux, readonlyMux *http.ServeMux, option *FilerOption) (fs *FilerServer, err error) {

	v := util.GetViper()
	signingKey := v.GetString("jwt.filer_signing.key")
	v.SetDefault("jwt.filer_signing.expires_after_seconds", 10)
	expiresAfterSec := v.GetInt("jwt.filer_signing.expires_after_seconds")

	readSigningKey := v.GetString("jwt.filer_signing.read.key")
	v.SetDefault("jwt.filer_signing.read.expires_after_seconds", 60)
	readExpiresAfterSec := v.GetInt("jwt.filer_signing.read.expires_after_seconds")

	fs = &FilerServer{
		option:                option,
		grpcDialOption:        security.LoadClientTLS(util.GetViper(), "grpc.filer"),
		knownListeners:        make(map[int32]int32),
		inFlightDataLimitCond: sync.NewCond(new(sync.Mutex)),
	}
	fs.listenersCond = sync.NewCond(&fs.listenersLock)

	if len(option.Masters) == 0 {
		glog.Fatal("master list is required!")
	}

	fs.filer = filer.NewFiler(option.Masters, fs.grpcDialOption, option.Host, option.FilerGroup, option.Collection, option.DefaultReplication, option.DataCenter, func() {
		fs.listenersCond.Broadcast()
	})
	fs.filer.Cipher = option.Cipher
	// we do not support IP whitelist right now
	fs.filerGuard = security.NewGuard([]string{}, signingKey, expiresAfterSec, readSigningKey, readExpiresAfterSec)

	fs.checkWithMaster()

	go stats.LoopPushingMetric("filer", string(fs.option.Host), fs.metricsAddress, fs.metricsIntervalSec)
	go fs.filer.KeepMasterClientConnected()

	if !util.LoadConfiguration("filer", false) {
		v.SetDefault("leveldb2.enabled", true)
		v.SetDefault("leveldb2.dir", option.DefaultLevelDbDir)
		_, err := os.Stat(option.DefaultLevelDbDir)
		if os.IsNotExist(err) {
			os.MkdirAll(option.DefaultLevelDbDir, 0755)
		}
		glog.V(0).Infof("default to create filer store dir in %s", option.DefaultLevelDbDir)
	} else {
		glog.Warningf("skipping default store dir in %s", option.DefaultLevelDbDir)
	}
	util.LoadConfiguration("notification", false)

	fs.option.recursiveDelete = v.GetBool("filer.options.recursive_delete")
	v.SetDefault("filer.options.buckets_folder", "/buckets")
	fs.filer.DirBucketsPath = v.GetString("filer.options.buckets_folder")
	// TODO deprecated, will be be removed after 2020-12-31
	// replaced by https://github.com/seaweedfs/seaweedfs/wiki/Path-Specific-Configuration
	// fs.filer.FsyncBuckets = v.GetStringSlice("filer.options.buckets_fsync")
	isFresh := fs.filer.LoadConfiguration(v)

	notification.LoadConfiguration(v, "notification.")

	handleStaticResources(defaultMux)
	if !option.DisableHttp {
		defaultMux.HandleFunc("/", fs.filerHandler)
	}
	if defaultMux != readonlyMux {
		handleStaticResources(readonlyMux)
		readonlyMux.HandleFunc("/", fs.readonlyFilerHandler)
	}

	existingNodes := fs.filer.ListExistingPeerUpdates()
	startFromTime := time.Now().Add(-filer.LogFlushInterval)
	if isFresh {
		glog.V(0).Infof("%s bootstrap from peers %+v", option.Host, existingNodes)
		if err := fs.filer.MaybeBootstrapFromPeers(option.Host, existingNodes, startFromTime); err != nil {
			glog.Fatalf("%s bootstrap from %+v", option.Host, existingNodes)
		}
	}
	fs.filer.AggregateFromPeers(option.Host, existingNodes, startFromTime)

	fs.filer.LoadFilerConf()

	fs.filer.LoadRemoteStorageConfAndMapping()

	grace.OnInterrupt(func() {
		fs.filer.Shutdown()
	})

	return fs, nil
}

func (fs *FilerServer) checkWithMaster() {

	isConnected := false
	for !isConnected {
		for _, master := range fs.option.Masters {
			readErr := operation.WithMasterServerClient(false, master, fs.grpcDialOption, func(masterClient master_pb.SeaweedClient) error {
				resp, err := masterClient.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
				if err != nil {
					return fmt.Errorf("get master %s configuration: %v", master, err)
				}
				fs.metricsAddress, fs.metricsIntervalSec = resp.MetricsAddress, int(resp.MetricsIntervalSeconds)
				if fs.option.DefaultReplication == "" {
					fs.option.DefaultReplication = resp.DefaultReplication
				}
				return nil
			})
			if readErr == nil {
				isConnected = true
			} else {
				time.Sleep(7 * time.Second)
			}
		}
	}

}
