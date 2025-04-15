package main

import (
	"context"
	"flag"
	"fmt"
	"reflect"

	"github.com/spf13/viper"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/cassandra"
	"github.com/seaweedfs/seaweedfs/weed/filer/mysql"
	"github.com/seaweedfs/seaweedfs/weed/filer/mysql2"
	"github.com/seaweedfs/seaweedfs/weed/filer/postgres"
	"github.com/seaweedfs/seaweedfs/weed/filer/postgres2"
	"github.com/seaweedfs/seaweedfs/weed/filer/tikv"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	metaStoreMigrate  FilerStoreMigrate
	filerAddress      *string
	filerDirectory    *string
	backupFilerConfig *string
	clientId          int32
)

type FilerStoreMigrate struct {
	targetStore    filer.FilerStore
	grpcDialOption grpc.DialOption
}

func init() {
	filerAddress = flag.String("filer", "localhost:8888", "filer hostname:port")
	filerDirectory = flag.String("filerDir", "/", "a folder on the filer")
	backupFilerConfig = flag.String("config", "", "path to filer.toml specifying source/target filer store")
	clientId = util.RandomInt32()
	// add commonly used filer stores
	filer.Stores = append(filer.Stores, &tikv.TikvStore{})
	filer.Stores = append(filer.Stores, &cassandra.CassandraStore{})
	filer.Stores = append(filer.Stores, &mysql.MysqlStore{})
	filer.Stores = append(filer.Stores, &mysql2.MysqlStore2{})
	filer.Stores = append(filer.Stores, &postgres.PostgresStore{})
	filer.Stores = append(filer.Stores, &postgres2.PostgresStore2{})
}

func main() {
	flag.Parse()

	util.LoadSecurityConfiguration()
	metaStoreMigrate.grpcDialOption = security.LoadClientTLS(util.GetViper(), "grpc.client")

	v := viper.New()
	v.SetConfigFile(*backupFilerConfig)

	if err := v.ReadInConfig(); err != nil { // Handle errors reading the config file
		glog.Fatalf("Failed to load %s file: %v\nPlease use this command to generate the a %s.toml file\n"+
			"    weed scaffold -config=%s -output=.\n\n\n",
			*backupFilerConfig, err, "migrate_filer", "filer")
	}

	if err := metaStoreMigrate.initStore(v); err != nil {
		glog.V(0).Infof("init migrate filer store: %v", err)
		return
	}

	glog.V(0).Infof("traversing metadata tree...")
	if err := metaStoreMigrate.traverseMetadata(); err != nil {
		glog.Errorf("traverse meta data: %v", err)
	}

	return
}

func (metaMigrate *FilerStoreMigrate) initStore(v *viper.Viper) error {
	// load configuration for default filer store
	hasDefaultStoreConfigured := false
	for _, store := range filer.Stores {
		glog.V(0).Infof("checking %s store configuration, enabled: %v, migrate_target: %v", store.GetName(), v.GetBool(store.GetName()+".enabled"), v.GetBool(store.GetName()+".migrate_target"))
		if v.GetBool(store.GetName()+".enabled") && v.GetBool(store.GetName()+".migrate_target") {
			store = reflect.New(reflect.ValueOf(store).Elem().Type()).Interface().(filer.FilerStore)
			if err := store.Initialize(v, store.GetName()+"."); err != nil {
				glog.Fatalf("failed to initialize target store for %s: %+v", store.GetName(), err)
			}
			hasDefaultStoreConfigured = true
			glog.V(0).Infof("configured target filer store to %s", store.GetName())
			metaMigrate.targetStore = filer.NewFilerStoreWrapper(store)
			break
		}
	}
	if !hasDefaultStoreConfigured {
		return fmt.Errorf("no filer store enabled in %s", v.ConfigFileUsed())
	}

	return nil
}

func (metaMigrate *FilerStoreMigrate) traverseMetadata() (err error) {
	var saveErr error

	traverseErr := filer_pb.TraverseBfs(metaMigrate, util.FullPath(*filerDirectory), func(parentPath util.FullPath, entry *filer_pb.Entry) {
		// filer /topics directory
		if parentPath.Child(entry.Name) == util.FullPath("/topics") {
			println("skip /topics directory: ", parentPath.Child(entry.Name))
			return
		}

		println("+", parentPath.Child(entry.Name))
		if err := metaMigrate.targetStore.InsertEntry(context.Background(), filer.FromPbEntry(string(parentPath), entry)); err != nil {
			saveErr = fmt.Errorf("insert entry error: %v\n", err)
			return
		}

	})

	if traverseErr != nil {
		return fmt.Errorf("traverse: %v", traverseErr)
	}
	return saveErr
}

var _ = filer_pb.FilerClient(&FilerStoreMigrate{})

func (metaMigrate *FilerStoreMigrate) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithFilerClient(streamingMode, clientId, pb.ServerAddress(*filerAddress), metaMigrate.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		return fn(client)
	})

}

func (metaBackup *FilerStoreMigrate) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

func (metaBackup *FilerStoreMigrate) GetDataCenter() string {
	return ""
}
