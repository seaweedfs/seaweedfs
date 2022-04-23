package command

import (
	_ "net/http/pprof"

	_ "github.com/chrislusf/seaweedfs/weed/remote_storage/azure"
	_ "github.com/chrislusf/seaweedfs/weed/remote_storage/gcs"
	_ "github.com/chrislusf/seaweedfs/weed/remote_storage/hdfs"
	_ "github.com/chrislusf/seaweedfs/weed/remote_storage/s3"

	_ "github.com/chrislusf/seaweedfs/weed/replication/sink/azuresink"
	_ "github.com/chrislusf/seaweedfs/weed/replication/sink/b2sink"
	_ "github.com/chrislusf/seaweedfs/weed/replication/sink/filersink"
	_ "github.com/chrislusf/seaweedfs/weed/replication/sink/gcssink"
	_ "github.com/chrislusf/seaweedfs/weed/replication/sink/localsink"
	_ "github.com/chrislusf/seaweedfs/weed/replication/sink/s3sink"

	_ "github.com/chrislusf/seaweedfs/weed/filer/arangodb"
	_ "github.com/chrislusf/seaweedfs/weed/filer/cassandra"
	_ "github.com/chrislusf/seaweedfs/weed/filer/elastic/v7"
	_ "github.com/chrislusf/seaweedfs/weed/filer/etcd"
	_ "github.com/chrislusf/seaweedfs/weed/filer/hbase"
	_ "github.com/chrislusf/seaweedfs/weed/filer/leveldb"
	_ "github.com/chrislusf/seaweedfs/weed/filer/leveldb2"
	_ "github.com/chrislusf/seaweedfs/weed/filer/leveldb3"
	_ "github.com/chrislusf/seaweedfs/weed/filer/mongodb"
	_ "github.com/chrislusf/seaweedfs/weed/filer/mysql"
	_ "github.com/chrislusf/seaweedfs/weed/filer/mysql2"
	_ "github.com/chrislusf/seaweedfs/weed/filer/postgres"
	_ "github.com/chrislusf/seaweedfs/weed/filer/postgres2"
	_ "github.com/chrislusf/seaweedfs/weed/filer/redis"
	_ "github.com/chrislusf/seaweedfs/weed/filer/redis2"
	_ "github.com/chrislusf/seaweedfs/weed/filer/redis3"
	_ "github.com/chrislusf/seaweedfs/weed/filer/sqlite"
)
