package filer2

import (
	"os"

	"github.com/chrislusf/seaweedfs/weed/glog"
			"github.com/spf13/viper"
)

const (
	FILER_TOML_EXAMPLE = `
# A sample TOML config file for SeaweedFS filer store

[memory]
# local in memory, mostly for testing purpose
enabled = false

[leveldb]
# local on disk, mostly for simple single-machine setup, fairly scalable
enabled = false
dir = "."					# directory to store level db files

####################################################
# multiple filers on shared storage, fairly scalable
####################################################

[mysql]
# CREATE TABLE IF NOT EXISTS filemeta (
#   dirhash     BIGINT        COMMENT 'first 64 bits of MD5 hash value of directory field',
#   name        VARCHAR(1000) COMMENT 'directory or file name',
#   directory   VARCHAR(4096) COMMENT 'full path to parent directory',
#   meta        BLOB,
#   PRIMARY KEY (dirhash, name)
# ) DEFAULT CHARSET=utf8;
enabled = true
hostname = "localhost"
port = 3306
username = "root"
password = ""
database = ""              # create or use an existing database
connection_max_idle = 2
connection_max_open = 100

[postgres]
# CREATE TABLE IF NOT EXISTS filemeta (
#   dirhash     BIGINT,
#   name        VARCHAR(1000),
#   directory   VARCHAR(4096),
#   meta        bytea,
#   PRIMARY KEY (dirhash, name)
# );
enabled = false
hostname = "localhost"
port = 5432
username = "postgres"
password = ""
database = ""              # create or use an existing database
sslmode = "disable"
connection_max_idle = 100
connection_max_open = 100

[cassandra]
# CREATE TABLE filemeta (
#    directory varchar,
#    name varchar,
#    meta blob,
#    PRIMARY KEY (directory, name)
# ) WITH CLUSTERING ORDER BY (name ASC);
enabled = false
keyspace="seaweedfs"
hosts=[
	"localhost:9042",
]

[redis]
enabled = true
address  = "localhost:6379"
password = ""
db = 0

[redis_cluster]
enabled = false
addresses = [
    "localhost:6379",
]

`
)

var (
	Stores []FilerStore
)

func (f *Filer) LoadConfiguration(config *viper.Viper) {

	for _, store := range Stores {
		if config.GetBool(store.GetName() + ".enabled") {
			viperSub := config.Sub(store.GetName())
			if err := store.Initialize(viperSub); err != nil {
				glog.Fatalf("Failed to initialize store for %s: %+v",
					store.GetName(), err)
			}
			f.SetStore(store)
			glog.V(0).Infof("Configure filer for %s", store.GetName())
			return
		}
	}

	println()
	println("Supported filer stores are:")
	for _, store := range Stores {
		println("    " + store.GetName())
	}

	os.Exit(-1)
}
