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

`
)

var (
	Stores []FilerStore
)

func (f *Filer) LoadConfiguration() {

	// find a filer store
	viper.SetConfigName("filer")                 // name of config file (without extension)
	viper.AddConfigPath(".")                     // optionally look for config in the working directory
	viper.AddConfigPath("$HOME/.seaweedfs")      // call multiple times to add many search paths
	viper.AddConfigPath("/etc/seaweedfs/")       // path to look for the config file in
	if err := viper.ReadInConfig(); err != nil { // Handle errors reading the config file
		glog.Fatalf("Failed to load filer.toml file from current directory, or $HOME/.seaweedfs/, or /etc/seaweedfs/" +
			"\n\nPlease follow this example and add a filer.toml file to " +
			"current directory, or $HOME/.seaweedfs/, or /etc/seaweedfs/:\n" + FILER_TOML_EXAMPLE)
	}

	glog.V(0).Infof("Reading filer configuration from %s", viper.ConfigFileUsed())
	for _, store := range Stores {
		if viper.GetBool(store.GetName() + ".enabled") {
			viperSub := viper.Sub(store.GetName())
			if err := store.Initialize(viperSub); err != nil {
				glog.Fatalf("Failed to initialize store for %s: %+v",
					store.GetName(), err)
			}
			f.SetStore(store)
			glog.V(0).Infof("Configure filer for %s from %s", store.GetName(), viper.ConfigFileUsed())
			return
		}
	}

	println()
	println("Supported filer stores are:")
	for _, store := range Stores {
		println("    " + store.GetName())
	}

	println()
	println("Please configure a supported filer store in", viper.ConfigFileUsed())
	println()

	os.Exit(-1)
}
