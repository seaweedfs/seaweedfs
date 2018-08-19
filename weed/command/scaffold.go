package command

import (
	"io/ioutil"
	"path/filepath"
)

func init() {
	cmdScaffold.Run = runScaffold // break init cycle
}

var cmdScaffold = &Command{
	UsageLine: "scaffold [filer]",
	Short:     "generate basic configuration files",
	Long: `Generate filer.toml with all possible configurations for you to customize.

  `,
}

var (
	outputPath = cmdScaffold.Flag.String("output", "", "if not empty, save the configuration file to this directory")
	config     = cmdScaffold.Flag.String("config", "filer", "the configuration file to generate")
)

func runScaffold(cmd *Command, args []string) bool {

	content := ""
	switch *config {
	case "filer":
		content = FILER_TOML_EXAMPLE
	}
	if content == "" {
		println("need a valid -config option")
		return false
	}

	if *outputPath != "" {
		ioutil.WriteFile(filepath.Join(*outputPath, *config+".toml"), []byte(content), 0x755)
	} else {
		println(content)
	}
	return true
}

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


####################################################
# notification
# sends filer updates for each file to an external message queue
####################################################
[notification.log]
enabled = true

[notification.kafka]
enabled = false
hosts = [
  "localhost:9092"
]
topic = "seaweedfs_filer"

`
)
