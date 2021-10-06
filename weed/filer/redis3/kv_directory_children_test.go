package redis3

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/util/skiplist"
	"github.com/go-redis/redis/v8"
	"github.com/stvp/tempredis"
	"strconv"
	"testing"
)

var names = []string{
	"cassandra.in.sh",
	"cassandra",
	"debug-cql.bat",
	"nodetool",
	"nodetool.bat",
	"source-conf.ps1",
	"sstableloader",
	"sstableloader.bat",
	"sstablescrub",
	"sstablescrub.bat",
	"sstableupgrade",
	"sstableupgrade.bat",
	"sstableutil",
	"sstableutil.bat",
	"sstableverify",
	"sstableverify.bat",
	"stop-server",
	"stop-server.bat",
	"stop-server.ps1",
	"cassandra.in.bat",
	"cqlsh.py",
	"cqlsh",
	"cassandra.ps1",
	"cqlsh.bat",
	"debug-cql",
	"cassandra.bat",
}

func TestNameList(t *testing.T) {
	server, err := tempredis.Start(tempredis.Config{})
	if err != nil {
		panic(err)
	}
	defer server.Term()

	client := redis.NewClient(&redis.Options{
		Network: "unix",
		Addr:    server.Socket(),
	})

	store := newSkipListElementStore("/yyy/bin", client)
	var data []byte
	for _, name := range names {
		nameList := skiplist.LoadNameList(data, store, maxNameBatchSizeLimit)
		nameList.WriteName(name)

		nameList.ListNames("", func(name string) bool {
			return true
		})

		if nameList.HasChanges() {
			data = nameList.ToBytes()
		}
		println()
	}

	nameList := skiplist.LoadNameList(data, store, maxNameBatchSizeLimit)
	nameList.ListNames("", func(name string) bool {
		println(name)
		return true
	})

}

func BenchmarkNameList(b *testing.B) {

	server, err := tempredis.Start(tempredis.Config{})
	if err != nil {
		panic(err)
	}
	defer server.Term()

	client := redis.NewClient(&redis.Options{
		Network: "unix",
		Addr:    server.Socket(),
	})

	store := newSkipListElementStore("/yyy/bin", client)
	var data []byte
	for i := 0; i < b.N; i++ {
		nameList := skiplist.LoadNameList(data, store, maxNameBatchSizeLimit)

		nameList.WriteName("name"+strconv.Itoa(i))

		if nameList.HasChanges() {
			data = nameList.ToBytes()
		}
	}
}

func BenchmarkRedis(b *testing.B) {

	server, err := tempredis.Start(tempredis.Config{})
	if err != nil {
		panic(err)
	}
	defer server.Term()

	client := redis.NewClient(&redis.Options{
		Network: "unix",
		Addr:    server.Socket(),
	})

	for i := 0; i < b.N; i++ {
		client.ZAddNX(context.Background(),"/yyy/bin", &redis.Z{Score: 0, Member: "name"+strconv.Itoa(i)})
	}
}
