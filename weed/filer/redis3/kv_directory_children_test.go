package redis3

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/stvp/tempredis"
	"strconv"
	"testing"
	"time"
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

func yTestNameList(t *testing.T) {
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
		nameList := LoadItemList(data, "/yyy/bin", client, store, maxNameBatchSizeLimit)
		nameList.WriteName(name)

		nameList.ListNames("", func(name string) bool {
			println(name)
			return true
		})

		if nameList.HasChanges() {
			data = nameList.ToBytes()
		}
		println()
	}

	nameList := LoadItemList(data, "/yyy/bin", client, store, maxNameBatchSizeLimit)
	nameList.ListNames("", func(name string) bool {
		println(name)
		return true
	})

}

func yBenchmarkNameList(b *testing.B) {

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
		nameList := LoadItemList(data, "/yyy/bin", client, store, maxNameBatchSizeLimit)

		nameList.WriteName(strconv.Itoa(i) + "namexxxxxxxxxxxxxxxxxxx")

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
		client.ZAddNX(context.Background(), "/yyy/bin", redis.Z{Score: 0, Member: strconv.Itoa(i) + "namexxxxxxxxxxxxxxxxxxx"})
	}
}

func xTestNameListAdd(t *testing.T) {

	server, err := tempredis.Start(tempredis.Config{})
	if err != nil {
		panic(err)
	}
	defer server.Term()

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	client.FlushAll(context.Background())

	N := 364800

	ts0 := time.Now()
	store := newSkipListElementStore("/y", client)
	var data []byte
	nameList := LoadItemList(data, "/y", client, store, 100000)
	for i := 0; i < N; i++ {
		nameList.WriteName(fmt.Sprintf("%8d", i))
	}

	ts1 := time.Now()

	for i := 0; i < N; i++ {
		client.ZAddNX(context.Background(), "/x", redis.Z{Score: 0, Member: fmt.Sprintf("name %8d", i)})
	}
	ts2 := time.Now()

	fmt.Printf("%v %v", ts1.Sub(ts0), ts2.Sub(ts1))

	/*
		keys := client.Keys(context.Background(), "/*m").Val()
		for _, k := range keys {
			println("key", k)
			for i, v := range client.ZRangeByLex(context.Background(), k, &redis.ZRangeBy{
				Min:    "-",
				Max:    "+",
			}).Val() {
				println(" ", i, v)
			}
		}
	*/
}

func xBenchmarkNameList(b *testing.B) {

	server, err := tempredis.Start(tempredis.Config{})
	if err != nil {
		panic(err)
	}
	defer server.Term()

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	store := newSkipListElementStore("/yyy/bin", client)
	var data []byte
	for i := 0; i < b.N; i++ {
		nameList := LoadItemList(data, "/yyy/bin", client, store, maxNameBatchSizeLimit)

		nameList.WriteName(fmt.Sprintf("name %8d", i))

		if nameList.HasChanges() {
			data = nameList.ToBytes()
		}
	}
}

func xBenchmarkRedis(b *testing.B) {

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	for i := 0; i < b.N; i++ {
		client.ZAddNX(context.Background(), "/xxx/bin", redis.Z{Score: 0, Member: fmt.Sprintf("name %8d", i)})
	}
}
