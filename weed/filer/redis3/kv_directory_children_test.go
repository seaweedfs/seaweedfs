package redis3

import (
	"context"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stvp/tempredis"
)

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
