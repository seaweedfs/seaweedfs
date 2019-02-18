package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/spf13/viper"
	"log"
	"math/rand"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	master = flag.String("master", "127.0.0.1:9333", "the master server")
	repeat = flag.Int("n", 5, "repeat how many times")
)

func main() {
	flag.Parse()

	weed_server.LoadConfiguration("security", false)
	grpcDialOption := security.LoadClientTLS(viper.Sub("grpc"), "client")

	for i := 0; i < *repeat; i++ {
		assignResult, err := operation.Assign(*master, grpcDialOption, &operation.VolumeAssignRequest{Count: 1})
		if err != nil {
			log.Fatalf("assign: %v", err)
		}

		data := make([]byte, 1024)
		rand.Read(data)
		reader := bytes.NewReader(data)

		targetUrl := fmt.Sprintf("http://%s/%s", assignResult.Url, assignResult.Fid)

		_, err = operation.Upload(targetUrl, fmt.Sprintf("test%d", i), reader, false, "", nil, assignResult.Auth)
		if err != nil {
			log.Fatalf("upload: %v", err)
		}

		util.Delete(targetUrl, string(assignResult.Auth))

		util.Get(fmt.Sprintf("http://%s/vol/vacuum", *master))

	}

}
