package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	master           = flag.String("master", "127.0.0.1:9333", "the master server")
	repeat           = flag.Int("n", 5, "repeat how many times")
	garbageThreshold = flag.Float64("garbageThreshold", 0.3, "garbageThreshold")
)

func main() {
	flag.Parse()

	util.LoadConfiguration("security", false)
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	genFile(grpcDialOption, 0)

	for i := 0; i < *repeat; i++ {
		// create 2 files, and delete one of them

		assignResult, targetUrl := genFile(grpcDialOption, i)

		util.Delete(targetUrl, string(assignResult.Auth))

		println("vacuum", i, "threshold", *garbageThreshold)
		util.Get(fmt.Sprintf("http://%s/vol/vacuum?garbageThreshold=%f", *master, *garbageThreshold))

	}

}

func genFile(grpcDialOption grpc.DialOption, i int) (*operation.AssignResult, string) {
	assignResult, err := operation.Assign(*master, grpcDialOption, &operation.VolumeAssignRequest{Count: 1})
	if err != nil {
		log.Fatalf("assign: %v", err)
	}

	data := make([]byte, 1024)
	rand.Read(data)

	targetUrl := fmt.Sprintf("http://%s/%s", assignResult.Url, assignResult.Fid)

	_, err = operation.UploadData(targetUrl, fmt.Sprintf("test%d", i), false, data, false, "bench/test", nil, assignResult.Auth)
	if err != nil {
		log.Fatalf("upload: %v", err)
	}
	return assignResult, targetUrl
}
