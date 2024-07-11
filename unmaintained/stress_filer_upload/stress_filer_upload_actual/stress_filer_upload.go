package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	dir         = flag.String("dir", ".", "upload files under this directory")
	concurrency = flag.Int("c", 1, "concurrent number of uploads")
	times       = flag.Int("n", 1, "repeated number of times")
	destination = flag.String("to", "http://localhost:8888/", "destination directory on filer")

	statsChan = make(chan stat, 8)
)

type stat struct {
	size int64
}

func main() {
	flag.Parse()
	util_http.InitGlobalHttpClient()

	var fileNames []string

	files, err := os.ReadDir(*dir)
	if err != nil {
		log.Fatalf("fail to read dir %v: %v", *dir, err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		fileNames = append(fileNames, filepath.Join(*dir, file.Name()))
	}

	var wg sync.WaitGroup
	for x := 0; x < *concurrency; x++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			rand.Shuffle(len(fileNames), func(i, j int) {
				fileNames[i], fileNames[j] = fileNames[j], fileNames[i]
			})
			for t := 0; t < *times; t++ {
				for _, filename := range fileNames {
					if size, err := uploadFileToFiler(filename, *destination); err == nil {
						statsChan <- stat{
							size: size,
						}
					}
				}
			}
		}()
	}

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		var lastTime time.Time
		var counter, size int64
		for {
			select {
			case stat := <-statsChan:
				size += stat.size
				counter++
			case x := <-ticker.C:
				if !lastTime.IsZero() {
					elapsed := x.Sub(lastTime).Seconds()
					fmt.Fprintf(os.Stdout, "%.2f files/s, %.2f MB/s\n",
						float64(counter)/elapsed,
						float64(size/1024/1024)/elapsed)
				}
				lastTime = x
				size = 0
				counter = 0
			}
		}
	}()

	wg.Wait()

}

func uploadFileToFiler(filename, destination string) (size int64, err error) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	fi, err := file.Stat()

	if !strings.HasSuffix(destination, "/") {
		destination = destination + "/"
	}

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", file.Name())
	if err != nil {
		return 0, fmt.Errorf("fail to create form %v: %v", file.Name(), err)
	}
	_, err = io.Copy(part, file)
	if err != nil {
		return 0, fmt.Errorf("fail to write part %v: %v", file.Name(), err)
	}

	err = writer.Close()
	if err != nil {
		return 0, fmt.Errorf("fail to write part %v: %v", file.Name(), err)
	}

	uri := destination + file.Name()

	request, err := http.NewRequest(http.MethodPost, uri, body)
	if err != nil {
		return 0, fmt.Errorf("http POST %s: %v", uri, err)
	}

	request.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := util_http.GetGlobalHttpClient().Do(request)
	if err != nil {
		return 0, fmt.Errorf("http POST %s: %v", uri, err)
	} else {
		body := &bytes.Buffer{}
		_, err := body.ReadFrom(resp.Body)
		if err != nil {
			return 0, fmt.Errorf("read http POST %s response: %v", uri, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	return fi.Size(), nil
}
