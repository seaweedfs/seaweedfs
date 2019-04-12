package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	size        = flag.Int("size", 1024, "file size")
	concurrency = flag.Int("c", 4, "concurrent number of uploads")
	times       = flag.Int("n", 1024, "repeated number of times")
	destination = flag.String("to", "http://localhost:8888/", "destination directory on filer")

	statsChan = make(chan stat, 8)
)

type stat struct {
	size int64
}

func main() {

	flag.Parse()

	data := make([]byte, *size)
	println("data len", len(data))

	var wg sync.WaitGroup
	for x := 0; x < *concurrency; x++ {
		wg.Add(1)

		client := &http.Client{}

		go func() {
			defer wg.Done()
			for t := 0; t < *times; t++ {
				if size, err := uploadFileToFiler(client, data, fmt.Sprintf("file%d", t), *destination); err == nil {
					statsChan <- stat{
						size: size,
					}
				}else {
					log.Fatalf("upload: %v", err)
				}
			}
		}()
	}

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)

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

func uploadFileToFiler(client *http.Client, data []byte, filename, destination string) (size int64, err error) {

	if !strings.HasSuffix(destination, "/") {
		destination = destination + "/"
	}

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		return 0, fmt.Errorf("fail to create form %v: %v", filename, err)
	}

	part.Write(data)

	err = writer.Close()
	if err != nil {
		return 0, fmt.Errorf("fail to write part %v: %v", filename, err)
	}

	uri := destination + filename

	request, err := http.NewRequest("POST", uri, body)
	request.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := client.Do(request)
	if err != nil {
		return 0, fmt.Errorf("http POST %s: %v", uri, err)
	} else {
		body := &bytes.Buffer{}
		_, err := body.ReadFrom(resp.Body)
		if err != nil {
			return 0, fmt.Errorf("read http POST %s response: %v", uri, err)
		}
		resp.Body.Close()
	}

	return int64(len(data)), nil
}
