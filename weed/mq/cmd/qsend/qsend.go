package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/client"
	"github.com/seaweedfs/seaweedfs/weed/mq/messages"
	"os"
	"time"
)

var (
	master = flag.String("master", "localhost:9333", "master csv list")
	topic  = flag.String("topic", "", "topic name")
)

func main() {
	flag.Parse()

	publisher := client.NewPublisher(&client.PublisherOption{
		Masters: *master,
		Topic:   *topic,
	})

	err := eachLineStdin(func(line string) error {
		if len(line) > 0 {
			if err := publisher.Publish(&messages.Message{
				Key:        nil,
				Content:    []byte(line),
				Properties: nil,
				Ts:         time.Time{},
			}); err != nil {
				return err
			}
		}
		return nil
	})

	publisher.Shutdown()

	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
}

func eachLineStdin(eachLineFn func(string) error) error {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text()
		if err := eachLineFn(text); err != nil {
			return err
		}
	}

	// handle error
	if scanner.Err() != nil {
		return fmt.Errorf("scan stdin: %v", scanner.Err())
	}

	return nil
}
