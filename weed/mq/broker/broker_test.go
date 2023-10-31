package broker

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

var lis *bufconn.Listener

func init() {
	lis = bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	mq_pb.RegisterSeaweedMessagingServer(server, &MessageQueueBroker{})
	go func() {
		if err := server.Serve(lis); err != nil {
			fmt.Printf("Server exited with error: %v", err)
		}
	}()
}

func bufDialer(string, time.Duration) (net.Conn, error) {
	return lis.Dial()
}

func TestMessageQueueBroker_ListTopics(t *testing.T) {
	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := mq_pb.NewSeaweedMessagingClient(conn)
	request := &mq_pb.ListTopicsRequest{}

	_, err = client.ListTopics(context.Background(), request)
	if err == nil {
		t.Fatalf("Add failed: %v", err)
	}

}
