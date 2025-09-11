package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

func TestKafkaGo_ProduceOnly(t *testing.T) {
	// Start gateway
	gatewayServer := gateway.NewServer(gateway.Options{Listen: "127.0.0.1:0"})
	go func() {
		if err := gatewayServer.Start(); err != nil {
			t.Errorf("Failed to start gateway: %v", err)
		}
	}()
	defer gatewayServer.Close()

	time.Sleep(100 * time.Millisecond)

	host, port := gatewayServer.GetListenerAddr()
	addr := fmt.Sprintf("%s:%d", host, port)
		topic := "kgo-produce-only"
	gatewayServer.GetHandler().AddTopicForTesting(topic, 1)

	w := &kafka.Writer{
		Addr:         kafka.TCP(addr),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 50 * time.Millisecond,
	}
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := w.WriteMessages(ctx, kafka.Message{Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatalf("kafka-go produce failed: %v", err)
	}
}
