package sub_coordinator

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/stretchr/testify/assert"
)

var partitions = []topic.Partition{
	{
		RangeStart: 0,
		RangeStop:  1,
		RingSize:   3,
		UnixTimeNs: 0,
	},
	{
		RangeStart: 1,
		RangeStop:  2,
		RingSize:   3,
		UnixTimeNs: 0,
	},
	{
		RangeStart: 2,
		RangeStop:  3,
		RingSize:   3,
		UnixTimeNs: 0,
	},
}

func TestAddConsumerInstance(t *testing.T) {
	market := NewMarket(partitions, 10*time.Second)

	consumer := &ConsumerGroupInstance{
		InstanceId:        "first",
		MaxPartitionCount: 2,
	}
	err := market.AddConsumerInstance(consumer)

	assert.Nil(t, err)
	time.Sleep(1 * time.Second) // Allow time for background rebalancing
	market.ShutdownMarket()
	for adjustment := range market.AdjustmentChan {
		fmt.Printf("%+v\n", adjustment)
	}
}

func TestMultipleConsumerInstances(t *testing.T) {
	market := NewMarket(partitions, 10*time.Second)

	market.AddConsumerInstance(&ConsumerGroupInstance{
		InstanceId:        "first",
		MaxPartitionCount: 2,
	})
	market.AddConsumerInstance(&ConsumerGroupInstance{
		InstanceId:        "second",
		MaxPartitionCount: 2,
	})
	market.AddConsumerInstance(&ConsumerGroupInstance{
		InstanceId:        "third",
		MaxPartitionCount: 2,
	})

	time.Sleep(1 * time.Second) // Allow time for background rebalancing
	market.ShutdownMarket()
	for adjustment := range market.AdjustmentChan {
		fmt.Printf("%+v\n", adjustment)
	}
}

func TestConfirmAdjustment(t *testing.T) {
	market := NewMarket(partitions, 1*time.Second)

	market.AddConsumerInstance(&ConsumerGroupInstance{
		InstanceId:        "first",
		MaxPartitionCount: 2,
	})
	market.AddConsumerInstance(&ConsumerGroupInstance{
		InstanceId:        "second",
		MaxPartitionCount: 2,
	})
	market.AddConsumerInstance(&ConsumerGroupInstance{
		InstanceId:        "third",
		MaxPartitionCount: 2,
	})

	go func() {
		time.Sleep(5 * time.Second) // Allow time for background rebalancing
		market.ShutdownMarket()
	}()
	go func() {
		time.Sleep(2 * time.Second)
		market.RemoveConsumerInstance("third")
	}()

	for adjustment := range market.AdjustmentChan {
		fmt.Printf("%+v\n", adjustment)
		market.ConfirmAdjustment(adjustment)
	}

}
