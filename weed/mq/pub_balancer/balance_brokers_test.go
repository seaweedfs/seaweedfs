package pub_balancer

import (
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"reflect"
	"testing"
)

func TestBalanceTopicPartitionOnBrokers(t *testing.T) {

	brokers := cmap.New[*BrokerStats]()
	broker1Stats := &BrokerStats{
		TopicPartitionCount: 1,
		CpuUsagePercent:     1,
		TopicPartitionStats: cmap.New[*TopicPartitionStats](),
	}
	broker1Stats.TopicPartitionStats.Set("topic1:0", &TopicPartitionStats{
		TopicPartition: topic.TopicPartition{
			Topic:     topic.Topic{Namespace: "topic1", Name: "topic1"},
			Partition: topic.Partition{RangeStart: 0, RangeStop: 512, RingSize: 1024},
		},
	})
	broker2Stats := &BrokerStats{
		TopicPartitionCount: 2,
		CpuUsagePercent:     1,
		TopicPartitionStats: cmap.New[*TopicPartitionStats](),
	}
	broker2Stats.TopicPartitionStats.Set("topic1:1", &TopicPartitionStats{
		TopicPartition: topic.TopicPartition{
			Topic:     topic.Topic{Namespace: "topic1", Name: "topic1"},
			Partition: topic.Partition{RangeStart: 512, RangeStop: 1024, RingSize: 1024},
		},
	})
	broker2Stats.TopicPartitionStats.Set("topic2:0", &TopicPartitionStats{
		TopicPartition: topic.TopicPartition{
			Topic:     topic.Topic{Namespace: "topic2", Name: "topic2"},
			Partition: topic.Partition{RangeStart: 0, RangeStop: 1024, RingSize: 1024},
		},
	})
	brokers.Set("broker1", broker1Stats)
	brokers.Set("broker2", broker2Stats)

	type args struct {
		brokers cmap.ConcurrentMap[string, *BrokerStats]
	}
	tests := []struct {
		name string
		args args
		want BalanceAction
	}{
		{
			name: "test",
			args: args{
				brokers: brokers,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BalanceTopicPartitionOnBrokers(tt.args.brokers); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BalanceTopicPartitionOnBrokers() = %v, want %v", got, tt.want)
			}
		})
	}
}
