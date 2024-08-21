package sub_coordinator

import (
	"errors"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"sync"
	"time"
)

/*
Market is a data structure that keeps track of the state of the consumer group instances and the partitions.

When rebalancing, the market will try to balance the load of the partitions among the consumer group instances.
For each loop, the market will:
* If a consumer group instance has more partitions than the average, it will unassign some partitions.
* If a consumer group instance has less partitions than the average, it will assign some partitions.

Trigger rebalance when:
* A new consumer group instance is added
* Some partitions are unassigned from a consumer group instance.

If multiple reblance requests are received, after a certain period, the market will only process the latest request.

However, if the number of unassigned partition is increased to exactly the total number of partitions,
and total partitions are less than or equal to the sum of the max partition count of all consumer group instances,
the market will process the request immediately.
This is to ensure a partition can be migrated to another consumer group instance as soon as possible.

Emit these adjustments to the subscriber coordinator:
* Assign a partition to a consumer group instance
* Unassign a partition from a consumer group instance

Because the adjustment is sent to the subscriber coordinator, the market will keep track of the inflight adjustments.
The subscriber coordinator will send back the response to the market when the adjustment is processed.
If the adjustment is older than a certain time(inflightAdjustmentTTL), it would be considered expired.
Otherwise, the adjustment is considered inflight, so it would be used when calculating the load.

Later features:
* A consumer group instance is not keeping up with the load.

Since a coordinator, and thus the market, may be restarted or moved to another node, the market should be able to recover the state from the subscriber coordinator.
The subscriber coordinator should be able to send the current state of the consumer group instances and the partitions to the market.

*/

type PartitionSlot struct {
	Partition  topic.Partition
	AssignedTo *ConsumerGroupInstance // Track the consumer assigned to this partition slot
}

type Adjustment struct {
	isAssign  bool
	partition topic.Partition
	consumer  ConsumerGroupInstanceId
	ts        time.Time
}

type Market struct {
	mu                    sync.Mutex
	partitions            map[topic.Partition]*PartitionSlot
	consumerInstances     map[ConsumerGroupInstanceId]*ConsumerGroupInstance
	AdjustmentChan        chan *Adjustment
	inflightAdjustments   []*Adjustment
	inflightAdjustmentTTL time.Duration
	lastBalancedTime      time.Time
	stopChan              chan struct{}
	balanceRequestChan    chan struct{}
	hasBalanceRequest     bool
}

func NewMarket(partitions []topic.Partition, inflightAdjustmentTTL time.Duration) *Market {
	partitionMap := make(map[topic.Partition]*PartitionSlot)
	for _, partition := range partitions {
		partitionMap[partition] = &PartitionSlot{
			Partition: partition,
		}
	}
	m := &Market{
		partitions:            partitionMap,
		consumerInstances:     make(map[ConsumerGroupInstanceId]*ConsumerGroupInstance),
		AdjustmentChan:        make(chan *Adjustment, 100),
		inflightAdjustmentTTL: inflightAdjustmentTTL,
		stopChan:              make(chan struct{}),
		balanceRequestChan:    make(chan struct{}),
	}
	m.lastBalancedTime = time.Now()
	go m.loopBalanceLoad()

	return m
}

func (m *Market) ShutdownMarket() {
	close(m.stopChan)
	close(m.AdjustmentChan)
}

func (m *Market) AddConsumerInstance(consumer *ConsumerGroupInstance) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.consumerInstances[consumer.InstanceId]; exists {
		return errors.New("consumer instance already exists")
	}

	m.consumerInstances[consumer.InstanceId] = consumer
	m.balanceRequestChan <- struct{}{}

	return nil
}

func (m *Market) RemoveConsumerInstance(consumerId ConsumerGroupInstanceId) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	consumer, exists := m.consumerInstances[consumerId]
	if !exists {
		return nil
	}
	delete(m.consumerInstances, consumerId)

	for _, partition := range consumer.AssignedPartitions {
		if partitionSlot, exists := m.partitions[partition]; exists {
			partitionSlot.AssignedTo = nil
		}
	}
	m.balanceRequestChan <- struct{}{}

	return nil
}

func (m *Market) assignPartitionToConsumer(partition *PartitionSlot) {
	var bestConsumer *ConsumerGroupInstance
	var minLoad = int(^uint(0) >> 1) // Max int value

	inflightConsumerAdjustments := make(map[ConsumerGroupInstanceId]int)
	for _, adjustment := range m.inflightAdjustments {
		if adjustment.isAssign {
			inflightConsumerAdjustments[adjustment.consumer]++
		} else {
			inflightConsumerAdjustments[adjustment.consumer]--
		}
	}
	for _, consumer := range m.consumerInstances {
		consumerLoad := len(consumer.AssignedPartitions)
		if inflightAdjustments, exists := inflightConsumerAdjustments[consumer.InstanceId]; exists {
			consumerLoad += inflightAdjustments
		}
		// fmt.Printf("Consumer %+v has load %d, max %d, min %d\n", consumer.InstanceId, consumerLoad, consumer.MaxPartitionCount, minLoad)
		if consumerLoad < int(consumer.MaxPartitionCount) {
			if consumerLoad < minLoad {
				bestConsumer = consumer
				minLoad = consumerLoad
				// fmt.Printf("picked: Consumer %+v has load %d, max %d, min %d\n", consumer.InstanceId, consumerLoad, consumer.MaxPartitionCount, minLoad)
			}
		}
	}

	if bestConsumer != nil {
		// change consumer assigned partitions later when the adjustment is confirmed
		adjustment := &Adjustment{
			isAssign:  true,
			partition: partition.Partition,
			consumer:  bestConsumer.InstanceId,
			ts:        time.Now(),
		}
		m.AdjustmentChan <- adjustment
		m.inflightAdjustments = append(m.inflightAdjustments, adjustment)
		m.lastBalancedTime = adjustment.ts
	}
}

func (m *Market) loopBalanceLoad() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if m.hasBalanceRequest {
				m.hasBalanceRequest = false
				inflightAdjustments := make([]*Adjustment, 0, len(m.inflightAdjustments))
				for _, adjustment := range m.inflightAdjustments {
					if adjustment.ts.Add(m.inflightAdjustmentTTL).After(time.Now()) {
						inflightAdjustments = append(inflightAdjustments, adjustment)
					}
				}
				m.inflightAdjustments = inflightAdjustments

				m.doBalanceLoad()
				// println("Balance load completed.")
				m.Status()
			}
		case <-m.balanceRequestChan:
			m.hasBalanceRequest = true
		case <-m.stopChan:
			return
		}
	}
}

// doBalanceLoad will balance the load of the partitions among the consumer group instances.
// It will try to unassign partitions from the consumer group instances that have more partitions than the average.
// It will try to assign partitions to the consumer group instances that have less partitions than the average.
func (m *Market) doBalanceLoad() {
	if len(m.consumerInstances) == 0 {
		return
	}

	// find the average load for all consumers
	averageLoad := m.findAverageLoad()

	// find the consumers with the higher load than average
	if m.adjustBusyConsumers(averageLoad) {
		return
	}

	// find partitions with no consumer assigned
	m.adjustUnassignedPartitions()
}
func (m *Market) findAverageLoad() (averageLoad float32) {
	var totalLoad int
	for _, consumer := range m.consumerInstances {
		totalLoad += len(consumer.AssignedPartitions)
	}
	for _, adjustment := range m.inflightAdjustments {
		if adjustment.isAssign {
			totalLoad++
		} else {
			totalLoad--
		}
	}
	averageLoad = float32(totalLoad) / float32(len(m.consumerInstances))
	return
}

func (m *Market) adjustBusyConsumers(averageLoad float32) (hasAdjustments bool) {
	inflightConsumerAdjustments := make(map[ConsumerGroupInstanceId]int)
	for _, adjustment := range m.inflightAdjustments {
		if adjustment.isAssign {
			inflightConsumerAdjustments[adjustment.consumer]++
		} else {
			inflightConsumerAdjustments[adjustment.consumer]--
		}
	}
	for _, consumer := range m.consumerInstances {
		consumerLoad := len(consumer.AssignedPartitions)
		if inflightAdjustment, exists := inflightConsumerAdjustments[consumer.InstanceId]; exists {
			consumerLoad += inflightAdjustment
		}
		delta := int(float32(consumerLoad) - averageLoad)
		if delta <= 0 {
			continue
		}
		adjustTime := time.Now()
		for i := 0; i < delta; i++ {
			adjustment := &Adjustment{
				isAssign:  false,
				partition: consumer.AssignedPartitions[i],
				consumer:  consumer.InstanceId,
				ts:        adjustTime,
			}
			m.AdjustmentChan <- adjustment
			m.inflightAdjustments = append(m.inflightAdjustments, adjustment)
			m.lastBalancedTime = adjustment.ts
		}
		hasAdjustments = true
	}
	return
}

func (m *Market) adjustUnassignedPartitions() {
	inflightPartitionAdjustments := make(map[topic.Partition]bool)
	for _, adjustment := range m.inflightAdjustments {
		inflightPartitionAdjustments[adjustment.partition] = true
	}
	for _, partitionSlot := range m.partitions {
		if partitionSlot.AssignedTo == nil {
			if _, exists := inflightPartitionAdjustments[partitionSlot.Partition]; exists {
				continue
			}
			// fmt.Printf("Assigning partition %+v to consumer\n", partitionSlot.Partition)
			m.assignPartitionToConsumer(partitionSlot)
		}
	}
}

func (m *Market) ConfirmAdjustment(adjustment *Adjustment) {
	if adjustment.isAssign {
		m.confirmAssignPartition(adjustment.partition, adjustment.consumer)
	} else {
		m.unassignPartitionSlot(adjustment.partition)
	}
	glog.V(1).Infof("ConfirmAdjustment %+v", adjustment)
	m.Status()
}

func (m *Market) unassignPartitionSlot(partition topic.Partition) {
	m.mu.Lock()
	defer m.mu.Unlock()

	partitionSlot, exists := m.partitions[partition]
	if !exists {
		glog.V(0).Infof("partition %+v slot is not tracked", partition)
		return
	}

	if partitionSlot.AssignedTo == nil {
		glog.V(0).Infof("partition %+v slot is not assigned to any consumer", partition)
		return
	}

	consumer := partitionSlot.AssignedTo
	for i, p := range consumer.AssignedPartitions {
		if p == partition {
			consumer.AssignedPartitions = append(consumer.AssignedPartitions[:i], consumer.AssignedPartitions[i+1:]...)
			partitionSlot.AssignedTo = nil
			m.balanceRequestChan <- struct{}{}
			return
		}
	}

	glog.V(0).Infof("partition %+v slot not found in assigned consumer", partition)

}

func (m *Market) confirmAssignPartition(partition topic.Partition, consumerInstanceId ConsumerGroupInstanceId) {
	m.mu.Lock()
	defer m.mu.Unlock()

	partitionSlot, exists := m.partitions[partition]
	if !exists {
		glog.V(0).Infof("partition %+v slot is not tracked", partition)
		return
	}

	if partitionSlot.AssignedTo != nil {
		glog.V(0).Infof("partition %+v slot is already assigned to %+v", partition, partitionSlot.AssignedTo.InstanceId)
		return
	}

	consumerInstance, exists := m.consumerInstances[consumerInstanceId]
	if !exists {
		glog.V(0).Infof("consumer %+v is not tracked", consumerInstanceId)
		return
	}

	partitionSlot.AssignedTo = consumerInstance
	consumerInstance.AssignedPartitions = append(consumerInstance.AssignedPartitions, partition)

}

func (m *Market) Status() {
	m.mu.Lock()
	defer m.mu.Unlock()

	glog.V(1).Infof("Market has %d partitions and %d consumer instances", len(m.partitions), len(m.consumerInstances))
	for partition, slot := range m.partitions {
		if slot.AssignedTo == nil {
			glog.V(1).Infof("Partition %+v is not assigned to any consumer", partition)
		} else {
			glog.V(1).Infof("Partition %+v is assigned to consumer %+v", partition, slot.AssignedTo.InstanceId)
		}
	}
	for _, consumer := range m.consumerInstances {
		glog.V(1).Infof("Consumer %+v has %d partitions", consumer.InstanceId, len(consumer.AssignedPartitions))
	}
}
