package coordinator

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"sync"
)

func (cg *ConsumerGroup) SetMinMaxActiveInstances(min, max int32) {
	cg.MinimumActiveInstances = min
	cg.MaximumActiveInstances = max
}

func (cg *ConsumerGroup) AddConsumerGroupInstance(clientId string) *ConsumerGroupInstance {
	cgi := &ConsumerGroupInstance{
		ClientId: clientId,
	}
	cg.ConsumerGroupInstances.Set(clientId, cgi)
	return cgi
}

func (cg *ConsumerGroup) RemoveConsumerGroupInstance(clientId string) {
	cg.ConsumerGroupInstances.Remove(clientId)
}

func (cg *ConsumerGroup) CoordinateIfNeeded() {
	emptyInstanceCount, activeInstanceCount := int32(0), int32(0)
	for cgi := range cg.ConsumerGroupInstances.IterBuffered() {
		if cgi.Val.Partition == nil {
			// this consumer group instance is not assigned a partition
			// need to assign one
			emptyInstanceCount++
		} else {
			activeInstanceCount++
		}
	}

	var delta int32
	if emptyInstanceCount > 0 {
		if cg.MinimumActiveInstances <= 0 {
			// need to assign more partitions
			delta = emptyInstanceCount
		} else if activeInstanceCount < cg.MinimumActiveInstances && activeInstanceCount+emptyInstanceCount >= cg.MinimumActiveInstances {
			// need to assign more partitions
			delta = cg.MinimumActiveInstances - activeInstanceCount
		}
	}

	if cg.MaximumActiveInstances > 0 {
		if activeInstanceCount > cg.MaximumActiveInstances {
			// need to remove some partitions
			delta = cg.MaximumActiveInstances - activeInstanceCount
		}
	}
	if delta == 0 {
		return
	}
	cg.doCoordinate(activeInstanceCount + delta)
}

func (cg *ConsumerGroup) doCoordinate(target int32) {
	// stop existing instances from processing
	var wg sync.WaitGroup
	for cgi := range cg.ConsumerGroupInstances.IterBuffered() {
		if cgi.Val.Partition != nil {
			wg.Add(1)
			go func(cgi *ConsumerGroupInstance) {
				defer wg.Done()
				// stop processing
				// flush internal state
				// wait for all messages to be processed
				// close the connection
			}(cgi.Val)
		}
	}
	wg.Wait()

	partitions := topic.SplitPartitions(target)

	// assign partitions to new instances
	i := 0
	for cgi := range cg.ConsumerGroupInstances.IterBuffered() {
		cgi.Val.Partition = partitions[i]
		i++
		wg.Add(1)
		go func(cgi *ConsumerGroupInstance) {
			defer wg.Done()
			// start processing
			// start consuming from the last offset
		}(cgi.Val)
	}
	wg.Wait()
}
