package broker

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
)

func (broker *MessageBroker) Subscribe(stream messaging_pb.SeaweedMessaging_SubscribeServer) error {

	// process initial request
	in, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}

	var messageCount int64
	subscriberId := in.Init.SubscriberId
	fmt.Printf("+ subscriber %s\n", subscriberId)
	defer func() {
		fmt.Printf("- subscriber %s: %d messages\n", subscriberId, messageCount)
	}()

	// TODO look it up
	topicConfig := &messaging_pb.TopicConfiguration{
		// IsTransient: true,
	}

	if err = stream.Send(&messaging_pb.BrokerMessage{
		Redirect: nil,
	}); err != nil {
		return err
	}

	// get lock
	tp := TopicPartition{
		Namespace: in.Init.Namespace,
		Topic:     in.Init.Topic,
		Partition: in.Init.Partition,
	}
	lock := broker.topicLocks.RequestLock(tp, topicConfig, false)
	defer broker.topicLocks.ReleaseLock(tp, false)

	lastReadTime := time.Now()
	switch in.Init.StartPosition {
	case messaging_pb.SubscriberMessage_InitMessage_TIMESTAMP:
		lastReadTime = time.Unix(0, in.Init.TimestampNs)
	case messaging_pb.SubscriberMessage_InitMessage_LATEST:
	case messaging_pb.SubscriberMessage_InitMessage_EARLIEST:
	}
	var processedTsNs int64

	// how to process each message
	// an error returned will end the subscription
	eachMessageFn := func(m *messaging_pb.Message) error {
		err := stream.Send(&messaging_pb.BrokerMessage{
			Data: m,
		})
		if err != nil {
			glog.V(0).Infof("=> subscriber %v: %+v", subscriberId, err)
		}
		return err
	}

	eachLogEntryFn := func(logEntry *filer_pb.LogEntry) error {
		m := &messaging_pb.Message{}
		if err = proto.Unmarshal(logEntry.Data, m); err != nil {
			glog.Errorf("unexpected unmarshal messaging_pb.Message: %v", err)
			return err
		}
		// fmt.Printf("sending : %d bytes ts %d\n", len(m.Value), logEntry.TsNs)
		if err = eachMessageFn(m); err != nil {
			glog.Errorf("sending %d bytes to %s: %s", len(m.Value), subscriberId, err)
			return err
		}
		processedTsNs = logEntry.TsNs
		return nil
	}

	if err := broker.readPersistedLogBuffer(&tp, lastReadTime, eachLogEntryFn); err != nil {
		return err
	}

	if processedTsNs != 0 {
		lastReadTime = time.Unix(0, processedTsNs)
	}

	messageCount, err = lock.logBuffer.LoopProcessLogData(lastReadTime, func() bool {
		lock.Mutex.Lock()
		lock.cond.Wait()
		lock.Mutex.Unlock()
		return true
	}, eachLogEntryFn)

	return err

}

func (broker *MessageBroker) readPersistedLogBuffer(tp *TopicPartition, startTime time.Time, eachLogEntryFn func(logEntry *filer_pb.LogEntry) error) (err error) {
	startDate := fmt.Sprintf("%04d-%02d-%02d", startTime.Year(), startTime.Month(), startTime.Day())
	startHourMinute := fmt.Sprintf("%02d-%02d.segment", startTime.Hour(), startTime.Minute())

	sizeBuf := make([]byte, 4)
	startTsNs := startTime.UnixNano()

	topicDir := fmt.Sprintf("/topics/%s/%s", tp.Namespace, tp.Topic)
	partitionSuffix := fmt.Sprintf(".part%02d", tp.Partition)

	return filer_pb.List(broker, topicDir, "", func(dayEntry *filer_pb.Entry, isLast bool) error {
		dayDir := fmt.Sprintf("%s/%s", topicDir, dayEntry.Name)
		return filer_pb.List(broker, dayDir, "", func(hourMinuteEntry *filer_pb.Entry, isLast bool) error {
			if dayEntry.Name == startDate {
				if strings.Compare(hourMinuteEntry.Name, startHourMinute) < 0 {
					return nil
				}
			}
			if !strings.HasSuffix(hourMinuteEntry.Name, partitionSuffix){
				return nil
			}
			// println("partition", tp.Partition, "processing", dayDir, "/", hourMinuteEntry.Name)
			chunkedFileReader := filer2.NewChunkStreamReader(broker, hourMinuteEntry.Chunks)
			defer chunkedFileReader.Close()
			if err := filer2.ReadEachLogEntry(chunkedFileReader, sizeBuf, startTsNs, eachLogEntryFn); err != nil {
				chunkedFileReader.Close()
				if err == io.EOF {
					return nil
				}
				return fmt.Errorf("reading %s/%s: %v", dayDir, hourMinuteEntry.Name, err)
			}
			return nil
		}, "", false, 24*60)
	}, startDate, true, 366)

}
