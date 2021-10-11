package broker

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/util/log_buffer"
	"io"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/chrislusf/seaweedfs/weed/filer"
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

	var processedTsNs int64
	var messageCount int64
	subscriberId := in.Init.SubscriberId

	// TODO look it up
	topicConfig := &messaging_pb.TopicConfiguration{
		// IsTransient: true,
	}

	// get lock
	tp := TopicPartition{
		Namespace: in.Init.Namespace,
		Topic:     in.Init.Topic,
		Partition: in.Init.Partition,
	}
	fmt.Printf("+ subscriber %s for %s\n", subscriberId, tp.String())
	defer func() {
		fmt.Printf("- subscriber %s for %s %d messages last %v\n", subscriberId, tp.String(), messageCount, time.Unix(0, processedTsNs))
	}()

	lock := broker.topicManager.RequestLock(tp, topicConfig, false)
	defer broker.topicManager.ReleaseLock(tp, false)

	isConnected := true
	go func() {
		for isConnected {
			if _, err := stream.Recv(); err != nil {
				// println("disconnecting connection to", subscriberId, tp.String())
				isConnected = false
				lock.cond.Signal()
			}
		}
	}()

	lastReadTime := time.Now()
	switch in.Init.StartPosition {
	case messaging_pb.SubscriberMessage_InitMessage_TIMESTAMP:
		lastReadTime = time.Unix(0, in.Init.TimestampNs)
	case messaging_pb.SubscriberMessage_InitMessage_LATEST:
	case messaging_pb.SubscriberMessage_InitMessage_EARLIEST:
		lastReadTime = time.Unix(0, 0)
	}

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
		if m.IsClose {
			// println("processed EOF")
			return io.EOF
		}
		processedTsNs = logEntry.TsNs
		messageCount++
		return nil
	}

	// fmt.Printf("subscriber %s read %d on disk log %v\n", subscriberId, messageCount, lastReadTime)

	for {

		if err = broker.readPersistedLogBuffer(&tp, lastReadTime, eachLogEntryFn); err != nil {
			if err != io.EOF {
				// println("stopping from persisted logs", err.Error())
				return err
			}
		}

		if processedTsNs != 0 {
			lastReadTime = time.Unix(0, processedTsNs)
		}

		lastReadTime, err = lock.logBuffer.LoopProcessLogData("broker", lastReadTime, func() bool {
			lock.Mutex.Lock()
			lock.cond.Wait()
			lock.Mutex.Unlock()
			return isConnected
		}, eachLogEntryFn)
		if err != nil {
			if err == log_buffer.ResumeFromDiskError {
				continue
			}
			glog.Errorf("processed to %v: %v", lastReadTime, err)
			time.Sleep(3127 * time.Millisecond)
			if err != log_buffer.ResumeError {
				break
			}
		}
	}

	return err

}

func (broker *MessageBroker) readPersistedLogBuffer(tp *TopicPartition, startTime time.Time, eachLogEntryFn func(logEntry *filer_pb.LogEntry) error) (err error) {
	startTime = startTime.UTC()
	startDate := fmt.Sprintf("%04d-%02d-%02d", startTime.Year(), startTime.Month(), startTime.Day())
	startHourMinute := fmt.Sprintf("%02d-%02d", startTime.Hour(), startTime.Minute())

	sizeBuf := make([]byte, 4)
	startTsNs := startTime.UnixNano()

	topicDir := genTopicDir(tp.Namespace, tp.Topic)
	partitionSuffix := fmt.Sprintf(".part%02d", tp.Partition)

	return filer_pb.List(broker, topicDir, "", func(dayEntry *filer_pb.Entry, isLast bool) error {
		dayDir := fmt.Sprintf("%s/%s", topicDir, dayEntry.Name)
		return filer_pb.List(broker, dayDir, "", func(hourMinuteEntry *filer_pb.Entry, isLast bool) error {
			if dayEntry.Name == startDate {
				hourMinute := util.FileNameBase(hourMinuteEntry.Name)
				if strings.Compare(hourMinute, startHourMinute) < 0 {
					return nil
				}
			}
			if !strings.HasSuffix(hourMinuteEntry.Name, partitionSuffix) {
				return nil
			}
			// println("partition", tp.Partition, "processing", dayDir, "/", hourMinuteEntry.Name)
			chunkedFileReader := filer.NewChunkStreamReader(broker, hourMinuteEntry.Chunks)
			defer chunkedFileReader.Close()
			if _, err := filer.ReadEachLogEntry(chunkedFileReader, sizeBuf, startTsNs, eachLogEntryFn); err != nil {
				chunkedFileReader.Close()
				if err == io.EOF {
					return err
				}
				return fmt.Errorf("reading %s/%s: %v", dayDir, hourMinuteEntry.Name, err)
			}
			return nil
		}, "", false, 24*60)
	}, startDate, true, 366)

}
