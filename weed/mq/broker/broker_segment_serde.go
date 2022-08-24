package broker

import (
	"bytes"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/mq"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
	"time"
)

func (broker *MessageQueueBroker) checkSegmentOnFiler(segment *mq.Segment) (brokers []pb.ServerAddress, err error) {
	info, found, err := broker.readSegmentOnFiler(segment)
	if err != nil {
		return
	}
	if !found {
		return
	}
	for _, b := range info.Brokers {
		brokers = append(brokers, pb.ServerAddress(b))
	}

	return
}

func (broker *MessageQueueBroker) saveSegmentBrokersOnFiler(segment *mq.Segment, brokers []pb.ServerAddress) (err error) {
	var nodes []string
	for _, b := range brokers {
		nodes = append(nodes, string(b))
	}
	broker.saveSegmentToFiler(segment, &mq_pb.SegmentInfo{
		Segment:          segment.ToPbSegment(),
		StartTsNs:        time.Now().UnixNano(),
		Brokers:          nodes,
		StopTsNs:         0,
		PreviousSegments: nil,
		NextSegments:     nil,
	})
	return
}

func (broker *MessageQueueBroker) readSegmentOnFiler(segment *mq.Segment) (info *mq_pb.SegmentInfo, found bool, err error) {
	dir, name := segment.DirAndName()

	found, err = filer_pb.Exists(broker, dir, name, false)
	if !found || err != nil {
		return
	}

	err = pb.WithFilerClient(false, broker.GetFiler(), broker.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		// read filer conf first
		data, err := filer.ReadInsideFiler(client, dir, name)
		if err != nil {
			return fmt.Errorf("ReadEntry: %v", err)
		}

		// parse into filer conf object
		info = &mq_pb.SegmentInfo{}
		if err = jsonpb.Unmarshal(data, info); err != nil {
			return err
		}
		found = true
		return nil
	})

	return
}

func (broker *MessageQueueBroker) saveSegmentToFiler(segment *mq.Segment, info *mq_pb.SegmentInfo) (err error) {
	dir, name := segment.DirAndName()

	var buf bytes.Buffer
	filer.ProtoToText(&buf, info)

	err = pb.WithFilerClient(false, broker.GetFiler(), broker.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		// read filer conf first
		err := filer.SaveInsideFiler(client, dir, name, buf.Bytes())
		if err != nil {
			return fmt.Errorf("save segment info: %v", err)
		}
		return nil
	})

	return
}
