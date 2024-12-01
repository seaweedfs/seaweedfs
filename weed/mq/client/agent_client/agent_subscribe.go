package agent_client

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func (a *SubscribeSession) SubscribeMessageRecord(
	onEachMessageFn func(key []byte, record *schema_pb.RecordValue),
	onCompletionFn func()) error {
	for {
		resp, err := a.stream.Recv()
		if err != nil {
			return err
		}
		onEachMessageFn(resp.Key, resp.Value)
	}
}
