package shell

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/balancer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"io"
)

func init() {
	Commands = append(Commands, &commandMqTopicList{})
}

type commandMqTopicList struct {
}

func (c *commandMqTopicList) Name() string {
	return "mq.topic.list"
}

func (c *commandMqTopicList) Help() string {
	return `print out all topics`
}

func (c *commandMqTopicList) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {

	brokerBalancer, err := findBrokerBalancer(commandEnv)
	if err != nil {
		return err
	}

	//pb.WithBrokerGrpcClient(false, brokerBalancer, commandEnv.option.GrpcDialOption, func(client pb.SeaweedMessagingClient) error {
	//	resp, err := client.ListTopics(context.Background(), &pb.ListTopicsRequest{})
	//	if err != nil {
	//		return err
	//	}
	//	for _, topic := range resp.Topics {
	//		fmt.Fprintf(writer, "%s\n", topic)
	//	}
	//	return nil
	//})

	fmt.Fprintf(writer, "current balancer: %s\n", brokerBalancer)

	return nil
}

func findBrokerBalancer(commandEnv *CommandEnv) (brokerBalancer string, err error) {
	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.FindLockOwner(context.Background(), &filer_pb.FindLockOwnerRequest{
			Name: balancer.LockBrokerBalancer,
		})
		if err != nil {
			return err
		}
		brokerBalancer = resp.Owner
		return nil
	})
	return
}
