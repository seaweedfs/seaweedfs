package shell

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"io"
)

func init() {
	Commands = append(Commands, &commandMqBalanceTopics{})
}

type commandMqBalanceTopics struct {
}

func (c *commandMqBalanceTopics) Name() string {
	return "mq.balance"
}

func (c *commandMqBalanceTopics) Help() string {
	return `balance topic partitions

`
}

func (c *commandMqBalanceTopics) HasTag(CommandTag) bool {
	return false
}

func (c *commandMqBalanceTopics) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {

	// find the broker balancer
	brokerBalancer, err := findBrokerBalancer(commandEnv)
	if err != nil {
		return err
	}
	fmt.Fprintf(writer, "current balancer: %s\n", brokerBalancer)

	// balance topics
	return pb.WithBrokerGrpcClient(false, brokerBalancer, commandEnv.option.GrpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
		_, err := client.BalanceTopics(context.Background(), &mq_pb.BalanceTopicsRequest{})
		if err != nil {
			return err
		}
		return nil
	})

}
