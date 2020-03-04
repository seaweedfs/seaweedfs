package weed_server

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type MessageBrokerOption struct {
	Filers             []string
	DefaultReplication string
	MaxMB              int
	Port               int
}

type MessageBroker struct {
	option         *MessageBrokerOption
	grpcDialOption grpc.DialOption
}

func NewMessageBroker(option *MessageBrokerOption) (messageBroker *MessageBroker, err error) {

	messageBroker = &MessageBroker{
		option:         option,
		grpcDialOption: security.LoadClientTLS(util.GetViper(), "grpc.msg_broker"),
	}

	go messageBroker.loopForEver()

	return messageBroker, nil
}

func (broker *MessageBroker) loopForEver() {

	for {
		broker.checkPeers()
		time.Sleep(3 * time.Second)
	}

}

func (broker *MessageBroker) checkPeers() {

	// contact a filer about masters
	var masters []string
	for _, filer := range broker.option.Filers {
		err := broker.withFilerClient(filer, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err != nil {
				return err
			}
			masters = append(masters, resp.Masters...)
			return nil
		})
		if err != nil {
			fmt.Printf("failed to read masters from %+v: %v\n", broker.option.Filers, err)
			return
		}
	}

	// contact each masters for filers
	var filers []string
	for _, master := range masters {
		err := broker.withMasterClient(master, func(client master_pb.SeaweedClient) error {
			resp, err := client.ListMasterClients(context.Background(), &master_pb.ListMasterClientsRequest{
				ClientType: "filer",
			})
			if err != nil {
				return err
			}

			fmt.Printf("filers: %+v\n", resp.GrpcAddresses)
			filers = append(filers, resp.GrpcAddresses...)

			return nil
		})
		if err != nil {
			fmt.Printf("failed to list filers: %v\n", err)
			return
		}
	}

	// contact each filer about brokers
	for _, filer := range filers {
		err := broker.withFilerClient(filer, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err != nil {
				return err
			}
			masters = append(masters, resp.Masters...)
			return nil
		})
		if err != nil {
			fmt.Printf("failed to read masters from %+v: %v\n", broker.option.Filers, err)
			return
		}
	}

}

func (broker *MessageBroker) withFilerClient(filer string, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithFilerClient(filer, broker.grpcDialOption, fn)

}

func (broker *MessageBroker) withMasterClient(master string, fn func(client master_pb.SeaweedClient) error) error {

	return pb.WithMasterClient(master, broker.grpcDialOption, func(client master_pb.SeaweedClient) error {
		return fn(client)
	})

}
