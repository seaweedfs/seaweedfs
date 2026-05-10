package shell

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func init() {
	Commands = append(Commands, &commandMqTopicConfigure{})
}

type commandMqTopicConfigure struct {
}

func (c *commandMqTopicConfigure) Name() string {
	return "mq.topic.configure"
}

func (c *commandMqTopicConfigure) Help() string {
	return `configure a topic with a given name

	Example:
		mq.topic.configure -namespace <namespace> -topic <topic_name> -partitionCount <partition_count>

	Retention (delete messages older than the configured duration):
		mq.topic.configure -namespace <namespace> -topic <topic_name> \
			-retention 168h -retentionEnabled

		# disable retention on an existing topic
		mq.topic.configure -namespace <namespace> -topic <topic_name> \
			-retentionEnabled=false

	-retention accepts any Go duration string ("24h", "168h", "30m"). Use
	-retentionSeconds for raw seconds when scripting. Specifying both is an
	error. Setting -retention or -retentionSeconds without -retentionEnabled
	stages the value but leaves enforcement off; pass -retentionEnabled=true
	(or just -retentionEnabled) to actually enable expiry.
`
}

func (c *commandMqTopicConfigure) HasTag(CommandTag) bool {
	return false
}

func (c *commandMqTopicConfigure) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {

	// parse parameters
	mqCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	namespace := mqCommand.String("namespace", "", "namespace name")
	topicName := mqCommand.String("topic", "", "topic name")
	partitionCount := mqCommand.Int("partitionCount", 6, "partition count")
	retention := mqCommand.Duration("retention", 0, "retention duration (Go duration string, e.g. 168h). Mutually exclusive with -retentionSeconds.")
	retentionSeconds := mqCommand.Int64("retentionSeconds", 0, "retention duration in seconds. Mutually exclusive with -retention.")
	retentionEnabled := mqCommand.Bool("retentionEnabled", false, "enable retention enforcement on the topic")
	if err := mqCommand.Parse(args); err != nil {
		return err
	}

	if *retention != 0 && *retentionSeconds != 0 {
		return fmt.Errorf("-retention and -retentionSeconds are mutually exclusive")
	}
	if *retention < 0 || *retentionSeconds < 0 {
		return fmt.Errorf("retention duration must be >= 0")
	}

	// find the broker balancer
	brokerBalancer, err := findBrokerBalancer(commandEnv)
	if err != nil {
		return err
	}
	fmt.Fprintf(writer, "current balancer: %s\n", brokerBalancer)

	// build retention proto only when the user touched a retention flag, so
	// existing callers that don't care about retention keep the prior
	// "leave server-side state alone" behavior.
	var retentionProto *mq_pb.TopicRetention
	retentionTouched := false
	mqCommand.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "retention", "retentionSeconds", "retentionEnabled":
			retentionTouched = true
		}
	})
	if retentionTouched {
		seconds := *retentionSeconds
		if *retention != 0 {
			seconds = int64((*retention) / time.Second)
		}
		retentionProto = &mq_pb.TopicRetention{
			RetentionSeconds: seconds,
			Enabled:          *retentionEnabled,
		}
	}

	// create topic
	return pb.WithBrokerGrpcClient(false, brokerBalancer, commandEnv.option.GrpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
		resp, err := client.ConfigureTopic(context.Background(), &mq_pb.ConfigureTopicRequest{
			Topic: &schema_pb.Topic{
				Namespace: *namespace,
				Name:      *topicName,
			},
			PartitionCount: int32(*partitionCount),
			Retention:      retentionProto,
		})
		if err != nil {
			return err
		}
		output, _ := json.MarshalIndent(resp, "", "  ")
		fmt.Fprintf(writer, "response:\n%+v\n", string(output))
		return nil
	})

}
