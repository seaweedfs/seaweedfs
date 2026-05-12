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
	error.

	When you set only some retention flags (for example, -retentionEnabled
	without -retention), the unspecified field is read from the current
	server-side configuration so it isn't accidentally zeroed. Omitting all
	retention flags leaves the existing retention configuration alone.
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

	// Detect which retention flags the user actually provided. Using Visit
	// (rather than value comparison) means an explicit `-retention=0` is
	// treated as "user provided" and still triggers the mutual-exclusion
	// check or partial-merge with current state.
	var userSetRetention, userSetSeconds, userSetEnabled bool
	mqCommand.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "retention":
			userSetRetention = true
		case "retentionSeconds":
			userSetSeconds = true
		case "retentionEnabled":
			userSetEnabled = true
		}
	})

	if userSetRetention && userSetSeconds {
		return fmt.Errorf("-retention and -retentionSeconds are mutually exclusive")
	}
	if *retention < 0 || *retentionSeconds < 0 {
		return fmt.Errorf("retention duration must be >= 0")
	}

	retentionTouched := userSetRetention || userSetSeconds || userSetEnabled

	// find the broker balancer
	brokerBalancer, err := findBrokerBalancer(commandEnv)
	if err != nil {
		return err
	}
	fmt.Fprintf(writer, "current balancer: %s\n", brokerBalancer)

	// Build the retention proto. When the user touches any retention flag we
	// must send a fully-populated TopicRetention so partial flags don't zero
	// the other field server-side: fetch the current configuration and use
	// its values for whatever the user didn't specify.
	var retentionProto *mq_pb.TopicRetention
	if retentionTouched {
		var currentRetention *mq_pb.TopicRetention
		if err := pb.WithBrokerGrpcClient(false, brokerBalancer, commandEnv.option.GrpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
			cur, getErr := client.GetTopicConfiguration(context.Background(), &mq_pb.GetTopicConfigurationRequest{
				Topic: &schema_pb.Topic{Namespace: *namespace, Name: *topicName},
			})
			if getErr != nil {
				// Topic may not exist yet — that's fine, we'll create it with
				// the user-supplied retention only.
				return nil
			}
			if cur != nil {
				currentRetention = cur.Retention
			}
			return nil
		}); err != nil {
			return err
		}

		var seconds int64
		var enabled bool
		if currentRetention != nil {
			seconds = currentRetention.RetentionSeconds
			enabled = currentRetention.Enabled
		}
		if userSetRetention {
			seconds = int64((*retention) / time.Second)
		} else if userSetSeconds {
			seconds = *retentionSeconds
		}
		if userSetEnabled {
			enabled = *retentionEnabled
		}
		retentionProto = &mq_pb.TopicRetention{
			RetentionSeconds: seconds,
			Enabled:          enabled,
		}
	}

	// create / update topic
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
