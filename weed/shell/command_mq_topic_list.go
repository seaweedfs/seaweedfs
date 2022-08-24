package shell

import (
	"fmt"
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

func (c *commandMqTopicList) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fmt.Fprintf(writer, "%s\n", commandEnv.option.Directory)

	return nil
}
