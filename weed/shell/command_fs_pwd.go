package shell

import (
	"fmt"
	"io"
)

func init() {
	commands = append(commands, &commandFsPwd{})
}

type commandFsPwd struct {
}

func (c *commandFsPwd) Name() string {
	return "fs.pwd"
}

func (c *commandFsPwd) Help() string {
	return `print out current directory`
}

func (c *commandFsPwd) Do(args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	fmt.Fprintf(writer, "http://%s:%d%s\n",
		commandEnv.option.FilerHost,
		commandEnv.option.FilerPort,
		commandEnv.option.Directory,
	)

	return nil
}
