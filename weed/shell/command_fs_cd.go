package shell

import (
	"io"
)

func init() {
	Commands = append(Commands, &commandFsCd{})
}

type commandFsCd struct {
}

func (c *commandFsCd) Name() string {
	return "fs.cd"
}

func (c *commandFsCd) Help() string {
	return `change directory to a directory /path/to/dir

	The full path can be too long to type. For example,
		fs.ls /some/path/to/file_name

	can be simplified as

		fs.cd /some/path
		fs.ls to/file_name
`
}

func (c *commandFsCd) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsCd) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	path, err := commandEnv.parseUrl(findInputDirectory(args))
	if err != nil {
		return err
	}

	if path == "/" {
		commandEnv.option.Directory = "/"
		return nil
	}

	err = commandEnv.checkDirectory(path)

	if err == nil {
		commandEnv.option.Directory = path
	}

	return err
}
