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

func (c *commandFsCd) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	input := findInputDirectory(args)

	filerServer, filerPort, path, err := commandEnv.parseUrl(input)
	if err != nil {
		return err
	}

	if path == "/" {
		commandEnv.option.FilerHost = filerServer
		commandEnv.option.FilerPort = filerPort
		commandEnv.option.Directory = "/"
		return nil
	}

	err = commandEnv.checkDirectory(filerServer, filerPort, path)

	if err == nil {
		commandEnv.option.FilerHost = filerServer
		commandEnv.option.FilerPort = filerPort
		commandEnv.option.Directory = path
	}

	return err
}
