package main

import (
	"bufio"
	"fmt"
	"os"
)

func init() {
	cmdShell.Run = runShell // break init cycle
}

var cmdShell = &Command{
	UsageLine: "shell",
	Short:     "run interactive commands, now just echo",
	Long: `run interactive commands.

  `,
}

var ()

func runShell(command *Command, args []string) bool {
	r := bufio.NewReader(os.Stdin)
	o := bufio.NewWriter(os.Stdout)
	e := bufio.NewWriter(os.Stderr)
	prompt := func() {
		o.WriteString("> ")
		o.Flush()
	}
	readLine := func() string {
		ret, err := r.ReadString('\n')
		if err != nil {
			fmt.Fprint(e, err)
			os.Exit(1)
		}
		return ret
	}
	execCmd := func(cmd string) int {
		if cmd != "" {
			o.WriteString(cmd)
		}
		return 0
	}

	cmd := ""
	for {
		prompt()
		cmd = readLine()
		execCmd(cmd)
	}
	return true
}
