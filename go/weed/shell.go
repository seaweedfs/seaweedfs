package main

import (
	"bufio"
	"fmt"
	"log"
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
		var err error
		if _, err = o.WriteString("> "); err != nil {
			log.Printf("error writing to stdout: %s", err)
		}
		if err = o.Flush(); err != nil {
			log.Printf("error flushing stdout: %s", err)
		}
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
			if _, err := o.WriteString(cmd); err != nil {
				log.Printf("error writing to stdout: %s", err)
			}
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
