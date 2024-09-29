package shell

import "io"

type command interface {
	Name() string
	Help() string
	Do([]string, *CommandEnv, io.Writer) error
	IsResourceHeavy() bool
}

var (
	Commands = []command{}
)
