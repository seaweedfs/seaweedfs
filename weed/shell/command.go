package shell

import "io"

type command interface {
	Name() string
	Help() string
	Do([]string, *CommandEnv, io.Writer) error
	HasTag(tag CommandTag) bool
}

var (
	Commands = []command{}
)

type CommandTag string

const (
	ResourceHeavy CommandTag = "resourceHeavy"
)
