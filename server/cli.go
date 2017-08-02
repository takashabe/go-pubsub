package server

import (
	"flag"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

// default parameters
const (
	defaultFile = "config/app.yaml"
)

// Exit codes. used only in Run()
const (
	ExitCodeOK = 0

	// Specific error codes. begin 10-
	ExitCodeError = 10 + iota
	ExitCodeParseError
	ExitCodeInvalidArgsError
)

var (
	// ErrParseFailed is failed to cli args parse
	ErrParseFailed = errors.New("failed to parse args")
)

type param struct {
	port int
	file string
}

// CLI is the command line interface object
type CLI struct {
	OutStream io.Writer
	ErrStream io.Writer
}

// Run invokes the CLI with the given arguments
func (c *CLI) Run(args []string) int {
	param := &param{}
	err := c.parseArgs(args[1:], param)
	if err != nil {
		fmt.Fprintf(c.ErrStream, "args parse error: %v", err)
		return ExitCodeParseError
	}

	server, err := NewServer(param.file)
	if err != nil {
		fmt.Fprintf(c.ErrStream, "invalid args. failed to initialize server: %v", err)
		return ExitCodeInvalidArgsError
	}

	if err := server.Run(param.port); err != nil {
		fmt.Fprintf(c.ErrStream, "failed from server: %v", err)
		return ExitCodeError
	}
	return ExitCodeOK
}

func (c *CLI) parseArgs(args []string, p *param) error {
	flags := flag.NewFlagSet("param", flag.ContinueOnError)
	flags.SetOutput(c.ErrStream)

	flags.StringVar(&p.file, "file", defaultFile, "Config file. require anything config file.")

	err := flags.Parse(args)
	if err != nil {
		return errors.Wrapf(ErrParseFailed, err.Error())
	}
	return nil
}
