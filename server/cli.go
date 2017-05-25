package server

import (
	"flag"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

// default parameters
const (
	defaultPort = 8080
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
	outStream io.Writer
	errStream io.Writer
}

// Run invokes the CLI with the given arguments
func (c *CLI) Run(args []string) int {
	param := &param{}
	err := c.parseArgs(args[1:], param)
	if err != nil {
		fmt.Fprintf(c.errStream, "args parse error: %v", err)
		return ExitCodeParseError
	}

	server, err := NewServer(param.file)
	if err != nil {
		fmt.Fprintf(c.errStream, "invalid args. failed to initialize server: %v", err)
		return ExitCodeInvalidArgsError
	}

	if err := server.Run(param.port); err != nil {
		fmt.Fprintf(c.errStream, "failed from server: %v", err)
		return ExitCodeError
	}
	return ExitCodeOK
}

func (c *CLI) parseArgs(args []string, p *param) error {
	flags := flag.NewFlagSet("param", flag.ContinueOnError)
	flags.SetOutput(c.errStream)

	// TODO: add datastore driver config
	flags.IntVar(&p.port, "port", defaultPort, "")
	flags.StringVar(&p.file, "file", defaultFile, "")

	err := flags.Parse(args)
	if err != nil {
		return errors.Wrapf(ErrParseFailed, err.Error())
	}
	return nil
}
