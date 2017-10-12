package main

import (
	"os"

	"github.com/takashabe/go-pubsub/server"
)

func main() {
	c := &server.CLI{
		OutStream: os.Stdout,
		ErrStream: os.Stderr,
	}
	os.Exit(c.Run(os.Args))
}
