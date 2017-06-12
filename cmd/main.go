package main

import (
	"os"

	"github.com/takashabe/go-message-queue/server"
)

func main() {
	c := &server.CLI{
		OutStream: os.Stdout,
		ErrStream: os.Stderr,
	}
	os.Exit(c.Run(os.Args))
}
