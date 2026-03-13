package main

import "github.com/htb/htb-replicator/cli"

// Build information injected by the linker via -ldflags.
var (
	Version   = "dev"
	Commit    = "none"
	BuildDate = "unknown"
)

func main() {
	cli.Execute()
}
