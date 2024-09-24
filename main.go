package main

import (
	"log"

	"github.com/ralucas/centipede/cmd"
)

// Main executes the root command found in the
// cmd directory.
func main() {
	if err := cmd.Initialize().Execute(); err != nil {
		log.Fatal(err)
	}
}
