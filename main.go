package main

import (
	"fmt"
	"os"

	"it.smaso/tgfuse/filesystem"
)

func main() {
	args := os.Args
	if len(args) < 2 {
		fmt.Println("Missing file")
		os.Exit(1)
	}
	file := args[1]
	cf, err := filesystem.ReadChunkfile(file)
	if err != nil {
		fmt.Printf("Failed to read chunk file: %s", err.Error())
		os.Exit(1)
	}

	for _, ci := range cf.Chunks {
		if err := ci.Send(); err != nil {
			fmt.Println(err)
		}
	}
}
