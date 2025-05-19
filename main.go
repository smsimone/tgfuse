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

	if cf.NumChunks == 0 {
		fmt.Println("Failed to split file in chunks")
		os.Exit(1)
	}

	fmt.Println("Prepared chunk file", cf.NumChunks, cf.Id)
	for _, ci := range cf.Chunks {
		if err := ci.Send(); err != nil {
			fmt.Println(err)
		}
	}
	fmt.Println("Sent files to telegram")

	if err := cf.UploadToDatabase(); err != nil {
		fmt.Println("Failed to upload file", err)
	}
	fmt.Println("Uploaded keys to etcd server")
}
