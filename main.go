package main

import (
	"context"
	"fmt"
	"github.com/hanwen/go-fuse/v2/fs"
	"it.smaso/tgfuse/filesystem"
	"it.smaso/tgfuse/tgfuse"
	"log"
	"os"
	"syscall"
)

func main() {
	args := os.Args
	if len(args) < 2 {
		fmt.Println("Missing mounting point")
		os.Exit(1)
	}

	root := &tgfuse.RootNode{
		Nodes: map[string]*tgfuse.CfInode{},
	}

	go StartMemoryChecker()
	go StartGarbageCollector(root)

	server, err := fs.Mount(args[1], root, &fs.Options{})

	go func() {
		files, _ := filesystem.FetchFromEtcd()
		for idx := range *files {
			ctx := context.Background()
			file := tgfuse.CfInode{
				File: &(*files)[idx],
			}
			ch := root.NewPersistentInode(
				ctx,
				&file,
				fs.StableAttr{Mode: syscall.S_IFREG},
			)
			root.AddChild((*files)[idx].OriginalFilename, ch, true)
			root.Nodes[(*files)[idx].OriginalFilename] = &file
		}
		log.Println("Added all the entries to root")
	}()

	if err != nil {
		log.Fatalf("Mount failed: %v\n", err)
	}
	log.Println("Mounted successfully")
	server.Wait()
}
