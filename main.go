package main

import (
	"context"
	"fmt"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"it.smaso/tgfuse/filesystem"
	"it.smaso/tgfuse/tgfuse"
	"log"
	"os"
	"os/signal"
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

	server, err := fs.Mount(args[1], root, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: "tgfuse",
		},
		UID: uint32(os.Getuid()),
		GID: uint32(os.Getgid()),
	})

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		switch sig := <-signals; sig {
		case syscall.SIGINT, syscall.SIGTERM:
			_ = server.Unmount()
			log.Println("Unmounted tgfuse folder")
			os.Exit(0)
		}
	}()

	go func() {
		files, _ := filesystem.FetchFromEtcd()
		for idx := range *files {
			ctx := context.Background()
			file := tgfuse.CfInode{File: &(*files)[idx]}

			ch := root.NewInode(
				ctx,
				&file,
				fs.StableAttr{Mode: syscall.S_IFREG | 0755},
			)
			filename := (*files)[idx].OriginalFilename
			root.AddChild(filename, ch, true)

			// root.Nodes[(*files)[idx].OriginalFilename] = &file
			root.Nodes[filename] = &file
			// root.CfChildren[filename] = &file
		}
		log.Println("Added all the entries to root")
	}()

	if err != nil {
		log.Fatalf("Mount failed: %v\n", err)
	}
	log.Println("Mounted successfully")
	server.Wait()
}
