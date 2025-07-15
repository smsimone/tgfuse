package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"it.smaso/tgfuse/configs"
	db "it.smaso/tgfuse/database"
	"it.smaso/tgfuse/logger"
	"it.smaso/tgfuse/tgfuse"
)

func main() {
	args := os.Args
	if len(args) < 2 {
		logger.LogErr("Missing mounting point")
		os.Exit(1)
	}

	root := tgfuse.NewRoot()

	database := db.Connect(configs.DB_CONFIG)
	logger.LogInfo("Connected to database")

	// go StartMemoryChecker()
	// go StartGarbageCollector(root)

	server, err := fs.Mount(args[1], root, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: "tgfuse",
		},
		UID: uint32(os.Getuid()),
		GID: uint32(os.Getgid()),
	})

	defer func() {
		if err := recover(); err != nil {
			_ = server.Unmount()
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		switch sig := <-signals; sig {
		case syscall.SIGINT, syscall.SIGTERM:
			_ = server.Unmount()
			logger.LogInfo("Unmounted tgfuse folder")
			os.Exit(0)
		}
	}()

	go func() {
		files, _ := database.GetAllChunkFiles()
		for idx := range *files {
			ctx := context.Background()
			file := tgfuse.CfInode{File: &(*files)[idx]}

			ch := root.NewInode(
				ctx,
				&file,
				fs.StableAttr{Mode: syscall.S_IFREG | 0o755},
			)
			filename := (*files)[idx].OriginalFilename
			root.AddChild(filename, ch, true)

			// root.Nodes[(*files)[idx].OriginalFilename] = &file
			root.Nodes[filename] = &file
			// root.CfChildren[filename] = &file
		}
		logger.LogInfo("Added all the entries to root")
	}()

	if err != nil {
		panic(fmt.Sprintf("Mount failed: %v", err))
	}
	logger.LogInfo("Mounted successfully")
	server.Wait()
}
