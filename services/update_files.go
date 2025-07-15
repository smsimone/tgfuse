package services

import (
	"context"
	"fmt"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"it.smaso/tgfuse/configs"
	db "it.smaso/tgfuse/database"
	"it.smaso/tgfuse/filesystem"
	"it.smaso/tgfuse/logger"
	"it.smaso/tgfuse/tgfuse"
)

func UpdateFiles(rn *tgfuse.RootNode) {
	for {
		files, _ := db.Connect(configs.DB_CONFIG).GetAllChunkFiles()
		currNames := rn.GetCurrentNames()
		toDelete := map[string]bool{}
		for _, name := range currNames {
			toDelete[name] = true
		}

		for idx := range *files {
			cf := (*files)[idx]
			if _, found := rn.Nodes[cf.OriginalFilename]; !found {
				addMissingFile(&cf, rn)
			} else {
				toDelete[cf.OriginalFilename] = false
			}
		}

		for name := range toDelete {
			if toDelete[name] {
				deleteFile(name, rn)
			}
		}

		time.Sleep(time.Duration(configs.FILES_UPDATE) * time.Second)
	}
}

func deleteFile(filename string, rn *tgfuse.RootNode) {
	success, live := rn.RmChild(filename)
	if !live {
		panic("Root node was removed")
	}
	if !success {
		logger.LogErr(fmt.Sprintf("Failed to remove node %s", filename))
	} else {
		delete(rn.Nodes, filename)
		logger.LogInfo(fmt.Sprintf("Deleted file %s from root node", filename))
	}
}

func addMissingFile(cf *filesystem.ChunkFile, rn *tgfuse.RootNode) {
	inode := tgfuse.CfInode{File: cf}
	ch := rn.NewInode(
		context.Background(),
		&inode,
		fs.StableAttr{Mode: syscall.S_IFREG | 0o755},
	)
	rn.AddChild(cf.OriginalFilename, ch, true)
	rn.Nodes[cf.OriginalFilename] = &inode
	logger.LogInfo(fmt.Sprintf("Added new file to filesystem: %s", cf.OriginalFilename))
}
