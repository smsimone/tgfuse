package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type TgFuse struct {
	fs.Inode
}

func (tg *TgFuse) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	return 0
}

func (r *TgFuse) OnAdd(ctx context.Context) {
	ch := r.NewPersistentInode(
		ctx, &fs.MemRegularFile{
			Data: []byte("file.txt"),
			Attr: fuse.Attr{
				Mode: 0644,
			},
		}, fs.StableAttr{Ino: 2})
	r.AddChild("file.txt", ch, false)
}

func main() {
	args := os.Args
	if len(args) < 2 {
		fmt.Println("Missing mounting point")
		os.Exit(1)
	}

	server, err := fs.Mount(args[1], &TgFuse{}, &fs.Options{})
	if err != nil {
		log.Fatalf("Mount failed: %v\n", err)
	}
	server.Wait()
}

// func main() {
// 	restore()
// 	os.Exit(1)
//
// 	args := os.Args
// 	if len(args) < 2 {
// 		fmt.Println("missing file")
// 		os.Exit(1)
// 	}
//
// 	file := args[1]
// 	cf, err := filesystem.ReadChunkfile(file)
// 	if err != nil {
// 		fmt.Printf("Failed to read chunk file: %s", err.Error())
// 		os.Exit(1)
// 	}
// 	if cf.NumChunks == 0 {
// 		fmt.Println("Failed to split file in chunks")
// 		os.Exit(1)
// 	}
//
// 	fmt.Println("Prepared chunk file", cf.NumChunks, cf.Id)
// 	for idx := range cf.Chunks {
// 		err := cf.Chunks[idx].Send()
// 		if err != nil {
// 			fmt.Println(err)
// 			os.Exit(1)
// 		}
// 		if cf.Chunks[idx].Buf != nil || cf.Chunks[idx].FileId == nil {
// 			fmt.Println("Failed to update internal state")
// 			os.Exit(1)
// 		}
// 	}
// 	fmt.Println("Sent files to telegram")
//
// 	if err = cf.UploadToDatabase(); err != nil {
// 		fmt.Printf("Failed to upload file: %s", err.Error())
// 		os.Exit(1)
// 	}
//
// 	fmt.Println("Uploaded keys to etcd server")
//
// 	files, err := filesystem.FetchFromEtcd()
// 	if err != nil {
// 		fmt.Printf("Failed to restore from etcd: %s", err)
// 		os.Exit(1)
// 	}
//
// 	for _, cf := range *files {
// 		fmt.Println(cf.Id, cf.NumChunks)
// 	}
//
// 	restore()
// }
//
// func restore() {
// 	files, err := filesystem.FetchFromEtcd()
// 	if err != nil {
// 		fmt.Println("Failed to restore from etcd", err)
// 		os.Exit(1)
// 	}
//
// 	for _, cf := range *files {
// 		for idx := range cf.Chunks {
// 			if err := cf.Chunks[idx].FetchBuffer(); err != nil {
// 				fmt.Println("Failed to download chunk item buf", err)
// 			}
// 			fmt.Println("Fetched buffer for", idx)
// 		}
//
// 		fmt.Println(cf.Id, cf.NumChunks)
// 		if err := cf.WriteFile(fmt.Sprintf("./%s", cf.OriginalFilename)); err != nil {
// 			fmt.Println(err)
// 		}
// 	}
// }
//
