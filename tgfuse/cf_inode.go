package tgfuse

import (
	"context"
	"fmt"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"it.smaso/tgfuse/filesystem"
	"log"
	"sync"
	"syscall"
)

type CfInode struct {
	fs.Inode
	File *filesystem.ChunkFile
}

// ---------------------
// filesystem implementation
// ---------------------

var _ = (fs.NodeOpener)((*CfInode)(nil))
var _ = (fs.NodeReader)((*CfInode)(nil))
var _ = (fs.NodeGetattrer)((*CfInode)(nil))

func (cf *CfInode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Size = uint64(cf.File.OriginalSize)
	out.Mode = 0755
	return 0
}

func (cf *CfInode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Println("Looking up File", name)
	out.Mode = 0755
	out.Size = uint64(cf.File.OriginalSize)
	return cf.NewInode(ctx, cf, fs.StableAttr{Mode: syscall.S_IFREG}), 0
}

func (cf *CfInode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	log.Println("Reading", cf.File.OriginalFilename)
	wg := sync.WaitGroup{}
	for idx := range cf.File.Chunks {
		go func() {
			wg.Add(1)
			defer wg.Done()
			if err := cf.File.Chunks[idx].FetchBuffer(); err != nil {
				fmt.Println("Failed to download chunk item buf", err)
			}
			log.Println("Downloaded", cf.File.Chunks[idx].FileId)
		}()
	}
	wg.Wait()

	log.Println("Reading", cf.File.OriginalFilename)
	bytes := cf.File.GetBytes()
	log.Println("Reading", len(bytes), "bytes")
	end := off + int64(len(dest))
	if end > int64(len(bytes)) {
		end = int64(len(bytes))
	}

	return fuse.ReadResultData(bytes[off:end]), 0
}

func (cf *CfInode) Open(ctx context.Context, openFlags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.Println("Opening file", cf.File.OriginalFilename)
	// disallow writes
	if fuseFlags&(syscall.O_RDWR|syscall.O_WRONLY) != 0 {
		return nil, 0, syscall.EROFS
	}

	// Return FOPEN_DIRECT_IO so content is not cached.
	return fh, fuse.FOPEN_DIRECT_IO, 0
}
