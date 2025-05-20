package tgfuse

import (
	"context"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"it.smaso/tgfuse/filesystem"
	"log"
	"sort"
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
	end := off + int64(len(dest))
	if end > int64(cf.File.OriginalSize) {
		end = int64(cf.File.OriginalSize)
	}

	wg := sync.WaitGroup{}

	sort.Slice(cf.File.Chunks, func(i, j int) bool {
		return cf.File.Chunks[i].Idx < cf.File.Chunks[j].Idx
	})

	for idx := range cf.File.Chunks {
		ci := &cf.File.Chunks[idx]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := ci.FetchBuffer(); err != nil {
				log.Println("Failed to fetch buffer", err)
			} else {
				log.Println("Fetched buffer", ci.Idx)
			}
		}()
	}
	log.Println("Downloaded all chunks of file")

	wg.Wait()
	bytes := cf.File.GetBytes()

	return fuse.ReadResultData(bytes[off:end]), 0
}

func (cf *CfInode) Open(ctx context.Context, openFlags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// disallow writes
	if fuseFlags&(syscall.O_RDWR|syscall.O_WRONLY) != 0 {
		return nil, 0, syscall.EROFS
	}

	// Return FOPEN_DIRECT_IO so content is not cached.
	return fh, fuse.FOPEN_DIRECT_IO, 0
}
