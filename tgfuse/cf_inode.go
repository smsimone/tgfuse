package tgfuse

import (
	"context"
	"log"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"it.smaso/tgfuse/filesystem"
)

const TTL = 10 // 5 * 60 // seconds

type CfInode struct {
	fs.Inode
	File          *filesystem.ChunkFile
	lastRead      time.Time
	currentlyRead bool
}

// ---------------------
// filesystem implementation
// ---------------------

var (
	_ = (fs.NodeOpener)((*CfInode)(nil))
	_ = (fs.NodeReader)((*CfInode)(nil))
	_ = (fs.NodeGetattrer)((*CfInode)(nil))
)

func (cf *CfInode) ReadyForCleanup() bool {
	if cf.currentlyRead {
		return false
	}
	delay := time.Since(cf.lastRead).Seconds()
	return delay > TTL
}

func (cf *CfInode) ClearBuffers() {
	for idx := range cf.File.Chunks {
		ci := &cf.File.Chunks[idx]
		ci.PruneFromRam()
	}
}

func (cf *CfInode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Size = uint64(cf.File.OriginalSize)
	out.Mode = 0o755
	return 0
}

func (cf *CfInode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Println("Looking up File", name)
	out.Mode = 0o755
	out.Size = uint64(cf.File.OriginalSize)
	return cf.NewInode(ctx, cf, fs.StableAttr{Mode: syscall.S_IFREG}), 0
}

func (cf *CfInode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	cf.lastRead = time.Now()
	cf.currentlyRead = true

	end := off + int64(len(dest))
	log.Println("Reading", cf.File.OriginalFilename, "offset", off, "end", end)
	if end > int64(cf.File.OriginalSize) {
		end = int64(cf.File.OriginalSize)
		defer func() {
			cf.currentlyRead = false
		}()
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
			}
		}()
	}

	wg.Wait()
	bytes := cf.File.GetBytes()

	return fuse.ReadResultData(bytes[off:end]), 0
}

func (cf *CfInode) Open(ctx context.Context, openFlags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.Println("Opening file with flags", openFlags)
	if fuseFlags&(syscall.O_RDWR|syscall.O_WRONLY) != 0 {
		return nil, 0, syscall.EROFS
	}
	return fh, fuse.FOPEN_DIRECT_IO, 0
}
