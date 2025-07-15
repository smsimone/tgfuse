package tgfuse

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"it.smaso/tgfuse/configs"
	"it.smaso/tgfuse/filesystem"
	"it.smaso/tgfuse/logger"
)

type CfInode struct {
	fs.Inode
	File          *filesystem.ChunkFile
	lastRead      time.Time
	currentlyRead bool
	writeTmpFile  sync.Once
}

type CfHandle struct {
	fs.FileHandle
	inode *CfInode
}

// ---------------------
// filesystem implementation
// ---------------------

var (
	_ = (fs.NodeOpener)((*CfInode)(nil))
	_ = (fs.NodeReader)((*CfInode)(nil))
	_ = (fs.NodeGetattrer)((*CfInode)(nil))
)

var (
	_ = (fs.FileHandle)((*CfHandle)(nil))
	_ = (fs.FileHandle)((*CfHandle)(nil))
	_ = (fs.FileReader)((*CfHandle)(nil))
)

func (cf *CfInode) ReadyForCleanup() bool {
	if cf.currentlyRead {
		return false
	}
	delay := time.Since(cf.lastRead).Seconds()
	return delay > configs.CHUNK_TTL
}

func (cf *CfInode) ClearBuffers() {
	for idx := range cf.File.Chunks {
		ci := &cf.File.Chunks[idx]
		ci.PruneFromRam()
	}
}

func (cf *CfInode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Size = uint64(cf.File.OriginalSize)
	out.Mode = 0o444
	return 0
}

func (h *CfHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	return h.inode.Read(ctx, h, dest, off)
}

func (cf *CfInode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logger.LogInfo(fmt.Sprintf("Looking up File %s", name))
	out.Mode = 0o755
	out.Size = uint64(cf.File.OriginalSize)
	return &cf.Inode, 0
}

func (cf *CfInode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	cf.lastRead = time.Now()
	cf.currentlyRead = true
	defer func() {
		cf.currentlyRead = false
	}()

	end := min(off+int64(len(dest)), int64(cf.File.OriginalSize))

	sort.Slice(cf.File.Chunks, func(i, j int) bool {
		return cf.File.Chunks[i].Idx < cf.File.Chunks[j].Idx
	})

	cf.File.PrefetchChunks(off, end)

	bytes := cf.File.GetBytes(off, end)
	return fuse.ReadResultData(bytes), 0
}

func (cf *CfInode) Open(ctx context.Context, openFlags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if openFlags&(syscall.O_WRONLY|syscall.O_RDWR) != 0 {
		return nil, 0, syscall.EROFS
	}

	return &CfHandle{inode: cf}, 0, 0
}
