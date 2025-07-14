package tgfuse

import (
	"context"
	"log"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"it.smaso/tgfuse/filesystem"
)

const TTL = 10 // 5 * 60 // seconds

// CfInode represents the inode on the machine and
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
	// TODO: if reading using "file" command, return only the metadata

	end := off + int64(len(dest))
	log.Println("Reading", cf.File.OriginalFilename, "offset", off, "end", end)
	cf.lastRead = time.Now()
	cf.currentlyRead = true

	if end > int64(cf.File.OriginalSize) {
		end = int64(cf.File.OriginalSize)
		defer func() {
			cf.currentlyRead = false
		}()
	}

	bytes, err := cf.File.FetchBytes(off, end)
	if err != nil {
		log.Println("Error reading file", err)
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(bytes), 0
}

func (cf *CfInode) Open(ctx context.Context, openFlags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.Println("Opening file with flags", openFlags)
	if fuseFlags&(syscall.O_RDWR|syscall.O_WRONLY) != 0 {
		return nil, 0, syscall.EROFS
	}
	return fh, fuse.FOPEN_DIRECT_IO, 0
}
