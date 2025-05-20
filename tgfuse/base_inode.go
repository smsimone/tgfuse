package tgfuse

import (
	"context"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"log"
	"syscall"
)

type baseInode struct {
	fs.Inode
	data []byte
	name string
	mode uint32
}

var _ = (fs.NodeWriter)((*baseInode)(nil))
var _ = (fs.NodeGetattrer)((*baseInode)(nil))
var _ = (fs.NodeReader)((*baseInode)(nil))
var _ = (fs.NodeOpener)((*baseInode)(nil))

func (bi *baseInode) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (written uint32, errno syscall.Errno) {
	log.Println("Writing data to", bi.name)
	bi.data = data
	return uint32(len(data)), syscall.F_OK
}

func (bi *baseInode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	bufLen := len(bi.data)
	end := int(off) + bufLen
	if end > bufLen {
		end = bufLen
	}

	return fuse.ReadResultData(bi.data[off:end]), 0
}

func (bi *baseInode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Size = uint64(len(bi.data))
	out.Mode = bi.mode
	return 0
}

func (bi *baseInode) Open(ctx context.Context, openFlags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.Println("Opening data from", bi.name)
	// disallow writes
	if fuseFlags&(syscall.O_RDWR|syscall.O_WRONLY) != 0 {
		return nil, 0, syscall.EROFS
	}

	// Return FOPEN_DIRECT_IO so content is not cached.
	return fh, fuse.FOPEN_DIRECT_IO, 0
}
