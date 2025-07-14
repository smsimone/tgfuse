package tgfuse

import (
	"context"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"it.smaso/tgfuse/filesystem"
	"log"
	"sync"
	"syscall"
)

type virtualInode struct {
	fs.Inode
	data []byte
	name string
	mode uint32
}

var (
	_ = (fs.NodeWriter)((*virtualInode)(nil))
	_ = (fs.NodeGetattrer)((*virtualInode)(nil))
	_ = (fs.NodeReader)((*virtualInode)(nil))
	_ = (fs.NodeOpener)((*virtualInode)(nil))
	_ = (fs.NodeFlusher)((*virtualInode)(nil))
)

func (bi *virtualInode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= int64(len(bi.data)) {
		return fuse.ReadResultData(nil), 0
	}
	end := int(off) + len(dest)
	if end > len(bi.data) {
		end = len(bi.data)
	}
	return fuse.ReadResultData(bi.data[off:end]), 0
}

func (bi *virtualInode) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	newLen := int(off) + len(data)
	if newLen > cap(bi.data) {
		newBuf := make([]byte, newLen)
		copy(newBuf, bi.data)
		bi.data = newBuf
	} else if newLen > len(bi.data) {
		bi.data = bi.data[:newLen]
	}
	copy(bi.data[off:], data)
	// log.Printf("Wrote %d bytes to %s at offset %d\n", len(data), bi.name, off)
	return uint32(len(data)), 0
}

func (bi *virtualInode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Size = uint64(len(bi.data))
	out.Mode = bi.mode
	return 0
}

func (bi *virtualInode) Open(ctx context.Context, openFlags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.Println("Opening data from", bi.name)
	if fuseFlags&(syscall.O_RDWR|syscall.O_WRONLY) != 0 {
		return nil, 0, syscall.EROFS
	}

	return bi, fuse.FOPEN_DIRECT_IO, 0
}

func (bi *virtualInode) Flush(ctx context.Context, f fs.FileHandle) syscall.Errno {
	log.Println("Flushing data from", bi.name)

	cf, err := filesystem.SplitBytes(bi.name, &bi.data)
	if err != nil {
		log.Println("Failed to split file into chunkfile", err)
		return syscall.EIO
	}

	wg := sync.WaitGroup{}
	for idx := range cf.Chunks {
		wg.Add(1)
		go func(chunk *filesystem.ChunkItem) {
			defer wg.Done()
			if err := chunk.Send(); err != nil {
				log.Println("Failed to send chunk", err)
			}
		}(&cf.Chunks[idx])
	}
	wg.Wait()

	if err := cf.UploadToDatabase(); err != nil {
		log.Println("Failed to upload to database", err)
		return syscall.EIO
	}

	return 0
}
