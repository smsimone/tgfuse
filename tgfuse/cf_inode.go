package tgfuse

import (
	"context"
	"log"
	"os"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"it.smaso/tgfuse/filesystem"
)

const TTL = 5 * 60 // seconds

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
	out.Mode = 0o444
	return 0
}

func (h *CfHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	log.Printf(">> CfHandle.Read(): off=%d len=%d", off, len(dest))
	return h.inode.Read(ctx, h, dest, off)
}

func (cf *CfInode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Println("Looking up File", name)
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
		go func(item *filesystem.ChunkItem) {
			defer wg.Done()
			if err := item.FetchBuffer(); err != nil {
				log.Println("Failed to fetch buffer", err)
			}
		}(ci)
	}

	wg.Wait()

	cf.writeTmpFile.Do(func() {
		file, _ := os.Create("random_file.pdf")
		defer func() {
			_ = file.Close()
		}()
		_, err := file.Write(cf.File.GetBytes(0, int64(cf.File.OriginalSize)-1))
		if err != nil {
			log.Println("Failed to write tmp file", err)
		}
	})

	bytes := cf.File.GetBytes(off, end)

	return fuse.ReadResultData(bytes), 0
}

func (cf *CfInode) Open(ctx context.Context, openFlags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	log.Println(">> Open(): flags", openFlags)

	if openFlags&(syscall.O_WRONLY|syscall.O_RDWR) != 0 {
		log.Println("Returning EROFS")
		return nil, 0, syscall.EROFS
	}

	return &CfHandle{inode: cf}, 0, 0
}
