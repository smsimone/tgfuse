package tgfuse

import (
	"context"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"log"
	"syscall"
)

type RootNode struct {
	fs.Inode
	Nodes map[string]*CfInode
}

var _ = (fs.NodeCreater)((*RootNode)(nil))

func (rn *RootNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.Println("Creating File", name)
	out.Mode = mode
	out.Size = 0

	n := rn.NewInode(
		ctx,
		&baseInode{name: name, mode: mode},
		fs.StableAttr{Mode: mode},
	)
	return n, nil, 0, syscall.F_OK
}

func (rn *RootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Println("Looking up File", name)

	node, ok := rn.Nodes[name]
	if !ok {
		log.Println("Requested non existent file", name)
		return nil, syscall.ENOENT
	}

	out.Mode = 0o444
	out.Size = uint64(node.File.OriginalSize)

	stable := fs.StableAttr{Mode: syscall.S_IFREG}
	return rn.NewInode(ctx, node, stable), 0
}
