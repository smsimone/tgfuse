package tgfuse

import (
	"context"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"syscall"
	"time"
)

type RootNode struct {
	fs.Inode
	Nodes map[string]*CfInode
}

var (
	// _ = (fs.NodeCreater)((*RootNode)(nil))
	_ = (fs.NodeReaddirer)((*RootNode)(nil))
	_ = (fs.NodeGetattrer)((*RootNode)(nil))
	_ = (fs.NodeLookuper)((*RootNode)(nil))
)

func (rn *RootNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0o755
	out.Mtime = uint64(time.Now().UnixMilli())
	out.Atime = uint64(time.Now().UnixMilli())
	return 0
}

func (rn *RootNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{
			Name: ".",
			Mode: fuse.S_IFDIR,
		},
		{
			Name: "..",
			Mode: fuse.S_IFDIR,
		},
	}

	for _, node := range rn.Nodes {
		entries = append(entries, fuse.DirEntry{
			Name: node.File.OriginalFilename,
			Mode: node.Inode.Mode(),
		})

	}
	return fs.NewListDirStream(entries), 0
}

func (rn *RootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	node, ok := rn.Children()[name]
	if !ok {
		return nil, syscall.ENOENT
	}
	cfNode := rn.Nodes[name]

	attr := node.StableAttr()
	out.Attr.Mode = attr.Mode
	out.Attr.Ino = attr.Ino
	out.Size = uint64(cfNode.File.OriginalSize)

	// out.Attr.Size = attr.res
	out.SetEntryTimeout(1 * time.Second)
	out.SetAttrTimeout(1 * time.Second)

	return node, 0
}

// func (rn *RootNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
// 	log.Println("Creating File", name)
// 	out.Mode = mode
// 	out.Size = 0
//
// 	return n, nil, 0, 0
// }
