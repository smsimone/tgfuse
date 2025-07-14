package filesystem

import (
	"bytes"
	"fmt"
	"strconv"

	"it.smaso/tgfuse/database"
)

type Status = string

const (
	UPLOADED Status = "uploaded"
	MEMORY   Status = "memory"
)

type ChunkFile struct {
	Ino              uint64
	Id               string
	OriginalFilename string
	OriginalSize     int
	NumChunks        int
	Chunks           []ChunkItem
	fullByte         *[]byte
}

func (cf *ChunkFile) GetKeyParams() []database.KeyParam {
	return []database.KeyParam{
		{
			Key: fmt.Sprintf("/cf/%s/filename", cf.Id),
			GetValue: func() string {
				return cf.OriginalFilename
			},
			SetValue: func(s string) {
				cf.OriginalFilename = s
			},
		},
		{
			Key: fmt.Sprintf("/cf/%s/size", cf.Id),
			GetValue: func() string {
				return fmt.Sprintf("%d", cf.OriginalSize)
			},
			SetValue: func(s string) {
				val, _ := strconv.Atoi(s)
				cf.OriginalSize = val
			},
		},
		{
			Key: fmt.Sprintf("/cf/%s/num_chunks", cf.Id),
			GetValue: func() string {
				return fmt.Sprintf("%d", cf.NumChunks)
			},
			SetValue: func(s string) {
				val, _ := strconv.Atoi(s)
				cf.NumChunks = val
			},
		},
	}
}

type ChunkItem struct {
	Idx         int
	Size        int
	Name        string
	Buf         *bytes.Buffer
	FileId      *string
	FileState   Status
	chunkFileId string

	Start int64
	End   int64
}

func (c *ChunkItem) PruneFromRam() {
	if c.FileState == MEMORY {
		c.Buf = nil
		c.FileState = UPLOADED
		c.Buf = &bytes.Buffer{}
	}
}

func (c *ChunkItem) GetKeyParams() []database.KeyParam {
	return []database.KeyParam{
		{
			Key: fmt.Sprintf("/ci/%s/%d/size", c.chunkFileId, c.Idx),
			GetValue: func() string {
				return strconv.Itoa(c.Size)
			},
			SetValue: func(s string) {
				c.Size, _ = strconv.Atoi(s)
			},
		},
		{
			Key: fmt.Sprintf("/ci/%s/%d/name", c.chunkFileId, c.Idx),
			GetValue: func() string {
				return c.Name
			},
			SetValue: func(s string) {
				c.Name = s
			},
		},
		{
			Key: fmt.Sprintf("/ci/%s/%d/file_id", c.chunkFileId, c.Idx),
			GetValue: func() string {
				return *c.FileId
			},
			SetValue: func(s string) {
				c.FileId = &s
			},
		},
	}
}
