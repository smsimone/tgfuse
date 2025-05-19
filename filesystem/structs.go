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
}

func (c *ChunkFile) GetKeyParams() []database.KeyParam {
	return []database.KeyParam{
		{
			Key: fmt.Sprintf("/cf/%s/filename", c.Id),
			GetValue: func() string {
				return c.OriginalFilename
			},
			SetValue: func(s string) {
				c.OriginalFilename = s
			},
		},
		{
			Key: fmt.Sprintf("/cf/%s/size", c.Id),
			GetValue: func() string {
				return fmt.Sprintf("%d", c.OriginalSize)
			},
			SetValue: func(s string) {
				val, _ := strconv.Atoi(s)
				c.OriginalSize = val
			},
		},
		{
			Key: fmt.Sprintf("/cf/%s/num_chunks", c.Id),
			GetValue: func() string {
				return fmt.Sprintf("%d", c.NumChunks)
			},
			SetValue: func(s string) {
				val, _ := strconv.Atoi(s)
				c.NumChunks = val
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
