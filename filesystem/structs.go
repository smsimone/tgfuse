package filesystem

import (
	"bytes"
	"fmt"
	"it.smaso/tgfuse/database"
	"strconv"
)

type Status = string

const (
	UPLOADED Status = "uploaded"
	MEMORY   Status = "memory"
)

type ChunkFile struct {
	Id               string
	OriginalFilename string
	OriginalSize     int
	NumChunks        int
	Chunks           []ChunkItem
}

func (c ChunkFile) GetKeyParams() []database.KeyParam {
	return []database.KeyParam{
		{
			Key: fmt.Sprintf("/ci/%s/filename", c.Id),
			GetValue: func() string {
				return c.OriginalFilename
			},
			SetValue: func(s string) {
				c.OriginalFilename = s
			},
		},
		{
			Key: fmt.Sprintf("/ci/%s/size", c.Id),
			GetValue: func() string {
				return fmt.Sprintf("%d", c.OriginalSize)
			},
			SetValue: func(s string) {
				val, _ := strconv.Atoi(s)
				c.OriginalSize = val
			},
		},
		{
			Key: fmt.Sprintf("/ci/%s/num_chunks", c.Id),
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
	Idx       int
	Size      int
	Name      string
	Buf       *bytes.Buffer
	FileId    *string
	FileState Status
}
