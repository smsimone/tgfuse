package filesystem

import (
	"bytes"
	"fmt"
	"it.smaso/tgfuse/database"
	"it.smaso/tgfuse/telegram"
	"strconv"
	"sync"
)

type Status = string

const (
	UPLOADED Status = "uploaded"
	MEMORY   Status = "memory"
)

// ChunkItem is the single chunk that has been uploaded
type ChunkItem struct {
	Idx         int
	Size        int
	Name        string
	Buf         *bytes.Buffer
	FileId      *string
	FileState   Status
	chunkFileId string
	lock        sync.Mutex

	Start int64
	End   int64
}

func (ci *ChunkItem) GetBuffer() *bytes.Buffer {
	return ci.Buf
}

func (ci *ChunkItem) GetName() string {
	return ci.Name
}

func (ci *ChunkItem) Send() error {
	fileId, err := telegram.SendFile(ci)
	if err != nil {
		return err
	}
	ci.FileId = fileId
	ci.Buf = nil
	ci.FileState = UPLOADED
	return nil
}

func (ci *ChunkItem) ForceLock() {
	ci.lock.Lock()
}

func (ci *ChunkItem) FetchBuffer() error {
	defer ci.lock.Unlock()
	if ci.FileState == MEMORY {
		return nil
	}
	bts, err := telegram.GetInstance().DownloadFile(*ci.FileId)
	if err != nil {
		return err
	}
	ci.Buf = bytes.NewBuffer(*bts)
	ci.FileState = MEMORY
	return nil
}

func (ci *ChunkItem) PruneFromRam() {
	if ci.FileState == MEMORY {
		ci.Buf = nil
		ci.FileState = UPLOADED
		ci.Buf = &bytes.Buffer{}
	}
}

func (ci *ChunkItem) GetKeyParams() []database.KeyParam {
	return []database.KeyParam{
		{
			Key: fmt.Sprintf("/ci/%s/%d/size", ci.chunkFileId, ci.Idx),
			GetValue: func() string {
				return strconv.Itoa(ci.Size)
			},
			SetValue: func(s string) {
				ci.Size, _ = strconv.Atoi(s)
			},
		},
		{
			Key: fmt.Sprintf("/ci/%s/%d/name", ci.chunkFileId, ci.Idx),
			GetValue: func() string {
				return ci.Name
			},
			SetValue: func(s string) {
				ci.Name = s
			},
		},
		{
			Key: fmt.Sprintf("/ci/%s/%d/file_id", ci.chunkFileId, ci.Idx),
			GetValue: func() string {
				return *ci.FileId
			},
			SetValue: func(s string) {
				ci.FileId = &s
			},
		},
	}
}
