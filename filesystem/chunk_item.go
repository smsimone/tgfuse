package filesystem

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"

	"it.smaso/tgfuse/database"
	"it.smaso/tgfuse/logger"
	"it.smaso/tgfuse/telegram"
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
	return bytes.NewBuffer(ci.Buf.Bytes())
}

func (ci *ChunkItem) GetName() string {
	return ci.Name
}

func (ci *ChunkItem) CanBeSent() bool {
	return ci.FileState == MEMORY && ci.Buf.Len() > 0
}

func (ci *ChunkItem) Send() error {
	if !ci.CanBeSent() {
		panic(fmt.Sprintf("Cannot send chunk [%d]", ci.Idx))
	}
	fileID, err := telegram.SendFile(ci)
	if err != nil {
		logger.LogErr(fmt.Sprintf("Chunk [%d] has not been sent", ci.Idx))
		return err
	}
	ci.FileId = fileID
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
