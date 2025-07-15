package filesystem

import (
	"bytes"
	"fmt"
	"sync"

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
	ChunkFileId string
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
