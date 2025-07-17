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
	FILE     Status = "file"
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

func (ci *ChunkItem) FetchBuffer(cf *ChunkFile) error {
	if ci.FileState == MEMORY || ci.FileState == FILE {
		logger.LogErr(fmt.Sprintf("ignored request to fetch already downloaded buffer for chunk [%d]", ci.Idx))
		return nil
	}
	bts, err := telegram.GetInstance().DownloadFile(*ci.FileId)
	if err != nil {
		logger.LogErr(fmt.Sprintf("failed to download chunk [%d]: %s", ci.Idx, err.Error()))
		return err
	}
	defer ci.lock.Unlock()

	logger.LogInfo(fmt.Sprintf("downloaded chunk [%d] from telegram", ci.Idx))
	ci.Buf = bytes.NewBuffer(*bts)
	ci.FileState = MEMORY

	// moves the bytes out of ram
	if cf.tmpFile != nil {
		handle := cf.tmpFile.getFile()
		_, err := handle.WriteAt(ci.GetBuffer().Bytes(), ci.Start)
		if err != nil {
			logger.LogErr(fmt.Sprintf("Failed to write chunk [%d] to tmp file: %s", ci.Idx, err.Error()))
		} else {
			ci.FileState = FILE
			ci.Buf = nil
			logger.LogInfo(fmt.Sprintf("Wrote chunk [%d] to tmp file", ci.Idx))
		}
	}

	return nil
}

func (ci *ChunkItem) GetBytes(start, end int64, cf *ChunkFile) []byte {
	logger.LogInfo(fmt.Sprintf("Getting bytes of chunk [%d]", ci.Idx))

	switch ci.FileState {
	case MEMORY:
		return ci.Buf.Bytes()[start:end]
	case FILE:
		file := cf.tmpFile.getFile()
		buf := make([]byte, ci.Size)
		_, err := file.ReadAt(buf, ci.Start)
		if err != nil {
			logger.LogErr(fmt.Sprintf("Failed to read bytes for chunk [%d] from tmp file: %s", ci.Idx, err.Error()))
		}
		return buf
	case UPLOADED:
		logger.LogErr(fmt.Sprintf("Chunk [%d] has not been downloaded yet", ci.Idx))
		return []byte{}
	}

	return []byte{}
}

func (ci *ChunkItem) PruneFromRam() {
	if ci.FileState == MEMORY {
		ci.Buf = nil
		ci.FileState = UPLOADED
		ci.Buf = &bytes.Buffer{}
	}
}
