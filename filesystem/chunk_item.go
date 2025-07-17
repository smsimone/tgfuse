package filesystem

import (
	"bytes"
	"fmt"
	"sync"

	"it.smaso/tgfuse/logger"
	"it.smaso/tgfuse/telegram"
)

type Status = string
type ChunkItemOpts = func(*ChunkItem)

const (
	UPLOADED Status = "uploaded"
	MEMORY   Status = "memory"
	FILE     Status = "file"
)

// ChunkItem is the single chunk that has been uploaded
type ChunkItem struct {
	Idx           int
	Size          int
	Name          string
	Buf           *bytes.Buffer
	FileId        *string
	FileState     Status
	ChunkFileId   string
	lock          sync.RWMutex
	isDownloading bool

	Start int64
	End   int64
}

func NewChunkItem(opts ...ChunkItemOpts) *ChunkItem {
	inst := &ChunkItem{
		isDownloading: false,
		FileState:     UPLOADED,
	}
	for _, opt := range opts {
		opt(inst)
	}
	return inst
}

func WithIdx(idx int) func(*ChunkItem) {
	return func(ci *ChunkItem) {
		ci.Idx = idx
	}
}
func WithChunkFileId(cfId string) func(*ChunkItem) {
	return func(ci *ChunkItem) {
		ci.ChunkFileId = cfId
	}
}
func WithStart(start int64) func(*ChunkItem) {
	return func(ci *ChunkItem) {
		ci.Start = start
	}
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

// shouldBeDownloaded check wether the chunk must be downloaded or if it's already downloaded
func (ci *ChunkItem) shouldBeDownloaded() bool {
	if ci.isDownloading {
		return false
	}
	return ci.FileState != FILE && ci.FileState != MEMORY
}

func (ci *ChunkItem) fetchBuffer(cf *ChunkFile) error {
	ci.isDownloading = true
	defer func() {
		ci.isDownloading = false
	}()

	bts, err := telegram.GetInstance().DownloadFile(*ci.FileId)
	if err != nil {
		logger.LogErr(fmt.Sprintf("failed to download chunk [%d]: %s", ci.Idx, err.Error()))
		return err
	}

	logger.LogInfo(fmt.Sprintf("downloaded chunk [%d] from telegram", ci.Idx))
	ci.Buf = bytes.NewBuffer(*bts)
	ci.FileState = MEMORY

	// moves the bytes out of ram
	if cf.tmpFile != nil {
		handle := cf.tmpFile.getFile()
		if _, err := handle.WriteAt(*bts, ci.Start); err != nil {
			logger.LogErr(fmt.Sprintf("Failed to write chunk [%d] to tmp file: %s", ci.Idx, err.Error()))
		} else {
			ci.FileState = FILE
			ci.Buf = nil
		}
	}

	return nil
}

func (ci *ChunkItem) GetBytes(start, end int64, cf *ChunkFile) []byte {
	ci.lock.RLocker().Lock()
	defer ci.lock.RLocker().Unlock()

	logger.LogInfo(fmt.Sprintf("Getting bytes of chunk [%d]", ci.Idx))

	switch ci.FileState {
	case MEMORY:
		return ci.Buf.Bytes()[start:end]
	case FILE:
		file := cf.tmpFile.getFile()
		buf := make([]byte, end-start)
		_, err := file.ReadAt(buf, ci.Start+start)
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
	switch ci.FileState {
	case MEMORY:
		ci.Buf = nil
		ci.FileState = UPLOADED
		ci.Buf = &bytes.Buffer{}
	case FILE:
		ci.Buf = nil
		ci.FileState = UPLOADED
	}
}
