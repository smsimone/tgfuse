package filesystem

import (
	"bytes"

	"it.smaso/tgfuse/telegram"
)

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

func (ci *ChunkItem) FetchBuffer() error {
	if ci.FileState == MEMORY {
		return nil
	}
	bts, err := telegram.DownloadFile(*ci.FileId)
	if err != nil {
		return err
	}
	ci.Buf = bytes.NewBuffer(*bts)
	ci.FileState = MEMORY
	return nil
}
