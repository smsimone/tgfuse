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
	if err := telegram.SendFile(ci); err != nil {
		return err
	}
	ci.Buf = nil
	ci.FileState = UPLOADED
	return nil
}
