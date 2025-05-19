package filesystem

import (
	"bytes"
	"io"
	"os"
	"path"
	"slices"

	"github.com/google/uuid"
	"it.smaso/tgfuse/configs"
)

func ReadChunkfile(filepath string) (*ChunkFile, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	cf := ChunkFile{
		OriginalFilename: path.Base(filepath),
		OriginalSize:     len(fileBytes),
	}

	var ci []ChunkItem
	var count int = 0
	for chunk := range slices.Chunk(fileBytes, configs.CHUNK_SIZE) {
		ci = append(ci, ChunkItem{
			Idx:       count,
			Size:      len(chunk),
			Name:      uuid.NewString(),
			Buf:       bytes.NewBuffer(chunk),
			FileState: MEMORY,
			FileId:    nil,
		})
		count = count + 1
	}
	cf.Chunks = ci
	cf.NumChunks = count

	return &cf, nil
}
