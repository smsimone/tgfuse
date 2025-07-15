package filesystem

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"slices"

	"github.com/google/uuid"
	"it.smaso/tgfuse/configs"
	"it.smaso/tgfuse/logger"
)

// ChunkFile represents the aggregation of all the chunks
type ChunkFile struct {
	Ino              uint64
	Id               string
	OriginalFilename string
	OriginalSize     int
	NumChunks        int
	Chunks           []ChunkItem
}

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

	return SplitBytes(path.Base(filepath), &fileBytes)
}

func SplitBytes(filename string, fileBytes *[]byte) (*ChunkFile, error) {
	if fileBytes == nil {
		panic("fileBytes must not be nil")
	}

	cf := ChunkFile{
		OriginalFilename: filename,
		OriginalSize:     len(*fileBytes),
		Id:               uuid.NewString(),
	}

	var ci []ChunkItem
	var count int = 0
	for chunk := range slices.Chunk(*fileBytes, configs.CHUNK_SIZE) {
		ci = append(ci, ChunkItem{
			Idx:         count,
			Size:        len(chunk),
			Name:        uuid.NewString(),
			Buf:         bytes.NewBuffer(chunk),
			FileState:   MEMORY,
			FileId:      nil,
			ChunkFileId: cf.Id,
		})
		count++
	}
	cf.Chunks = ci
	cf.NumChunks = count

	return &cf, nil
}

func (cf *ChunkFile) PrefetchChunks(start, end int64) {
	for idx := range cf.Chunks {
		chunk := &cf.Chunks[idx]
		if end <= chunk.Start || start >= chunk.End {
			continue
		}
		if chunk.FileState == MEMORY {
			continue
		}
		chunk.ForceLock()
		go func(item *ChunkItem) {
			if err := item.FetchBuffer(); err != nil {
				logger.LogErr(fmt.Sprintf("Failed to fetch buffer %s", err.Error()))
			}
		}(chunk)
	}
}

func (cf *ChunkFile) GetBytes(start, end int64) []byte {
	var result []byte
	for idx := range cf.Chunks {
		chunk := &cf.Chunks[idx]

		// Skip chunks that do not intersect the requested range
		if end <= chunk.Start || start >= chunk.End {
			// logger.LogInfo(fmt.Sprintf("Skipping chunk %d: doesn't intersect with req(%d, %d) <> chunk(%d, %d)", idx, start, end, chunk.Start, chunk.End))
			continue
		}

		// Compute relative positions within this chunk
		relativeStart := max(0, start-chunk.Start)
		relativeEnd := min(chunk.End-chunk.Start, end-chunk.Start)

		chunk.lock.Lock()
		buf := chunk.Buf.Bytes()

		if relativeStart >= int64(len(buf)) || relativeEnd > int64(len(buf)) {
			logger.LogErr(fmt.Sprintf("Invalid range [%d:%d] for chunk %d (buffer size %d)", relativeStart, relativeEnd, chunk.Idx, len(buf)))
			chunk.lock.Unlock()
			continue
		}

		logger.LogInfo(fmt.Sprintf("Copying bytes from chunk %d [%d:%d]", idx, relativeStart, relativeEnd))
		result = append(result, buf[relativeStart:relativeEnd]...)
		chunk.lock.Unlock()
	}

	return result
}

func (cf *ChunkFile) WriteFile(outFile string) error {
	file, err := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		logger.LogErr(fmt.Sprintf("Failed to open output file: %s", err.Error()))
		return err
	}
	defer file.Close()

	for idx := range cf.Chunks {
		chunk := &cf.Chunks[idx]
		if chunk.Buf == nil {
			return fmt.Errorf("buffer was nil for chunk %d", chunk.Idx)
		}
		_, _ = file.Write(chunk.Buf.Bytes())
	}

	logger.LogInfo(fmt.Sprintf("Wrote file: %s", outFile))
	return nil
}
