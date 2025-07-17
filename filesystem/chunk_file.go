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

// temporaryFile represents the temporary file containing the
type temporaryFile struct {
	name           string
	handle         *os.File
	bytesAvailable int64 // contains the number of available bytes counting from 0
}

func (tf *temporaryFile) getFile() *os.File {
	if tf.handle != nil {
		return tf.handle
	}
	h, err := os.Open(tf.name)
	if err != nil {
		panic(fmt.Sprintf("Failed to open existing temporary file: %s", err.Error()))
	}
	tf.handle = h
	return tf.handle
}

// ChunkFile represents the aggregation of all the chunks
type ChunkFile struct {
	Ino              uint64
	Id               string
	OriginalFilename string
	OriginalSize     int
	NumChunks        int
	Chunks           []ChunkItem
	tmpFile          *temporaryFile
	isDownloading    bool
}

// ReadChunkFile reads a file given its path and creates its correspondent chunk file
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

// SplitBytes given the bytes of the file, splits then in the correspondent chunk file
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

func (cf *ChunkFile) StartDownload() {
	if cf.isDownloading {
		return
	}

	for idx := range cf.Chunks {
		item := &cf.Chunks[idx]
		item.ForceLock()
	}

	go func() {
		for idx := range cf.Chunks {
			item := &cf.Chunks[idx]
			if err := item.FetchBuffer(cf); err != nil {
				logger.LogErr(fmt.Sprintf("Failed to download chunk item [%d]: %s", item.Idx, err.Error()))
			}
		}
	}()
}

func (cf *ChunkFile) GetBytes(start, end int64) []byte {
	if cf.tmpFile == nil {
		if err := os.MkdirAll(configs.TMP_FILE_FOLDER, 0o755); err != nil {
			logger.LogErr(fmt.Sprintf("Failed to create tmp dir: %s", err.Error()))
		}
		filepath := path.Join(configs.TMP_FILE_FOLDER, cf.Id)
		file, err := os.Create(filepath)
		if err != nil {
			panic(fmt.Sprintf("Failed to open temporary file %s -> %s", filepath, err.Error()))
		}
		file.Truncate(int64(cf.OriginalSize))

		cf.tmpFile = &temporaryFile{
			name:           filepath,
			bytesAvailable: 0,
			handle:         file,
		}
	}

	var result []byte
	for idx := range cf.Chunks {
		chunk := &cf.Chunks[idx]

		if end <= chunk.Start || start >= chunk.End {
			continue
		}

		relativeStart := max(0, start-chunk.Start)
		relativeEnd := min(chunk.End-chunk.Start, end-chunk.Start)

		logger.LogErr(fmt.Sprintf("Locking ChunkFile GetBytes on chunk [%d]", chunk.Idx))
		chunk.ForceLock()

		if relativeStart >= int64(chunk.Size) || relativeEnd > int64(chunk.Size) {
			logger.LogErr(fmt.Sprintf("Invalid range [%d:%d] for chunk %d (buffer size %d)", relativeStart, relativeEnd, chunk.Idx, chunk.Size))
			chunk.lock.Unlock()
			continue
		}

		logger.LogInfo(fmt.Sprintf("Copying bytes from chunk %d [%d:%d]", idx, relativeStart, relativeEnd))
		readBuf := chunk.GetBytes(relativeStart, relativeEnd, cf)
		result = append(result, readBuf...)
		chunk.lock.Unlock()
	}

	return result
}

// WriteFile writes all the chunk files to a file
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
