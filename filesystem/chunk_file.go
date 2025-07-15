package filesystem

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"strconv"
	"sync"

	"github.com/google/uuid"
	"it.smaso/tgfuse/configs"
	"it.smaso/tgfuse/database"
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
			chunkFileId: cf.Id,
		})
		count++
	}
	cf.Chunks = ci
	cf.NumChunks = count

	return &cf, nil
}

func FetchFromEtcd() (*[]ChunkFile, error) {
	cfIds, err := database.GetAllFileIds()
	if err != nil {
		return nil, err
	}

	var chunkFiles []ChunkFile
	wg := sync.WaitGroup{}
	var mutex sync.Mutex

	errs := make([]error, 0)
	for _, cfId := range *cfIds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cf := ChunkFile{Id: cfId, Chunks: []ChunkItem{}}

			if err := database.Restore(&cf); err != nil {
				logger.LogErr(fmt.Sprintf("Failed to restore cf: %s", err.Error()))
				mutex.Lock()
				errs = append(errs, fmt.Errorf("failed to restore cf: %v", err))
				mutex.Unlock()
				return
			}

			var curr int64 = 0
			for ciIdx := range cf.NumChunks {
				ci := ChunkItem{Idx: ciIdx, chunkFileId: cfId, Start: curr}
				if err := database.Restore(&ci); err != nil {
					logger.LogErr(fmt.Sprintf("Failed to restore cf %s", err.Error()))
				} else {
					ci.End = ci.Start + int64(ci.Size)
					curr += int64(ci.Size)
					cf.Chunks = append(cf.Chunks, ci)
				}
			}

			mutex.Lock()
			chunkFiles = append(chunkFiles, cf)
			mutex.Unlock()
		}()

	}
	wg.Wait()

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return &chunkFiles, nil
}

func (cf *ChunkFile) UploadToDatabase() error {
	if err := database.SendFile(cf); err != nil {
		logger.LogErr(fmt.Sprintf("Failed to send ChunkFile to database: %s", err.Error()))
		return err
	}

	for idx := range cf.Chunks {
		chunk := &cf.Chunks[idx]
		if chunk.FileId == nil {
			logger.LogErr("Somehow the file id came null")
			os.Exit(1)
		}
		if err := database.SendFile(chunk); err != nil {
			logger.LogErr(fmt.Sprintf("Failed to send ChunkItem to database: %s", err.Error()))
			return err
		}
	}

	return nil
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

func (cf *ChunkFile) GetKeyParams() []database.KeyParam {
	return []database.KeyParam{
		{
			Key: fmt.Sprintf("/cf/%s/filename", cf.Id),
			GetValue: func() string {
				return cf.OriginalFilename
			},
			SetValue: func(s string) {
				cf.OriginalFilename = s
			},
		},
		{
			Key: fmt.Sprintf("/cf/%s/size", cf.Id),
			GetValue: func() string {
				return fmt.Sprintf("%d", cf.OriginalSize)
			},
			SetValue: func(s string) {
				val, _ := strconv.Atoi(s)
				cf.OriginalSize = val
			},
		},
		{
			Key: fmt.Sprintf("/cf/%s/num_chunks", cf.Id),
			GetValue: func() string {
				return fmt.Sprintf("%d", cf.NumChunks)
			},
			SetValue: func(s string) {
				val, _ := strconv.Atoi(s)
				cf.NumChunks = val
			},
		},
	}
}
