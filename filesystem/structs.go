package filesystem

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"

	"it.smaso/tgfuse/database"
)

type Status = string

const (
	UPLOADED Status = "uploaded"
	MEMORY   Status = "memory"
)

// ChunkFile aggregates all the chunks that has been uploaded to telegram
type ChunkFile struct {
	Ino              uint64
	Id               string
	OriginalFilename string
	OriginalSize     int
	NumChunks        int
	Chunks           []ChunkItem
}

func (cf *ChunkFile) FetchBytes(start, end int64) ([]byte, error) {
	var (
		startIdx = -1
		endIdx   = -1
	)

	for idx, item := range cf.Chunks {
		if start >= item.Start && start <= item.End {
			startIdx = idx
		}
		if end > item.Start && end <= item.End {
			endIdx = idx
		}
	}

	// fallback per file che finisce esattamente al bordo
	if endIdx == -1 {
		endIdx = len(cf.Chunks) - 1
	}
	if startIdx == -1 {
		return nil, fmt.Errorf("invalid range: offset %d does not fall in any chunk", start)
	}

	log.Println("Reading chunks", startIdx, endIdx)

	errs := make([]error, 0)
	wg := sync.WaitGroup{}
	for idx := startIdx; idx <= endIdx; idx++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := cf.Chunks[i].FetchBuffer(); err != nil {
				log.Println("Failed to fetch chunk", err)
				errs = append(errs, err)
			}
		}(idx)
	}
	wg.Wait()

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	var bytes []byte
	for idx := startIdx; idx <= endIdx; idx++ {
		chunk := cf.Chunks[idx]
		chunkBytes := chunk.Buf.Bytes()

		relativeStart, relativeEnd := chunk.mapOffsets(start, end)
		log.Println("Reading from", relativeStart, "to", relativeEnd)

		sublist := chunkBytes[relativeStart:relativeEnd]
		bytes = append(bytes, sublist...)
	}
	if int64(len(bytes)) != end-start {
		return nil, fmt.Errorf("invalid range: offset %d does not match chunk length %d", start, end-start)
	}

	return bytes, nil
}

func (ci *ChunkItem) mapOffsets(start, end int64) (int, int) {
	relativeStart := int(max(0, start-ci.Start))
	relativeEnd := int(min(ci.End-ci.Start, end-ci.Start))

	if relativeStart < 0 || relativeEnd < 0 || relativeStart > relativeEnd {
		panic(fmt.Sprintf("Invalid relative slice: [%d:%d] in chunk [%d:%d]", relativeStart, relativeEnd, ci.Start, ci.End))
	}

	return relativeStart, relativeEnd
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

// ChunkItem represents the single byte chunk that has been sent to telegram. Each one of these
// is a partition of the ChunkFile
type ChunkItem struct {
	Idx         int
	Size        int
	Name        string
	Buf         *bytes.Buffer
	FileId      *string
	FileState   Status
	chunkFileId string

	Start int64
	End   int64
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
