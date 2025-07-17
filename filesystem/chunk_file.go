package filesystem

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"it.smaso/tgfuse/configs"
	"it.smaso/tgfuse/filesystem/atime"
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

func (tf *temporaryFile) getLastAccessTime() (*time.Time, error) {
	if stat, err := os.Stat(tf.name); err != nil {
		return nil, err
	} else {
		if at := atime.GetAtime(stat); at == nil {
			modTime := stat.ModTime()
			return &modTime, nil
		} else {
			return at, nil
		}
	}
}

type ChunkFileOpt = func(*ChunkFile)

// ChunkFile represents the aggregation of all the chunks
type ChunkFile struct {
	Ino              uint64
	Id               string
	OriginalFilename string
	OriginalSize     int
	NumChunks        int
	Chunks           []*ChunkItem
	tmpFile          *temporaryFile
	isDownloading    bool
	readyMutex       sync.Mutex
	readyToDownload  bool
}

func NewChunkFile(opts ...ChunkFileOpt) *ChunkFile {
	cf := &ChunkFile{
		Chunks:          []*ChunkItem{},
		readyToDownload: false,
	}
	cf.readyMutex.Lock()
	for _, opt := range opts {
		opt(cf)
	}
	return cf
}
func WithId(id string) func(*ChunkFile) {
	return func(cf *ChunkFile) {
		cf.Id = id

		filepath := path.Join(configs.TMP_FILE_FOLDER, cf.Id)
		if stat, err := os.Stat(filepath); err == nil {
			cf.tmpFile = &temporaryFile{
				name:           filepath,
				bytesAvailable: stat.Size(),
			}
		}
	}
}

func (cf *ChunkFile) Enable() {
	logger.LogInfo(fmt.Sprintf("File '%s' is now ready to be read", cf.OriginalFilename))
	cf.readyMutex.Unlock()
	cf.readyToDownload = true
	logger.LogInfo(fmt.Sprintf("File '%s' -> %v [%p]", cf.OriginalFilename, cf.readyToDownload, cf))
	logger.LogInfo(fmt.Sprintf("File '%s' has %d chunks [%p]", cf.OriginalFilename, len(cf.Chunks), cf))
}

func (cf *ChunkFile) WaitForReadable() {
	logger.LogInfo(fmt.Sprintf("Waiting for file to be readable -> %v [%p]", cf.readyToDownload, cf))
	if !cf.readyToDownload {
		logger.LogInfo("Locking on wait for readable")
		cf.readyMutex.Lock()
		defer cf.readyMutex.Unlock()
		cf.readyToDownload = true
		logger.LogInfo("File is now readable")
	} else {
		logger.LogInfo("Chunkfile is already ready")
	}
}

func (cf *ChunkFile) ReadyToClean() bool {
	if cf.tmpFile == nil {
		return true
	}
	lastAccess, err := cf.tmpFile.getLastAccessTime()
	if err != nil {
		logger.LogErr(fmt.Sprintf("Failed to read last access time: %s", err.Error()))
		return false
	}
	return time.Since(*lastAccess).Seconds() > float64(configs.FILE_TTL)
}

func (cf *ChunkFile) DeleteTmpFile() {
	if cf.tmpFile != nil {
		if err := os.Remove(cf.tmpFile.name); err != nil {
			logger.LogErr(fmt.Sprintf("Failed to delete temporary file %s: %s", cf.tmpFile.name, err.Error()))
		}
		cf.tmpFile = nil
		for idx := range cf.Chunks {
			ci := cf.Chunks[idx]
			ci.FileState = UPLOADED
		}
	}
}

func (cf *ChunkFile) HasBytes(start, end int64) bool {
	if cf.tmpFile == nil {
		return false
	}
	return cf.tmpFile.bytesAvailable >= end
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

	var ci []*ChunkItem
	var count int = 0
	for chunk := range slices.Chunk(*fileBytes, configs.CHUNK_SIZE) {
		ci = append(ci, &ChunkItem{
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

// StartDownload locks and starts the download of all the chunks, if needed
func (cf *ChunkFile) StartDownload() {
	if cf.isDownloading {
		return
	}
	if cf.tmpFile != nil && cf.tmpFile.bytesAvailable == int64(cf.OriginalSize) {
		return
	}

	cf.isDownloading = true

	for idx := range cf.Chunks {
		go func(item *ChunkItem) {
			if item.shouldBeDownloaded() {
				item.lock.Lock()
				logger.LogInfo(fmt.Sprintf("Locked chunk [%d] to be downloaded", item.Idx))
				go func() {
					defer item.lock.Unlock()
					if err := item.fetchBuffer(cf); err != nil {
						logger.LogErr(fmt.Sprintf("Failed to download chunk item [%d]: %s", item.Idx, err.Error()))
					}
				}()
			}
		}(cf.Chunks[idx])
	}

	cf.isDownloading = false
}

func (cf *ChunkFile) GetBytes(start, end int64) []byte {
	if cf.tmpFile == nil {
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
		chunk := cf.Chunks[idx]

		if end <= chunk.Start || start >= chunk.End {
			continue
		}

		relativeStart := max(0, start-chunk.Start)
		relativeEnd := min(chunk.End-chunk.Start, end-chunk.Start)

		chunkSize := int64(chunk.Size)
		if relativeStart >= chunkSize || relativeEnd > chunkSize {
			logger.LogErr(fmt.Sprintf("Invalid range [%d:%d] for chunk %d (buffer size %d)", relativeStart, relativeEnd, chunk.Idx, chunk.Size))
			continue
		}

		logger.LogInfo(fmt.Sprintf("Copying bytes from chunk %d [%d:%d]", idx, relativeStart, relativeEnd))
		readBuf := chunk.GetBytes(relativeStart, relativeEnd, cf)
		result = append(result, readBuf...)
		logger.LogInfo(fmt.Sprintf("Unlocked chunk [%d]", chunk.Idx))
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
		chunk := cf.Chunks[idx]
		if chunk.Buf == nil {
			return fmt.Errorf("buffer was nil for chunk %d", chunk.Idx)
		}
		_, _ = file.Write(chunk.Buf.Bytes())
	}

	logger.LogInfo(fmt.Sprintf("Wrote file: %s", outFile))
	return nil
}
