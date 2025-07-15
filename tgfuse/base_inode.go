package tgfuse

import (
	"bytes"
	"context"
	"log"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"it.smaso/tgfuse/configs"
	"it.smaso/tgfuse/filesystem"
	"it.smaso/tgfuse/telegram"
)

type virtualInode struct {
	fs.Inode
	data         []byte
	name         string
	mode         uint32
	cf           *filesystem.ChunkFile
	currentChunk *filesystem.ChunkItem
	chunks       []*filesystem.ChunkItem
	fileSize     int64
}

var (
	_ = (fs.NodeWriter)((*virtualInode)(nil))
	_ = (fs.NodeGetattrer)((*virtualInode)(nil))
	_ = (fs.NodeReader)((*virtualInode)(nil))
	_ = (fs.NodeOpener)((*virtualInode)(nil))
	_ = (fs.NodeFlusher)((*virtualInode)(nil))
)

func (bi *virtualInode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= int64(len(bi.data)) {
		return fuse.ReadResultData(nil), 0
	}
	end := min(int(off)+len(dest), len(bi.data))
	return fuse.ReadResultData(bi.data[off:end]), 0
}

func (bi *virtualInode) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	if bi.data == nil {
		bi.data = make([]byte, 0, configs.CHUNK_SIZE*2)
		bi.currentChunk = &filesystem.ChunkItem{
			Idx:       0,
			Buf:       new(bytes.Buffer),
			Name:      uuid.NewString(),
			FileState: filesystem.MEMORY,
		}
		bi.chunks = append(bi.chunks, bi.currentChunk)
	}

	bytesWritten := uint32(0)
	remainingData := data
	currentOffset := off

	for len(remainingData) > 0 {
		spaceInCurrentChunk := configs.CHUNK_SIZE - bi.currentChunk.Buf.Len()
		// chunk pieno
		if spaceInCurrentChunk <= 0 {
			bi.currentChunk.Size = bi.currentChunk.Buf.Len()
			newChunkIdx := bi.currentChunk.Idx + 1
			retryCount := 0
			for {
				if retryCount > 3 {
					log.Panicf("Failed to upload chunk [%d] three times in a row\n", bi.currentChunk.Idx)
				}
				if err := bi.currentChunk.Send(); err != nil {
					if tooManyRequests, ok := err.(*telegram.TooManyRequestsError); ok {
						log.Printf("Blocked because of too many requests. Retrying in %d seconds", tooManyRequests.Timeout)
						time.Sleep(time.Duration(tooManyRequests.Timeout) * time.Second)
					} else {
						log.Printf("Failed to send chunk [%d] -> %s", bi.currentChunk.Idx, err.Error())
						time.Sleep(2 * time.Second)
					}
					retryCount++
				} else {
					break
				}
			}
			log.Printf("Modified status of chunk [%d] -> %s - %s\n", bi.currentChunk.Idx, bi.currentChunk.FileState, *bi.currentChunk.FileId)
			bi.currentChunk = &filesystem.ChunkItem{
				Idx:       newChunkIdx,
				Buf:       new(bytes.Buffer),
				Name:      uuid.NewString(),
				FileState: filesystem.MEMORY,
			}
			bi.chunks = append(bi.chunks, bi.currentChunk)
			spaceInCurrentChunk = configs.CHUNK_SIZE
		}

		// quanti dati posso copiare ancora nel chunk
		writeLen := min(len(remainingData), spaceInCurrentChunk)

		// copio i nuovi dati all'interno del chunk
		n, err := bi.currentChunk.Buf.Write(remainingData[:writeLen])
		if err != nil {
			log.Printf("Errore durante la scrittura nel buffer del chunk: %v", err)
			return bytesWritten, syscall.EIO // O un altro errore appropriato
		}

		bytesWritten += uint32(n)
		remainingData = remainingData[n:]
		currentOffset += int64(n)
		bi.fileSize += int64(n)
	}

	return bytesWritten, 0
}

func (bi *virtualInode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Size = uint64(len(bi.data))
	out.Mode = bi.mode
	return 0
}

func (bi *virtualInode) Open(ctx context.Context, openFlags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.Println("Opening data from", bi.name)
	if fuseFlags&(syscall.O_RDWR|syscall.O_WRONLY) != 0 {
		return nil, 0, syscall.EROFS
	}

	return bi, fuse.FOPEN_DIRECT_IO, 0
}

func (bi *virtualInode) Flush(ctx context.Context, f fs.FileHandle) syscall.Errno {

	  // Invio l'ultimo chunk che manca
	  retryCount :=0
			for {
				if retryCount > 3 {
					log.Panicf("Failed to upload chunk [%d] three times in a row\n", bi.currentChunk.Idx)
				}
				if err := bi.currentChunk.Send(); err != nil {
					if tooManyRequests, ok := err.(*telegram.TooManyRequestsError); ok {
						log.Printf("Blocked because of too many requests. Retrying in %d seconds", tooManyRequests.Timeout)
						time.Sleep(time.Duration(tooManyRequests.Timeout) * time.Second)
					} else {
						log.Printf("Failed to send chunk [%d] -> %s", bi.currentChunk.Idx, err.Error())
						time.Sleep(2 * time.Second)
					}
					retryCount++
				} else {
					break
				}
			}

	for idx := range bi.chunks {
		chunk := bi.chunks[idx]
		log.Printf("Chunk: [%d] State: [%s] Id: [%p]\n", chunk.Idx, chunk.FileState, chunk.FileId)
		bi.cf.Chunks = append(bi.cf.Chunks, *chunk)
	}
	bi.cf.NumChunks = len(bi.chunks)
	bi.cf.OriginalSize = int(bi.fileSize)

	log.Println("Flushing data from", bi.name)

	if err := bi.cf.UploadToDatabase(); err != nil {
		log.Println("Failed to upload to database", err)
		return syscall.EIO
	}

	return 0
}
