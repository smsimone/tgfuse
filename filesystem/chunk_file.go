package filesystem

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"io"
	"it.smaso/tgfuse/configs"
	"it.smaso/tgfuse/database"
	"log"
	"os"
	"path"
	"slices"
	"sync"
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
		Id:               uuid.NewString(),
	}

	var ci []ChunkItem
	var count int = 0
	for chunk := range slices.Chunk(fileBytes, configs.CHUNK_SIZE) {
		ci = append(ci, ChunkItem{
			Idx:         count,
			Size:        len(chunk),
			Name:        uuid.NewString(),
			Buf:         bytes.NewBuffer(chunk),
			FileState:   MEMORY,
			FileId:      nil,
			chunkFileId: cf.Id,
		})
		count = count + 1
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
	for _, cfId := range *cfIds {
		cf := ChunkFile{Id: cfId, Chunks: []ChunkItem{}}

		if err := database.Restore(&cf); err != nil {
			fmt.Println("Failed to restore cf", err)
			return nil, err
		}

		wg := sync.WaitGroup{}

		var curr int64 = 0
		for ciIdx := range cf.NumChunks {
			wg.Add(1)
			go func() {
				defer wg.Done()

				ci := ChunkItem{Idx: ciIdx, chunkFileId: cfId, Start: curr}
				if err := database.Restore(&ci); err != nil {
					log.Println("Failed to restore cf", err)
				} else {
					ci.End = ci.Start + int64(ci.Size)
					curr += int64(ci.Size)
					cf.Chunks = append(cf.Chunks, ci)
				}
			}()
		}

		wg.Wait()

		chunkFiles = append(chunkFiles, cf)
	}

	return &chunkFiles, nil
}

func (cf *ChunkFile) UploadToDatabase() error {
	if err := database.SendFile(cf); err != nil {
		fmt.Printf("Failed to send ChunkFile to database: %s", err.Error())
		return err
	}

	for _, chunk := range cf.Chunks {
		if chunk.FileId == nil {
			fmt.Println("Somehow the file id came null")
			os.Exit(1)
		}
		if err := database.SendFile(&chunk); err != nil {
			fmt.Printf("Failed to send ChunkItem to database: %s", err.Error())
			return err
		}
	}

	return nil
}

func (cf *ChunkFile) GetBytes() []byte {
	var b []byte
	if cf.fullByte != nil {
		return *cf.fullByte
	}
	for _, chunk := range cf.Chunks {
		b = append(b, chunk.Buf.Bytes()...)
	}
	cf.fullByte = &b
	return b
}

func (cf *ChunkFile) WriteFile(outFile string) error {
	file, err := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Failed to open output file", err)
		return err
	}
	defer file.Close()

	for _, chunk := range cf.Chunks {
		if chunk.Buf == nil {
			return fmt.Errorf("buffer was nil for chunk %d", chunk.Idx)
		}
		file.Write(chunk.Buf.Bytes())
	}

	fmt.Println("Wrote file", outFile)
	return nil
}
