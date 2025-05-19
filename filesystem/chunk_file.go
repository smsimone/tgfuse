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
	"it.smaso/tgfuse/database"
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

	chunkFiles := []ChunkFile{}
	for _, cfId := range *cfIds {
		cf := ChunkFile{Id: cfId, Chunks: []ChunkItem{}}

		if err := database.Restore(&cf); err != nil {
			fmt.Println("Failed to restore cf", err)
			return nil, err
		}

		for ciIdx := range cf.NumChunks {
			fmt.Println("Restoring chunk id", ciIdx)
			ci := ChunkItem{Idx: ciIdx, chunkFileId: cfId}

			if err := database.Restore(&ci); err != nil {
				fmt.Println("Failed to restore cf", err)
				return nil, err
			}
			cf.Chunks = append(cf.Chunks, ci)
		}

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
		if err := database.SendFile(&chunk); err != nil {
			fmt.Printf("Failed to send ChunkItem to database: %s", err.Error())
			return err
		}
	}

	return nil
}
