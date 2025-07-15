package db

import (
	"log"

	"it.smaso/tgfuse/configs"
	"it.smaso/tgfuse/filesystem"
)

var instance DatabaseConnection

type DatabaseConnection interface {
	GetAllChunkFiles() (*[]filesystem.ChunkFile, error)
	UploadFile(cf *filesystem.ChunkFile) error
}

func Connect(conf configs.DBConfig) DatabaseConnection {
	if instance == nil {
		log.Printf("Loading database configuration: %+v", conf)
		switch conf := conf.(type) {
		case *configs.EtcdConfig:
			instance = &etcdClient{configs: *conf}
		case *configs.MongoConfig:
			panic("Mongo not implemented")
		}
	}
	return instance
}
