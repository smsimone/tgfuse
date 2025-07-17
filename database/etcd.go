package db

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"it.smaso/tgfuse/configs"
	"it.smaso/tgfuse/filesystem"
	"it.smaso/tgfuse/logger"
)

type etcdClient struct {
	DatabaseConnection
	configs configs.EtcdConfig
	client  *clientv3.Client
}

type KeyParam struct {
	Key      string
	GetValue func() string
	SetValue func(string)
}

type Keyed interface {
	GetKeyParams() []KeyParam
}

type KeyedChunkFile struct {
	Keyed
	chunkFile *filesystem.ChunkFile
}

type KeyedChunkItem struct {
	Keyed
	chunkItem *filesystem.ChunkItem
}

type SendKeyErr struct {
	Key string
	Err error
}

func (e *etcdClient) UploadFile(cf *filesystem.ChunkFile) error {
	kcf := KeyedChunkFile{chunkFile: cf}
	if err := e.SendFile(&kcf); err != nil {
		logger.LogErr(fmt.Sprintf("Failed to send ChunkFile to database: %s", err.Error()))
		return err
	}

	for idx := range cf.Chunks {
		chunk := cf.Chunks[idx]
		if chunk.FileId == nil {
			panic("Somehow the file id came null")
		}
		kci := KeyedChunkItem{chunkItem: chunk}
		if err := e.SendFile(&kci); err != nil {
			logger.LogErr(fmt.Sprintf("Failed to send ChunkItem to database: %s", err.Error()))
			return err
		}
	}

	return nil
}

func (e *etcdClient) GetAllChunkFiles() (*[]*filesystem.ChunkFile, error) {
	cfIds, err := e.GetAllFileIds()
	if err != nil {
		return nil, err
	}

	logger.LogInfo(fmt.Sprintf("Retrieved %d cfIds", len(*cfIds)))

	var chunkFiles []*filesystem.ChunkFile
	wg := sync.WaitGroup{}

	errs := make([]error, 0)
	for idx := range *cfIds {
		wg.Add(1)
		go func(cfID string) {
			defer wg.Done()
			cf := filesystem.NewChunkFile(filesystem.WithId(cfID))
			keyed := KeyedChunkFile{chunkFile: cf}

			if err := e.Restore(&keyed); err != nil {
				logger.LogErr(fmt.Sprintf("Failed to restore cf: %s", err.Error()))
				errs = append(errs, fmt.Errorf("failed to restore cf: %v", err))
				return
			}

			var curr int64 = 0

			for ciIdx := range cf.NumChunks {
				ci := filesystem.NewChunkItem(
					filesystem.WithIdx(ciIdx),
					filesystem.WithChunkFileId(cf.Id),
					filesystem.WithStart(curr),
				)

				kci := KeyedChunkItem{chunkItem: ci}
				if err := e.Restore(&kci); err != nil {
					logger.LogErr(fmt.Sprintf("Failed to restore cf %s", err.Error()))
				} else {
					ci.End = ci.Start + int64(ci.Size)
					curr += int64(ci.Size)
					if cf.HasBytes(ci.Start, ci.End) {
						ci.FileState = filesystem.FILE
					}
				}

				cf.Chunks = append(cf.Chunks, ci)
			}

			cf.Enable()

			chunkFiles = append(chunkFiles, cf)
		}((*cfIds)[idx])
	}
	wg.Wait()

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return &chunkFiles, nil
}

func (err SendKeyErr) Error() string { return fmt.Sprintf("%s: %s", err.Key, err.Err.Error()) }

func (e *etcdClient) getClient() (*clientv3.Client, error) {
	if e.client != nil {
		return e.client, nil
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{e.configs.URL},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		logger.LogErr("Failed to connect to etcd client")
		return nil, err
	}
	e.client = cli
	return cli, nil
}

func (e *etcdClient) putKey(key, val string) error {
	cli, err := e.getClient()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = cli.Put(ctx, key, val)
	if err != nil {
		return err
	}
	return nil
}

func (e *etcdClient) delKey(key string) error {
	cli, err := e.getClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = cli.Delete(ctx, key)
	if err != nil {
		return err
	}
	return nil
}

func (e *etcdClient) getKey(key string) (string, error) {
	cli, err := e.getClient()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := cli.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

func (e *etcdClient) GetAllFileIds() (*[]string, error) {
	cli, err := e.getClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := cli.Get(ctx, "/cf", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	keys := []string{}
	for _, item := range resp.Kvs {
		key := string(item.Key)
		cfId := strings.Split(key, "/")[2]
		if !slices.Contains(keys, cfId) {
			keys = append(keys, cfId)
		}
	}

	return &keys, nil
}

func (e *etcdClient) SendFile(obj Keyed) error {
	params := obj.GetKeyParams()
	for _, item := range params {
		if err := e.putKey(item.Key, item.GetValue()); err != nil {
			return SendKeyErr{Key: item.Key, Err: err}
		}
	}
	return nil
}

func (e *etcdClient) Restore(obj Keyed) error {
	params := obj.GetKeyParams()
	wg := sync.WaitGroup{}

	errors := []error{}
	for idx := range params {
		wg.Add(1)
		go func(item *KeyParam) {
			defer wg.Done()
			val, err := e.getKey(item.Key)
			if err != nil {
				errors = append(errors, SendKeyErr{Key: item.Key, Err: err})
			}
			item.SetValue(val)
		}(&params[idx])
	}
	wg.Wait()

	return nil
}

func (kcf *KeyedChunkFile) GetKeyParams() []KeyParam {
	cf := kcf.chunkFile
	return []KeyParam{
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

func (kci *KeyedChunkItem) GetKeyParams() []KeyParam {
	ci := kci.chunkItem
	return []KeyParam{
		{
			Key: fmt.Sprintf("/ci/%s/%d/size", ci.ChunkFileId, ci.Idx),
			GetValue: func() string {
				return strconv.Itoa(ci.Size)
			},
			SetValue: func(s string) {
				ci.Size, _ = strconv.Atoi(s)
			},
		},
		{
			Key: fmt.Sprintf("/ci/%s/%d/name", ci.ChunkFileId, ci.Idx),
			GetValue: func() string {
				return ci.Name
			},
			SetValue: func(s string) {
				ci.Name = s
			},
		},
		{
			Key: fmt.Sprintf("/ci/%s/%d/file_id", ci.ChunkFileId, ci.Idx),
			GetValue: func() string {
				return *ci.FileId
			},
			SetValue: func(s string) {
				ci.FileId = &s
			},
		},
	}
}
