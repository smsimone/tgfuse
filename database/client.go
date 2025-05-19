package database

import (
	"context"
	"slices"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func getClient() (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"89.168.16.172:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func putKey(key, val string) error {
	cli, err := getClient()
	if err != nil {
		return err
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = cli.Put(ctx, key, val)
	if err != nil {
		return err
	}
	return nil
}

func delKey(key string) error {
	cli, err := getClient()
	if err != nil {
		return err
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = cli.Delete(ctx, key)
	if err != nil {
		return err
	}
	return nil
}

func getKey(key string) (string, error) {
	cli, err := getClient()
	if err != nil {
		return "", err
	}
	defer cli.Close()
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

func GetAllFileIds() (*[]string, error) {
	cli, err := getClient()
	if err != nil {
		return nil, err
	}
	defer cli.Close()
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
