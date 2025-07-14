package telegram

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"it.smaso/tgfuse/configs"
	"log"
	"net/http"
)

var instance *Telegram

type Telegram struct {
	sem chan int
}

func GetInstance() *Telegram {
	if instance == nil {
		instance = &Telegram{
			sem: make(chan int, 5),
		}
	}
	return instance
}

func getFilePath(fileId string) (*string, error) {
	type response struct {
		Result struct {
			FilePath string `json:"file_path"`
		} `json:"result"`
	}

	url := fmt.Sprintf("https://api.telegram.org/bot%s/getFile?file_id=%s", configs.TG_BOT_TOKEN, fileId)

	req, err := http.NewRequest("GET", url, &bytes.Buffer{})
	if err != nil {
		return nil, err
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	jResp := response{}
	if err := json.Unmarshal(respBody, &jResp); err != nil {
		return nil, err
	}

	return &jResp.Result.FilePath, nil
}

func (tg *Telegram) DownloadFile(fileId string) (*[]byte, error) {
	tg.sem <- 1
	defer func() { <-tg.sem }()

	filePath, err := getFilePath(fileId)
	if err != nil {
		log.Println("Failed to get file path", err)
		return nil, err
	}

	url := fmt.Sprintf("https://api.telegram.org/file/bot%s/%s", configs.TG_BOT_TOKEN, *filePath)

	req, err := http.NewRequest("GET", url, &bytes.Buffer{})
	if err != nil {
		log.Println("Failed to create request", err)
		return nil, err
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println("Failed to send request", err)
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	return &respBody, nil
}
