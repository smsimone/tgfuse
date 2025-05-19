package telegram

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"it.smaso/tgfuse/configs"
)

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

func DownloadFile(fileId string) (*[]byte, error) {
	filePath, err := getFilePath(fileId)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("https://api.telegram.org/file/bot%s/%s", configs.TG_BOT_TOKEN, *filePath)

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

	return &respBody, nil
}
