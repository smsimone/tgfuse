package telegram

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"

	"it.smaso/tgfuse/configs"
)

type sendResponse struct {
	Ok     bool `json:"ok"`
	Result struct {
		Document struct {
			FileId string `json:"file_id"`
		} `json:"document"`
	} `json:"result"`
}

func SendFile(ci Sendable) error {
	buf := ci.GetBuffer()
	if buf == nil {
		return fmt.Errorf("missing buffer to send")
	}

	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendDocument", configs.TG_BOT_TOKEN)

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	if err := writer.WriteField("chat_id", configs.TG_CHAT_ID); err != nil {
		return fmt.Errorf("failed to write chat_id: %s", err.Error())
	}

	if err := writer.WriteField("caption", "Part file"); err != nil {
		return fmt.Errorf("failed to write caption: %s", err.Error())
	}

	part, err := writer.CreateFormFile("document", ci.GetName())
	if err != nil {
		return fmt.Errorf("failed to create form file: %s", err.Error())
	}
	if _, err := io.Copy(part, buf); err != nil {
		return fmt.Errorf("failed to copy file buffer: %s", err.Error())
	}
	if writer.Close() != nil {
		return fmt.Errorf("failed to close writer")
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return fmt.Errorf("failed to create http request: %s", err.Error())
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	var jsonResp sendResponse
	if err := json.Unmarshal(respBody, &jsonResp); err != nil {
		return fmt.Errorf("failed to unmarshal response: %s", err.Error())
	}

	fmt.Println("response:", jsonResp.Result.Document.FileId)
	ci.FileId = &jsonResp.Result.Document.FileId

	return nil
}
