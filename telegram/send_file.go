package telegram

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"

	"it.smaso/tgfuse/configs"
	"it.smaso/tgfuse/logger"
)

type TooManyRequestsError struct {
	Timeout int
}

func (t *TooManyRequestsError) Error() string {
	return fmt.Sprintf("Too many requests: retry after %d", t.Timeout)
}

type sendResponse struct {
	Ok          bool   `json:"ok"`
	ErrorCode   int    `json:"error_code"`
	Description string `json:"description"`
	Result      struct {
		Document struct {
			FileId string `json:"file_id"`
		} `json:"document"`
	} `json:"result"`
}

func SendFile(ci Sendable) (*string, error) {
	buf := ci.GetBuffer()
	if buf == nil || buf.Len() == 0 {
		return nil, fmt.Errorf("missing buffer to send")
	}

	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendDocument", configs.TG_BOT_TOKEN)

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	if err := writer.WriteField("chat_id", configs.TG_CHAT_ID); err != nil {
		return nil, fmt.Errorf("failed to write chat_id: %s", err.Error())
	}

	if err := writer.WriteField("caption", "Part file"); err != nil {
		return nil, fmt.Errorf("failed to write caption: %s", err.Error())
	}

	part, err := writer.CreateFormFile("document", ci.GetName())
	if err != nil {
		return nil, fmt.Errorf("failed to create form file: %s", err.Error())
	}
	if _, err := io.Copy(part, buf); err != nil {
		return nil, fmt.Errorf("failed to copy file buffer: %s", err.Error())
	}
	if writer.Close() != nil {
		return nil, fmt.Errorf("failed to close writer")
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %s", err.Error())
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	var jsonResp sendResponse
	if err := json.Unmarshal(respBody, &jsonResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %s", err.Error())
	}

	fileID := jsonResp.Result.Document.FileId
	if jsonResp.Ok {
		return &fileID, nil
	}

	logger.LogInfo(fmt.Sprintf("FileID: %s", fileID))

	if strings.Contains(jsonResp.Description, "too Many Requests") {
		comps := strings.Split(jsonResp.Description, " ")
		duration := comps[len(comps)-1]
		durationVal, err := strconv.Atoi(duration)
		if err != nil {
			logger.LogErr(fmt.Sprintf("Failed to convert %s to int", duration))
			return nil, &TooManyRequestsError{Timeout: 8}
		}
		return nil, &TooManyRequestsError{Timeout: durationVal}
	} else {
		return nil, fmt.Errorf("%s", jsonResp.Description)
	}
}
