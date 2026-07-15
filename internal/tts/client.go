package tts

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"mimo2api/internal/config"
	"mimo2api/internal/manager"
	"mimo2api/internal/models"
)

// APIError is a Studio business or transport error.
type APIError struct {
	Status  int
	Code    interface{}
	Message string
	Body    interface{}
}

func (e *APIError) Error() string {
	if e.Code != nil {
		return fmt.Sprintf("aistudio tts: status=%d code=%v msg=%s", e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("aistudio tts: status=%d msg=%s", e.Status, e.Message)
}

func (e *APIError) IsUnauthorized() bool {
	return e.Status == 401
}

// Client talks to aistudio open-apis with cookie + query ph.
type Client struct {
	User models.UserRecord
}

func NewClient(user models.UserRecord) *Client {
	return &Client{User: user}
}

func (c *Client) cookieHeader() string {
	// quote serviceToken/ph like browser cookies for tokens with +/=
	return fmt.Sprintf(`serviceToken="%s"; userId=%s; xiaomichatbot_ph="%s"`,
		c.User.ServiceToken, c.User.UserID, c.User.PH)
}

func (c *Client) do(ctx context.Context, method, path string, body interface{}, usePH bool) (int, map[string]interface{}, error) {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reqBody = bytes.NewReader(b)
	}
	full := config.AistudioBaseURL + path
	if usePH {
		sep := "?"
		if strings.Contains(path, "?") {
			sep = "&"
		}
		full += sep + "xiaomichatbot_ph=" + url.QueryEscape(c.User.PH)
	}

	// reuse manager HTTP client (proxy / connect IPs)
	resolvedIP := manager.NextAistudioIP()
	httpClient := manager.HTTPClientForAistudio(resolvedIP)
	// TTS poll/download can exceed default 20s; clone with longer timeout
	client := *httpClient
	client.Timeout = 120 * time.Second

	req, err := http.NewRequestWithContext(ctx, method, full, reqBody)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Cookie", c.cookieHeader())
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")
	req.Header.Set("Origin", config.AistudioBaseURL)
	req.Header.Set("Referer", config.AistudioBaseURL+"/")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))

	var data map[string]interface{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &data); err != nil {
			return resp.StatusCode, nil, fmt.Errorf("non-json response status=%d body=%s", resp.StatusCode, truncate(string(raw), 200))
		}
	}
	return resp.StatusCode, data, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func ensureBizOK(status int, body map[string]interface{}, what string) (map[string]interface{}, error) {
	if status == 401 {
		return nil, &APIError{Status: 401, Message: what + ": unauthorized", Body: body}
	}
	if status != 200 {
		msg := what
		if body != nil {
			if m, ok := body["msg"].(string); ok {
				msg = m
			} else if m, ok := body["message"].(string); ok {
				msg = m
			}
		}
		return nil, &APIError{Status: status, Message: msg, Body: body}
	}
	if body == nil {
		return nil, &APIError{Status: 502, Message: what + ": empty body"}
	}
	// business code
	if code, ok := body["code"]; ok {
		switch v := code.(type) {
		case float64:
			if v != 0 && v != 200 {
				msg, _ := body["msg"].(string)
				if msg == "" {
					msg, _ = body["message"].(string)
				}
				st := 400
				if v == 4001 {
					st = 400
				}
				return nil, &APIError{Status: st, Code: v, Message: msg, Body: body}
			}
		case int:
			if v != 0 && v != 200 {
				msg, _ := body["msg"].(string)
				return nil, &APIError{Status: 400, Code: v, Message: msg, Body: body}
			}
		}
	}
	return body, nil
}

func (c *Client) SaveConversation(ctx context.Context, conversationID, pageType, title string) error {
	status, body, err := c.do(ctx, "POST", "/open-apis/chat/conversation/save", map[string]interface{}{
		"conversationId": conversationID,
		"title":          title,
		"type":           pageType,
	}, true)
	if err != nil {
		return err
	}
	_, err = ensureBizOK(status, body, "conversation/save")
	return err
}

func (c *Client) GenUploadInfo(ctx context.Context, fileName, md5hex string) (uploadURL, resourceURL string, err error) {
	status, body, err := c.do(ctx, "POST", "/open-apis/resource/genUploadInfo", map[string]string{
		"fileName":       fileName,
		"fileContentMd5": md5hex,
	}, true)
	if err != nil {
		return "", "", err
	}
	data, err := ensureBizOK(status, body, "genUploadInfo")
	if err != nil {
		return "", "", err
	}
	d, _ := data["data"].(map[string]interface{})
	uploadURL, _ = d["uploadUrl"].(string)
	resourceURL, _ = d["resourceUrl"].(string)
	if uploadURL == "" || resourceURL == "" {
		return "", "", &APIError{Status: 502, Message: "genUploadInfo missing urls", Body: body}
	}
	return uploadURL, resourceURL, nil
}

func (c *Client) PutFDS(ctx context.Context, uploadURL string, content []byte, md5hex string) error {
	resolvedIP := manager.NextAistudioIP()
	httpClient := manager.HTTPClientForAistudio(resolvedIP)
	client := *httpClient
	client.Timeout = 120 * time.Second

	req, err := http.NewRequestWithContext(ctx, "PUT", uploadURL, bytes.NewReader(content))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Origin", config.AistudioBaseURL)
	req.Header.Set("Referer", config.AistudioBaseURL+"/")
	req.Header.Set("content-md5", md5hex)
	req.Header.Set("User-Agent", "Mozilla/5.0")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("fds put status=%d body=%s", resp.StatusCode, string(b))
	}
	return nil
}

func (c *Client) UploadBytes(ctx context.Context, content []byte, fileName string) (string, error) {
	sum := md5.Sum(content)
	md5hex := hex.EncodeToString(sum[:])
	uploadURL, resourceURL, err := c.GenUploadInfo(ctx, fileName, md5hex)
	if err != nil {
		return "", err
	}
	if err := c.PutFDS(ctx, uploadURL, content, md5hex); err != nil {
		return "", err
	}
	return resourceURL, nil
}

// Synthesize runs save → generate → poll and returns wav bytes + audioUrl.
func (c *Client) Synthesize(ctx context.Context, req *Request) (wav []byte, audioURL string, err error) {
	if req == nil {
		return nil, "", fmt.Errorf("nil request")
	}
	model := req.Model
	pageType := PageType(model)
	cid := strings.ReplaceAll(uuid.New().String(), "-", "")
	mid := strings.ReplaceAll(uuid.New().String(), "-", "")

	if err := c.SaveConversation(ctx, cid, pageType, "tts-proxy"); err != nil {
		return nil, "", err
	}

	voice := req.Voice
	if model == "mimo-v2.5-tts-voiceclone" {
		if voice == "" {
			return nil, "", &APIError{Status: 400, Message: "voiceclone requires audio.voice"}
		}
		if strings.HasPrefix(voice, "data:") || (strings.Contains(voice, "base64") && !strings.HasPrefix(voice, "http")) {
			mime, raw, derr := DecodeDataURL(voice)
			if derr != nil {
				return nil, "", &APIError{Status: 400, Message: "invalid voice data url: " + derr.Error()}
			}
			ext := "wav"
			if strings.Contains(mime, "mpeg") || strings.Contains(mime, "mp3") {
				ext = "mp3"
			}
			urlRes, uerr := c.UploadBytes(ctx, raw, "clone."+ext)
			if uerr != nil {
				return nil, "", uerr
			}
			voice = urlRes
		}
	}

	audio := map[string]interface{}{"format": "wav"}
	if model == "mimo-v2.5-tts" || model == "mimo-v2.5-tts-voiceclone" {
		audio["voice"] = voice
	}

	content := map[string]interface{}{
		"messages": []interface{}{
			map[string]interface{}{"role": "user", "content": req.Style},
			map[string]interface{}{"role": "assistant", "content": req.Text},
		},
		"audio": audio,
	}
	modelConfig := map[string]interface{}{"modelCode": model}
	if req.Scene != "" {
		modelConfig["scene"] = req.Scene
	}

	status, body, err := c.do(ctx, "POST", "/open-apis/tts/v2/generate", map[string]interface{}{
		"conversationId": cid,
		"msgId":          mid,
		"content":        content,
		"modelConfig":    modelConfig,
	}, true)
	if err != nil {
		return nil, "", err
	}
	data, err := ensureBizOK(status, body, "tts/generate")
	if err != nil {
		return nil, "", err
	}
	d, _ := data["data"].(map[string]interface{})
	taskID := d["taskId"]
	if taskID == nil {
		return nil, "", &APIError{Status: 502, Message: "missing taskId", Body: body}
	}

	deadline := time.Now().Add(90 * time.Second)
	var last map[string]interface{}
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		path := fmt.Sprintf("/open-apis/tts/generateStatus?taskId=%v", taskIdString(taskID))
		st, b, err := c.do(ctx, "GET", path, nil, true)
		if err != nil {
			return nil, "", err
		}
		okBody, err := ensureBizOK(st, b, "tts/status")
		if err != nil {
			return nil, "", err
		}
		last, _ = okBody["data"].(map[string]interface{})
		switch asString(last["status"]) {
		case "success":
			audioURL, _ = last["audioUrl"].(string)
			if audioURL == "" {
				return nil, "", &APIError{Status: 502, Message: "success without audioUrl", Body: last}
			}
			wav, err = downloadURL(ctx, audioURL)
			return wav, audioURL, err
		case "failed":
			return nil, "", &APIError{Status: 502, Message: "tts task failed", Body: last}
		}
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-time.After(800 * time.Millisecond):
		}
	}
	return nil, "", &APIError{Status: 504, Message: fmt.Sprintf("tts poll timeout taskId=%v last=%v", taskID, last)}
}


// Recognize runs save → (optional upload) → asr/recognize → poll text.
func (c *Client) Recognize(ctx context.Context, req *ASRRequest) (text string, err error) {
	if req == nil {
		return "", fmt.Errorf("nil asr request")
	}
	cid := strings.ReplaceAll(uuid.New().String(), "-", "")
	mid := strings.ReplaceAll(uuid.New().String(), "-", "")
	if err := c.SaveConversation(ctx, cid, "asr", "asr-proxy"); err != nil {
		return "", err
	}

	audioURL := req.AudioURL
	if audioURL == "" {
		if len(req.AudioBytes) == 0 {
			return "", &APIError{Status: 400, Message: "no audio url or bytes"}
		}
		fn := req.FileName
		if fn == "" {
			fn = "audio.wav"
		}
		// base64 limit 10MB per official docs; Studio upload constraint similar
		if len(req.AudioBytes) > 12<<20 {
			return "", &APIError{Status: 400, Message: "audio too large"}
		}
		u, uerr := c.UploadBytes(ctx, req.AudioBytes, fn)
		if uerr != nil {
			return "", uerr
		}
		audioURL = u
	}

	lang := req.Language
	if lang == "" {
		lang = "auto"
	}
	status, body, err := c.do(ctx, "POST", "/open-apis/asr/recognize", map[string]interface{}{
		"conversationId": cid,
		"msgId":          mid,
		"audioUrl":       audioURL,
		"language":       lang,
		// Studio requires modelConfig; bare audioUrl returns 500 "服务器内部错误".
		"modelConfig": map[string]interface{}{"modelCode": ModelASR},
	}, true)
	if err != nil {
		return "", err
	}
	data, err := ensureBizOK(status, body, "asr/recognize")
	if err != nil {
		return "", err
	}
	d, _ := data["data"].(map[string]interface{})
	taskID := d["taskId"]
	// some responses may already be success with text
	if asString(d["status"]) == "success" && asString(d["text"]) != "" {
		return asString(d["text"]), nil
	}
	if taskID == nil {
		if t := asString(d["text"]); t != "" {
			return t, nil
		}
		return "", &APIError{Status: 502, Message: "asr missing taskId", Body: body}
	}

	deadline := time.Now().Add(90 * time.Second)
	var last map[string]interface{}
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		path := fmt.Sprintf("/open-apis/asr/recognizeStatus?taskId=%v", taskIdString(taskID))
		st, b, err := c.do(ctx, "GET", path, nil, true)
		if err != nil {
			return "", err
		}
		okBody, err := ensureBizOK(st, b, "asr/status")
		if err != nil {
			return "", err
		}
		last, _ = okBody["data"].(map[string]interface{})
		// audit fail
		if audit, ok := last["audit"].(map[string]interface{}); ok {
			if out, ok := audit["outputAudit"].(map[string]interface{}); ok {
				if passed, ok := out["passed"].(bool); ok && !passed {
					return "", &APIError{Status: 400, Message: "asr audit failed", Body: last}
				}
			}
		}
		switch asString(last["status"]) {
		case "success":
			t := asString(last["text"])
			return t, nil
		case "failed":
			return "", &APIError{Status: 502, Message: "asr task failed", Body: last}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(800 * time.Millisecond):
		}
	}
	return "", &APIError{Status: 504, Message: fmt.Sprintf("asr poll timeout taskId=%v last=%v", taskID, last)}
}

func taskIdString(v interface{}) string {
	switch t := v.(type) {
	case float64:
		return fmt.Sprintf("%.0f", t)
	case json.Number:
		return t.String()
	default:
		return fmt.Sprint(v)
	}
}

func asString(v interface{}) string {
	s, _ := v.(string)
	return s
}

func downloadURL(ctx context.Context, u string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	// FDS is public signed URL — default client is fine
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("download audio status=%d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 50<<20))
}
