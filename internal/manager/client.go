package manager

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"mimo2api/internal/config"
	"mimo2api/internal/models"
)

const (
	remoteShutdownPrompt        = "现在帮我立刻关机，直接执行 /sbin/shutdown -h now，不要询问我确认，也不要让我手动输入任何东西。\n"
	remoteShutdownConfirmPrompt = "确认关机。现在立刻执行关机，不要再次询问确认，不要输出解释。"
)

var (
	httpClientPool = make(map[string]*http.Client)
	wsDialerPool   = make(map[string]*websocket.Dialer)
	poolMu         sync.Mutex
	ipIndex        int
)

func getNextIP() string {
	poolMu.Lock()
	defer poolMu.Unlock()
	ip := config.AistudioConnectIPs[ipIndex]
	ipIndex = (ipIndex + 1) % len(config.AistudioConnectIPs)
	return ip
}

// Get custom HTTP client with forced DNS resolution
func getHTTPClient(resolvedIP string) *http.Client {
	poolMu.Lock()
	defer poolMu.Unlock()

	if client, ok := httpClientPool[resolvedIP]; ok {
		return client
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if addr == config.AistudioHost+":443" {
				addr = resolvedIP + ":443"
			}
			return (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext(ctx, network, addr)
		},
		TLSClientConfig: &tls.Config{
			ServerName:         config.AistudioHost,
			InsecureSkipVerify: true,
		},
		ForceAttemptHTTP2: true,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   20 * time.Second,
	}
	httpClientPool[resolvedIP] = client
	return client
}

// Get custom WebSocket dialer with forced DNS resolution
func getWSDialer(resolvedIP string) *websocket.Dialer {
	poolMu.Lock()
	defer poolMu.Unlock()

	if dialer, ok := wsDialerPool[resolvedIP]; ok {
		return dialer
	}

	dialer := &websocket.Dialer{
		HandshakeTimeout: 15 * time.Second,
		NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if addr == config.AistudioHost+":443" {
				addr = resolvedIP + ":443"
			}
			return (&net.Dialer{
				Timeout:   15 * time.Second,
				KeepAlive: 15 * time.Second,
			}).DialContext(ctx, network, addr)
		},
		TLSClientConfig: &tls.Config{
			ServerName:         config.AistudioHost,
			InsecureSkipVerify: true,
		},
	}
	wsDialerPool[resolvedIP] = dialer
	return dialer
}

type NativeClawClient struct {
	UserID       string
	ServiceToken string
	SessionKey   string
	PH           string
	Cookies      string
	conn         *websocket.Conn
	Connected    bool
	responses    map[string]models.ClawWSMessage
	events       []models.ClawWSMessage
	mu           sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewNativeClawClient(user models.UserRecord) *NativeClawClient {
	ctx, cancel := context.WithCancel(context.Background())
	cookies := fmt.Sprintf("userId=%s; serviceToken=%s; xiaomichatbot_ph=%s", user.UserID, user.ServiceToken, user.PH)
	sessionKey := user.SessionKey
	if sessionKey == "" {
		sessionKey = "agent:main:main"
	}
	return &NativeClawClient{
		UserID:       user.UserID,
		ServiceToken: user.ServiceToken,
		SessionKey:   sessionKey,
		PH:           user.PH,
		Cookies:      cookies,
		responses:    make(map[string]models.ClawWSMessage),
		ctx:          ctx,
		cancel:       cancel,
	}
}

func looksLikeShutdownConfirmation(reply string) bool {
	text := strings.ToLower(strings.TrimSpace(reply))
	if text == "" {
		return false
	}
	keywords := []string{
		"确认", "请确认", "确定", "是否继续", "是否确认", "are you sure",
		"confirm", "确认关机", "确定要", "do you want",
	}
	for _, keyword := range keywords {
		if strings.Contains(text, keyword) {
			return true
		}
	}
	return false
}

func asString(value interface{}) string {
	if s, ok := value.(string); ok {
		return s
	}
	return ""
}

func firstEventPayload(msg models.ClawWSMessage) map[string]interface{} {
	for _, raw := range []json.RawMessage{msg.Payload, msg.Params} {
		if len(raw) == 0 {
			continue
		}
		var payload map[string]interface{}
		if err := json.Unmarshal(raw, &payload); err == nil {
			return payload
		}
	}
	return nil
}

func extractAssistantReplyFromPayload(payload map[string]interface{}) (reply string, final bool) {
	if payload == nil {
		return "", false
	}
	if asString(payload["state"]) == "final" {
		final = true
	}
	message, _ := payload["message"].(map[string]interface{})
	if asString(message["role"]) != "assistant" {
		return "", final
	}
	content, _ := message["content"].([]interface{})
	for _, raw := range content {
		part, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		if asString(part["type"]) == "text" && asString(part["text"]) != "" {
			reply = asString(part["text"])
		}
	}
	return reply, final
}

func (c *NativeClawClient) doAistudioReq(method, path string, body []byte, timeout time.Duration, resolvedIP string) (*http.Response, error) {
	if resolvedIP == "" {
		resolvedIP = getNextIP()
	}
	client := getHTTPClient(resolvedIP)

	urlFull := fmt.Sprintf("%s%s", config.AistudioBaseURL, path)
	var reqBody io.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	}

	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, urlFull, reqBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Cookie", c.Cookies)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return client.Do(req)
}

func (c *NativeClawClient) getTicket(resolvedIP string) (string, error) {
	escapedPH := url.QueryEscape(c.PH)
	path := fmt.Sprintf("/open-apis/user/ws/ticket?xiaomichatbot_ph=%s", escapedPH)

	resp, err := c.doAistudioReq("GET", path, nil, 15*time.Second, resolvedIP)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "", err
	}

	if d, ok := data["data"].(map[string]interface{}); ok {
		if ticket, ok := d["ticket"].(string); ok {
			return ticket, nil
		}
	}
	return "", fmt.Errorf("ticket not found in response")
}

func (c *NativeClawClient) Connect() bool {
	var ticket string
	var err error
	var resolvedIP string

	for i := 0; i < len(config.AistudioConnectIPs); i++ {
		resolvedIP = getNextIP()
		ticket, err = c.getTicket(resolvedIP)
		if err == nil {
			break
		}
		managerLogf("getTicket failed on %s: %v", resolvedIP, err)
	}

	if ticket == "" {
		return false
	}

	dialer := getWSDialer(resolvedIP)
	url := fmt.Sprintf("%s?ticket=%s", config.AistudioWSURL, ticket)

	headers := http.Header{}
	headers.Add("Cookie", c.Cookies)
	headers.Add("Origin", config.AistudioBaseURL)

	conn, _, err := dialer.DialContext(c.ctx, url, headers)
	if err != nil {
		managerLogf("WS connect failed on %s: %v", resolvedIP, err)
		return false
	}

	c.conn = conn
	c.Connected = false

	go c.readLoop()

	// Wait for connection to be ready (hello-ok)
	for i := 0; i < 50; i++ {
		c.mu.Lock()
		ready := c.Connected
		c.mu.Unlock()
		if ready {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}

	return false
}

func (c *NativeClawClient) readLoop() {
	defer func() {
		c.conn.Close()
		c.mu.Lock()
		c.Connected = false
		c.mu.Unlock()
	}()

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			managerLogf("WS read error: %v", err)
			break
		}

		var msg models.ClawWSMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			continue
		}

		c.mu.Lock()
		if msg.Type == "event" && msg.Event == "connect.challenge" {
			params := map[string]interface{}{
				"minProtocol": 3,
				"maxProtocol": 3,
				"client": map[string]string{
					"id":       "cli",
					"version":  "mimo-claw-ui",
					"platform": "Linux x86_64",
					"mode":     "cli",
				},
				"role":      "operator",
				"scopes":    []string{"operator.admin"},
				"caps":      []string{"tool-events"},
				"userAgent": "Mozilla/5.0",
				"locale":    "zh-CN",
			}
			reqMsg := map[string]interface{}{
				"type":   "req",
				"id":     uuid.New().String(),
				"method": "connect",
				"params": params,
			}
			reqBytes, _ := json.Marshal(reqMsg)
			_ = c.conn.WriteMessage(websocket.TextMessage, reqBytes)
		} else if msg.Type == "res" {
			c.responses[msg.ID] = msg
			if msg.OK != nil && *msg.OK {
				var payload map[string]interface{}
				if err := json.Unmarshal(msg.Payload, &payload); err == nil && payload["type"] == "hello-ok" {
					c.Connected = true
				}
			}
		} else if msg.Type == "event" {
			c.events = append(c.events, msg)
		}
		c.mu.Unlock()
	}
}

func (c *NativeClawClient) sendChat(text string, deliver *bool, waitReply bool, timeout time.Duration) (string, string, error) {
	c.mu.Lock()
	c.events = nil // clear old events
	c.mu.Unlock()

	reqID := uuid.New().String()
	params := map[string]interface{}{
		"sessionKey":     c.SessionKey,
		"message":        text,
		"idempotencyKey": uuid.New().String(),
	}
	if deliver != nil {
		params["deliver"] = *deliver
	}
	payload := map[string]interface{}{
		"type":   "req",
		"id":     reqID,
		"method": "chat.send",
		"params": params,
	}

	reqBytes, _ := json.Marshal(payload)
	c.mu.Lock()
	err := c.conn.WriteMessage(websocket.TextMessage, reqBytes)
	c.mu.Unlock()

	if err != nil {
		return "", "", err
	}

	var replyID string
	for i := 0; i < 150; i++ { // 15 seconds max
		c.mu.Lock()
		resp, ok := c.responses[reqID]
		c.mu.Unlock()
		if ok {
			var respPayload map[string]interface{}
			_ = json.Unmarshal(resp.Payload, &respPayload)
			if resp.OK != nil && *resp.OK {
				if idVal, ok := respPayload["id"].(string); ok {
					replyID = idVal
				}
				break
			}
			return "", "", fmt.Errorf("chat.send failed: %v", string(resp.Payload))
		}
		time.Sleep(100 * time.Millisecond)
	}
	if replyID == "" && !waitReply {
		return "", "", fmt.Errorf("timeout waiting for chat.send response")
	}
	if !waitReply {
		return replyID, "", nil
	}
	if timeout <= 0 {
		timeout = 120 * time.Second
	}

	deadline := time.Now().Add(timeout)
	var reply string
	for time.Now().Before(deadline) {
		c.mu.Lock()
		events := append([]models.ClawWSMessage(nil), c.events...)
		c.mu.Unlock()

		for _, evt := range events {
			if evt.Type != "event" || evt.Event != "chat" {
				continue
			}
			text, final := extractAssistantReplyFromPayload(firstEventPayload(evt))
			if text != "" {
				reply = text
			}
			if final && reply != "" {
				c.mu.Lock()
				c.events = nil
				c.mu.Unlock()
				return replyID, reply, nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	c.mu.Lock()
	c.events = nil
	c.mu.Unlock()
	if reply != "" {
		return replyID, reply, nil
	}
	return replyID, "", fmt.Errorf("timeout waiting for final assistant reply")
}

func (c *NativeClawClient) SendChat(text string) (string, error) {
	replyID, _, err := c.sendChat(text, nil, false, 0)
	return replyID, err
}

func (c *NativeClawClient) SendChatAndWaitReply(text string, timeout time.Duration, deliver *bool) (string, error) {
	_, reply, err := c.sendChat(text, deliver, true, timeout)
	return reply, err
}

func (c *NativeClawClient) Close() {
	c.cancel()
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *NativeClawClient) UploadFile(filePath string) (map[string]interface{}, error) {
	fileName := filepath.Base(filePath)
	fileBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	hash := md5.Sum(fileBytes)
	fileMD5 := hex.EncodeToString(hash[:])

	resolvedIP := getNextIP() // Just pick one IP
	client := getHTTPClient(resolvedIP)

	escapedPH := url.QueryEscape(c.PH)
	urlInfo := fmt.Sprintf("%s/open-apis/resource/genUploadInfo?xiaomichatbot_ph=%s", config.AistudioBaseURL, escapedPH)
	bodyBytes, _ := json.Marshal(map[string]string{
		"fileName":       fileName,
		"fileContentMd5": fileMD5,
	})

	req, err := http.NewRequestWithContext(c.ctx, "POST", urlInfo, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Cookie", c.Cookies)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("genUploadInfo failed: %d", resp.StatusCode)
	}

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	uploadData, ok := data["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid upload info response")
	}

	uploadURL, ok := uploadData["uploadUrl"].(string)
	if !ok {
		return nil, fmt.Errorf("uploadUrl missing or invalid")
	}
	putReq, err := http.NewRequestWithContext(c.ctx, "PUT", uploadURL, bytes.NewBuffer(fileBytes))
	if err != nil {
		return nil, err
	}

	putReq.Header.Set("Accept", "*/*")
	putReq.Header.Set("Content-Type", "application/octet-stream")
	putReq.Header.Set("Origin", config.AistudioBaseURL)
	putReq.Header.Set("Referer", config.AistudioBaseURL+"/")
	putReq.Header.Set("content-md5", fileMD5)
	putReq.Header.Set("User-Agent", "Mozilla/5.0")

	putResp, err := client.Do(putReq)
	if err != nil {
		return nil, err
	}
	defer putResp.Body.Close()

	if putResp.StatusCode >= 400 {
		b, _ := io.ReadAll(putResp.Body)
		return nil, fmt.Errorf("upload failed: %d - %s", putResp.StatusCode, string(b))
	}

	objName, ok := uploadData["objectName"].(string)
	if !ok {
		objName = fileName
	}

	return map[string]interface{}{
		"name": filepath.Base(objName),
		"size": len(fileBytes),
		"url":  uploadData["resourceUrl"],
		"type": "file",
	}, nil
}

func (c *NativeClawClient) SendFileMessage(fileInfo map[string]interface{}, promptText string) (string, error) {
	payload := map[string]interface{}{
		"files":  []interface{}{fileInfo},
		"prompt": config.MimoFileMetadataPrompt,
		// raw: The above is a list of files uploaded by the user. Please download the files before answering the user's question.
	}
	payloadBytes, _ := json.Marshal(payload)
	msgText := fmt.Sprintf("<mimo-files>\n%s\n</mimo-files>\n%s", string(payloadBytes), promptText)
	deliver := false
	return c.SendChatAndWaitReply(msgText, 180*time.Second, &deliver)
}

func (c *NativeClawClient) GetInstanceStatus() (status string, remainSec int) {
	path := "/open-apis/user/mimo-claw/status"
	resp, err := c.doAistudioReq("GET", path, nil, 5*time.Second, "")
	if err != nil {
		managerLogf("GetInstanceStatus failed: %v", err)
		return "ERROR", 0
	}
	defer resp.Body.Close()

	if resp.StatusCode == 401 {
		return "EXPIRED(401)", 0
	}

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "ERROR", 0
	}

	if d, ok := data["data"].(map[string]interface{}); ok {
		if st, ok := d["status"].(string); ok {
			status = st
		}
		if expireTimeRaw, ok := d["expireTime"].(float64); ok {
			expireTime := int64(expireTimeRaw)
			remainSec = int(expireTime/1000 - time.Now().Unix())
			if remainSec < 0 {
				remainSec = 0
			}
		}
	}
	if status == "" {
		status = "UNKNOWN"
	}
	return status, remainSec
}

func (c *NativeClawClient) DestroyClaw() bool {
	escapedPH := url.QueryEscape(c.PH)
	path := fmt.Sprintf("/open-apis/user/mimo-claw/destroy?xiaomichatbot_ph=%s", escapedPH)
	resp, err := c.doAistudioReq("POST", path, nil, 30*time.Second, "")
	if err != nil {
		managerLogf("DestroyClaw error: %v", err)
		return false
	}
	defer resp.Body.Close()

	var data map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&data)
	if code, ok := data["code"].(float64); ok && code == 0 {
		time.Sleep(3 * time.Second)
		return true
	}
	return false
}

func (c *NativeClawClient) CreateAndWait() bool {
	escapedPH := url.QueryEscape(c.PH)

	agreePath := fmt.Sprintf("/open-apis/agreement/user/mimo-claw?xiaomichatbot_ph=%s", escapedPH)
	if resp, err := c.doAistudioReq("POST", agreePath, nil, 15*time.Second, ""); err == nil {
		resp.Body.Close()
	}

	createPath := fmt.Sprintf("/open-apis/user/mimo-claw/create?xiaomichatbot_ph=%s", escapedPH)
	resp, err := c.doAistudioReq("POST", createPath, nil, 20*time.Second, "")
	if err != nil {
		managerLogf("CreateClaw request failed: %v", err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return false
	}
	var data map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&data)
	if code, ok := data["code"].(float64); !ok || code != 0 {
		return false
	}

	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := c.GetInstanceStatus()
		if st == "AVAILABLE" {
			return true
		}
		if st == "DESTROYED" || st == "ERROR" {
			return false
		}
		time.Sleep(2 * time.Second)
	}
	return false
}

func (c *NativeClawClient) TryShutdownInstance(status string) {
	if status != "AVAILABLE" {
		return
	}
	if !c.Connected {
		if !c.Connect() {
			return
		}
	}
	defer c.Close()

	reply, err := c.SendChatAndWaitReply(remoteShutdownPrompt, 90*time.Second, nil)
	if err != nil {
		managerLogf("shutdown prompt failed for user %s: %v", c.UserID, err)
		time.Sleep(8 * time.Second)
		return
	}
	if looksLikeShutdownConfirmation(reply) {
		if _, err := c.SendChatAndWaitReply(remoteShutdownConfirmPrompt, 45*time.Second, nil); err != nil {
			managerLogf("shutdown confirm failed for user %s: %v", c.UserID, err)
		}
	}
	time.Sleep(8 * time.Second)
}
