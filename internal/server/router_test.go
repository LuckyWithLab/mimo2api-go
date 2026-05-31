package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"mimo2api/internal/config"
)

func TestRouterV1MessagesForwarding(t *testing.T) {
	// 备份并清空可能干扰测试的配置
	oldAPIKeys := config.APIKeys
	oldWSAuthToken := config.WSAuthToken
	config.APIKeys = nil
	config.WSAuthToken = ""
	defer func() {
		config.APIKeys = oldAPIKeys
		config.WSAuthToken = oldWSAuthToken
	}()

	r := SetupRouter()
	server := httptest.NewServer(r)
	defer server.Close()

	// 将 http 转换为 ws url
	wsURL := strings.Replace(server.URL, "http", "ws", 1) + "/ws"

	// 连接到 websocket 建立代理客户端
	dialer := websocket.Dialer{}
	wsConn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer wsConn.Close()

	pathChan := make(chan string, 1)

	// 异步监听 wsConn 收到的请求 payload
	go func() {
		for {
			var msg map[string]interface{}
			if err := wsConn.ReadJSON(&msg); err != nil {
				return
			}

			if msgType, ok := msg["type"].(string); ok && msgType == "req" {
				if path, ok := msg["path"].(string); ok {
					pathChan <- path
				}

				// 响应 finish，避免 http 请求悬空超时
				reqID, _ := msg["req_id"].(string)
				_ = wsConn.WriteJSON(map[string]interface{}{
					"type":   "finish",
					"req_id": reqID,
				})
			}
		}
	}()

	// 稍等让 websocket 完成在服务器那边的注册
	time.Sleep(100 * time.Millisecond)

	// 发起 HTTP POST 请求到 /v1/messages
	reqBody := map[string]interface{}{
		"model":  "mimo-v2.5",
		"stream": true,
		"messages": []map[string]string{
			{"role": "user", "content": "hello"},
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req, err := http.NewRequest("POST", server.URL+"/v1/messages", bytes.NewBuffer(bodyBytes))
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("failed to do HTTP request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	// 验证代理客户端接收到的 path
	select {
	case path := <-pathChan:
		expected := "/anthropic/v1/messages"
		if path != expected {
			t.Errorf("expected path to be forwarded to %q, got %q", expected, path)
		}
	case <-time.After(3 * time.Second):
		t.Error("timeout waiting for request to be forwarded via WS")
	}
}
