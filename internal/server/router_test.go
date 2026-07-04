package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"mimo2api/internal/config"
)

func TestRouterV1MessagesForwarding(t *testing.T) {
	resetGatewayStateForTest()
	// 备份并清空可能干扰测试的配置
	oldAPIKeys := config.APIKeys
	oldWSAuthToken := config.WSAuthToken
	config.APIKeys = nil
	config.WSAuthToken = ""
	defer func() {
		config.APIKeys = oldAPIKeys
		config.WSAuthToken = oldWSAuthToken
		resetGatewayStateForTest()
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

				reqID, _ := msg["req_id"].(string)
				writeRouterTestResponse(wsConn, reqID, `data: {"id":"ok","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":null}]}`+"\n\n")
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

func TestRouterMimo25ConvertsSystemRoleBeforeForwarding(t *testing.T) {
	resetGatewayStateForTest()
	oldAPIKeys := config.APIKeys
	oldWSAuthToken := config.WSAuthToken
	config.APIKeys = nil
	config.WSAuthToken = ""
	defer func() {
		config.APIKeys = oldAPIKeys
		config.WSAuthToken = oldWSAuthToken
		resetGatewayStateForTest()
	}()

	r := SetupRouter()
	server := httptest.NewServer(r)
	defer server.Close()

	wsURL := strings.Replace(server.URL, "http", "ws", 1) + "/ws"
	dialer := websocket.Dialer{}
	wsConn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer wsConn.Close()

	bodyChan := make(chan string, 1)

	go func() {
		for {
			var msg map[string]interface{}
			if err := wsConn.ReadJSON(&msg); err != nil {
				return
			}
			if msgType, ok := msg["type"].(string); ok && msgType == "req" {
				if body, ok := msg["body"].(string); ok {
					bodyChan <- body
				}
				reqID, _ := msg["req_id"].(string)
				writeRouterTestResponse(wsConn, reqID, `{"id":"chatcmpl_ok","object":"chat.completion","model":"mimo-v2.5","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)

	reqBody := map[string]interface{}{
		"model":  "mimo-v2.5",
		"stream": false,
		"messages": []map[string]string{
			{"role": "system", "content": "be concise"},
			{"role": "user", "content": "hello"},
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", server.URL+"/v1/chat/completions", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("failed to do HTTP request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	select {
	case body := <-bodyChan:
		var parsed map[string]interface{}
		if err := json.Unmarshal([]byte(body), &parsed); err != nil {
			t.Fatalf("failed to parse forwarded body: %v", err)
		}
		messages, ok := parsed["messages"].([]interface{})
		if !ok || len(messages) != 2 {
			t.Fatalf("expected two forwarded messages, got %T %v", parsed["messages"], parsed["messages"])
		}
		firstMsg := messages[0].(map[string]interface{})
		if firstMsg["role"] != "user" {
			t.Fatalf("expected first forwarded role to be user, got %q", firstMsg["role"])
		}
		if firstMsg["content"] != "be concise" {
			t.Fatalf("expected system content to be preserved, got %q", firstMsg["content"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for body")
	}
}

func TestRouterV1ResponsesBridgeRouting(t *testing.T) {
	resetGatewayStateForTest()
	oldAPIKeys := config.APIKeys
	oldWSAuthToken := config.WSAuthToken
	config.APIKeys = nil
	config.WSAuthToken = ""
	defer func() {
		config.APIKeys = oldAPIKeys
		config.WSAuthToken = oldWSAuthToken
		resetGatewayStateForTest()
	}()

	r := SetupRouter()
	server := httptest.NewServer(r)
	defer server.Close()

	wsURL := strings.Replace(server.URL, "http", "ws", 1) + "/ws"
	dialer := websocket.Dialer{}
	wsConn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer wsConn.Close()

	pathChan := make(chan string, 1)
	bodyChan := make(chan string, 1)

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
				if body, ok := msg["body"].(string); ok {
					bodyChan <- body
				}
				reqID, _ := msg["req_id"].(string)
				writeRouterTestResponse(wsConn, reqID, `{"id":"chatcmpl_ok","object":"chat.completion","model":"mimo-v2.5","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)

	reqBody := map[string]interface{}{
		"model":  "mimo-v2.5",
		"input":  "hello",
		"stream": false,
	}
	bodyBytes, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", server.URL+"/v1/responses", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("failed to do HTTP request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	if contentType := resp.Header.Get("Content-Type"); !strings.Contains(contentType, "application/json") {
		t.Fatalf("expected application/json response, got %q", contentType)
	}
	rawResp, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	if !strings.Contains(string(rawResp), `"object":"response"`) {
		t.Fatalf("expected bridged responses body, got %s", string(rawResp))
	}

	select {
	case path := <-pathChan:
		if path != "/v1/chat/completions" {
			t.Errorf("expected path /v1/chat/completions, got %q", path)
		}
	case <-time.After(3 * time.Second):
		t.Error("timeout waiting for request to be forwarded via WS")
	}

	select {
	case body := <-bodyChan:
		var parsed map[string]interface{}
		if err := json.Unmarshal([]byte(body), &parsed); err != nil {
			t.Fatalf("failed to parse forwarded body: %v", err)
		}
		if _, ok := parsed["input"]; ok {
			t.Error("did not expect input field in bridged chat completions request")
		}
		if parsed["stream"] != false {
			t.Errorf("expected stream=false to be preserved, got %v", parsed["stream"])
		}
		messages, ok := parsed["messages"].([]interface{})
		if !ok || len(messages) == 0 {
			t.Fatalf("expected messages field in bridged body, got %T", parsed["messages"])
		}
		foundUser := false
		for _, raw := range messages {
			msg, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			if msg["role"] == "user" && msg["content"] == "hello" {
				foundUser = true
				break
			}
		}
		if !foundUser {
			t.Errorf("expected bridged messages to include user input, got %v", messages)
		}
	case <-time.After(3 * time.Second):
		t.Error("timeout waiting for body")
	}
}

func TestRouterV1ResponsesCompactRouting(t *testing.T) {
	resetGatewayStateForTest()
	oldAPIKeys := config.APIKeys
	oldWSAuthToken := config.WSAuthToken
	config.APIKeys = nil
	config.WSAuthToken = ""
	defer func() {
		config.APIKeys = oldAPIKeys
		config.WSAuthToken = oldWSAuthToken
		resetGatewayStateForTest()
	}()

	r := SetupRouter()
	server := httptest.NewServer(r)
	defer server.Close()

	wsURL := strings.Replace(server.URL, "http", "ws", 1) + "/ws"
	dialer := websocket.Dialer{}
	wsConn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer wsConn.Close()

	pathChan := make(chan string, 1)
	bodyChan := make(chan string, 1)

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
				if body, ok := msg["body"].(string); ok {
					bodyChan <- body
				}
				reqID, _ := msg["req_id"].(string)
				writeRouterTestResponse(wsConn, reqID, `{"id":"ok","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)

	reqBody := map[string]interface{}{
		"model": "mimo-v2.5-pro",
		"input": "Hello world",
	}
	bodyBytes, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", server.URL+"/v1/responses/compact", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("failed to do HTTP request: %v", err)
	}
	defer resp.Body.Close()

	// 验证转发路径为 /v1/chat/completions
	select {
	case path := <-pathChan:
		if path != "/v1/chat/completions" {
			t.Errorf("expected path /v1/chat/completions, got %q", path)
		}
	case <-time.After(3 * time.Second):
		t.Error("timeout waiting for request to be forwarded via WS")
	}

	// 验证 body 中包含 messages 且首条为 system 消息（压缩提示词）
	select {
	case body := <-bodyChan:
		var parsed map[string]interface{}
		json.Unmarshal([]byte(body), &parsed)
		if _, ok := parsed["messages"]; !ok {
			t.Error("expected messages field in forwarded body")
		}
		messages, ok := parsed["messages"].([]interface{})
		if ok && len(messages) > 0 {
			firstMsg := messages[0].(map[string]interface{})
			if firstMsg["role"] != "system" {
				t.Errorf("expected first message to be system, got %q", firstMsg["role"])
			}
		}
	case <-time.After(3 * time.Second):
		t.Error("timeout waiting for body")
	}
}

func writeRouterTestResponse(wsConn *websocket.Conn, reqID, body string) {
	_ = wsConn.WriteJSON(map[string]interface{}{
		"type":   "start",
		"req_id": reqID,
		"status": http.StatusOK,
		"headers": map[string]string{
			"Content-Type": "application/json",
		},
	})
	_ = wsConn.WriteJSON(map[string]interface{}{
		"type":   "chunk",
		"req_id": reqID,
		"body":   body,
	})
	_ = wsConn.WriteJSON(map[string]interface{}{
		"type":   "finish",
		"req_id": reqID,
	})
}
