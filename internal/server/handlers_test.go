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
	"mimo2api/internal/state"
)

func TestBodyFingerprintIncludesLength(t *testing.T) {
	got := bodyFingerprint("hello")
	if got == "" {
		t.Fatal("expected fingerprint string")
	}
	if got[:4] != "len=" {
		t.Fatalf("expected length prefix, got %q", got)
	}
}

func TestDetectUnauthorizedResponseParsesOpenAIStyleBody(t *testing.T) {
	body := `{"error":{"message":"Invalid API Key","param":"Please provide valid API Key","code":"401","type":"invalid_key"}}`
	status, reason, ok := detectUnauthorizedResponse(http.StatusOK, body)
	if !ok {
		t.Fatal("expected unauthorized response to be detected")
	}
	if status != http.StatusUnauthorized {
		t.Fatalf("expected status 401, got %d", status)
	}
	if reason == "" {
		t.Fatal("expected non-empty reason")
	}
}

func TestDetectUnauthorizedResponseDoesNotTreatGenericErrorObjectAs401(t *testing.T) {
	body := `{"error":{"message":"upstream overloaded","type":"server_error"}}`
	status, reason, ok := detectUnauthorizedResponse(http.StatusOK, body)
	if ok {
		t.Fatalf("expected generic error object not to be treated as 401, got status=%d reason=%q", status, reason)
	}
}

func TestApplyModelMappingUsesFallbackWhenMappingFileIsEmpty(t *testing.T) {
	mappingMu.Lock()
	oldMapping := modelMappingCache
	modelMappingCache = map[string]string{}
	mappingMu.Unlock()
	t.Cleanup(func() {
		mappingMu.Lock()
		modelMappingCache = oldMapping
		mappingMu.Unlock()
	})

	body := []byte(`{"model":"gpt-5.5","messages":[{"role":"user","content":"hello"}],"stream":true}`)
	mappedBody := applyModelMapping(body)

	var got map[string]interface{}
	if err := json.Unmarshal(mappedBody, &got); err != nil {
		t.Fatalf("failed to unmarshal mapped body: %v", err)
	}
	if got["model"] != "mimo-v2.5-pro" {
		t.Fatalf("expected fallback mapping to mimo-v2.5-pro, got %v", got["model"])
	}
}

func TestChatCompletionsHandlerRetriesUnauthorizedNode(t *testing.T) {
	resetGatewayStateForTest()

	oldAPIKeys := config.APIKeys
	oldWSAuthToken := config.WSAuthToken
	oldNode401Cooldown := config.Node401Cooldown
	oldNodeResponseIdleTimeout := config.NodeResponseIdleTimeout
	config.APIKeys = nil
	config.WSAuthToken = ""
	config.Node401Cooldown = 60
	config.NodeResponseIdleTimeout = 30
	t.Cleanup(func() {
		config.APIKeys = oldAPIKeys
		config.WSAuthToken = oldWSAuthToken
		config.Node401Cooldown = oldNode401Cooldown
		config.NodeResponseIdleTimeout = oldNodeResponseIdleTimeout
		resetGatewayStateForTest()
	})

	r := SetupRouter()
	server := httptest.NewServer(r)
	defer server.Close()

	wsBaseURL := strings.Replace(server.URL, "http", "ws", 1) + "/ws"
	node1 := dialTestNode(t, wsBaseURL+"?node_label=node-1")
	defer node1.Close()
	node2 := dialTestNode(t, wsBaseURL+"?node_label=node-2")
	defer node2.Close()

	go serveNodeResponse(t, node1, `{"error":{"message":"Invalid API Key","param":"Please provide valid API Key","code":"401","type":"invalid_key"}}`)
	go serveNodeResponse(t, node2, `{"id":"ok","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)

	time.Sleep(100 * time.Millisecond)

	reqBody := map[string]interface{}{
		"model":  "mimo-v2.5",
		"stream": false,
		"messages": []map[string]string{
			{"role": "user", "content": "hello"},
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req, err := http.NewRequest("POST", server.URL+"/v1/chat/completions", bytes.NewBuffer(bodyBytes))
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

	rawResp, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 after retry, got %d body=%s", resp.StatusCode, string(rawResp))
	}
	if !strings.Contains(string(rawResp), `"id":"ok"`) {
		t.Fatalf("expected response from retry node, got %s", string(rawResp))
	}

	nodes := state.GetActiveNodes()
	foundCooldown := false
	for _, node := range nodes {
		if node.Host == "node-1" && node.CooldownUntil > 0 {
			foundCooldown = true
			break
		}
	}
	if !foundCooldown {
		t.Fatalf("expected node-1 to enter cooldown, got nodes=%+v", nodes)
	}
}

func resetGatewayStateForTest() {
	state.ActiveClients = make(map[*websocket.Conn]*state.TunnelClient)
	state.PendingQueues = make(map[string]chan map[string]interface{})
	state.ReqIDToWS = make(map[string]*websocket.Conn)
	state.WSToReqIDs = make(map[*websocket.Conn]map[string]bool)
	state.BridgeReady = make(map[*websocket.Conn]bool)
	state.ActiveList = nil
	state.CurrentClientIdx = 0
}

func dialTestNode(t *testing.T, wsURL string) *websocket.Conn {
	t.Helper()
	dialer := websocket.Dialer{}
	wsConn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial websocket %s: %v", wsURL, err)
	}
	return wsConn
}

func serveNodeResponse(t *testing.T, wsConn *websocket.Conn, body string) {
	t.Helper()
	for {
		var msg map[string]interface{}
		if err := wsConn.ReadJSON(&msg); err != nil {
			return
		}
		if msgType, _ := msg["type"].(string); msgType != "req" {
			continue
		}

		reqID, _ := msg["req_id"].(string)
		if err := wsConn.WriteJSON(map[string]interface{}{
			"type":   "start",
			"req_id": reqID,
			"status": http.StatusOK,
			"headers": map[string]string{
				"Content-Type": "application/json",
			},
		}); err != nil {
			return
		}
		if err := wsConn.WriteJSON(map[string]interface{}{
			"type":   "chunk",
			"req_id": reqID,
			"body":   body,
		}); err != nil {
			return
		}
		_ = wsConn.WriteJSON(map[string]interface{}{
			"type":   "finish",
			"req_id": reqID,
		})
		return
	}
}

func TestWriteErrorResponseFormats(t *testing.T) {
	resetGatewayStateForTest()

	oldAPIKeys := config.APIKeys
	config.APIKeys = nil
	t.Cleanup(func() {
		config.APIKeys = oldAPIKeys
		resetGatewayStateForTest()
	})

	r := SetupRouter()

	// Case 1: OpenAI endpoint
	req1, _ := http.NewRequest("POST", "/v1/chat/completions", bytes.NewBufferString(`{"model":"test"}`))
	req1.Header.Set("Content-Type", "application/json")
	w1 := httptest.NewRecorder()
	r.ServeHTTP(w1, req1)

	if w1.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", w1.Code)
	}

	var errResp1 map[string]interface{}
	if err := json.Unmarshal(w1.Body.Bytes(), &errResp1); err != nil {
		t.Fatalf("failed to unmarshal OpenAI error: %v", err)
	}
	errObj1, ok := errResp1["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected OpenAI error response structure, got %s", w1.Body.String())
	}
	if errObj1["message"] != "Gateway Error: 没有可用的内网节点" {
		t.Fatalf("unexpected error message: %v", errObj1["message"])
	}
	if errObj1["type"] != "gateway_error" {
		t.Fatalf("unexpected error type: %v", errObj1["type"])
	}

	// Case 2: Anthropic endpoint
	req2, _ := http.NewRequest("POST", "/anthropic/v1/messages", bytes.NewBufferString(`{"model":"test"}`))
	req2.Header.Set("Content-Type", "application/json")
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)

	if w2.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", w2.Code)
	}

	var errResp2 map[string]interface{}
	if err := json.Unmarshal(w2.Body.Bytes(), &errResp2); err != nil {
		t.Fatalf("failed to unmarshal Anthropic error: %v", err)
	}
	if errResp2["type"] != "error" {
		t.Fatalf("expected Anthropic error response structure, got %s", w2.Body.String())
	}
	errObj2, ok := errResp2["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected Anthropic error field, got %s", w2.Body.String())
	}
	if errObj2["message"] != "Gateway Error: 没有可用的内网节点" {
		t.Fatalf("unexpected error message: %v", errObj2["message"])
	}
	if errObj2["type"] != "api_error" {
		t.Fatalf("unexpected error type: %v", errObj2["type"])
	}
}

func TestChatCompletionsHandlerNodeErrorAndChannelClosed(t *testing.T) {
	resetGatewayStateForTest()

	oldAPIKeys := config.APIKeys
	oldWSAuthToken := config.WSAuthToken
	oldNodeResponseIdleTimeout := config.NodeResponseIdleTimeout
	config.APIKeys = nil
	config.WSAuthToken = ""
	config.NodeResponseIdleTimeout = 5
	t.Cleanup(func() {
		config.APIKeys = oldAPIKeys
		config.WSAuthToken = oldWSAuthToken
		config.NodeResponseIdleTimeout = oldNodeResponseIdleTimeout
		resetGatewayStateForTest()
	})

	r := SetupRouter()
	server := httptest.NewServer(r)
	defer server.Close()

	wsBaseURL := strings.Replace(server.URL, "http", "ws", 1) + "/ws"

	// Case 1: Node returns error message (non-streaming)
	t.Run("node_error_non_streaming", func(t *testing.T) {
		resetGatewayStateForTest()
		node := dialTestNode(t, wsBaseURL+"?node_label=node-err")
		defer node.Close()

		go func() {
			for {
				var msg map[string]interface{}
				if err := node.ReadJSON(&msg); err != nil {
					return
				}
				if msgType, _ := msg["type"].(string); msgType != "req" {
					continue
				}
				reqID, _ := msg["req_id"].(string)
				// Start message
				_ = node.WriteJSON(map[string]interface{}{
					"type":   "start",
					"req_id": reqID,
					"status": http.StatusOK,
					"headers": map[string]string{
						"Content-Type": "application/json",
					},
				})
				// Send error message
				_ = node.WriteJSON(map[string]interface{}{
					"type":   "error",
					"req_id": reqID,
					"body":   "Upstream node failure description",
				})
				return
			}
		}()

		time.Sleep(50 * time.Millisecond)

		reqBody := map[string]interface{}{
			"model":  "mimo-v2.5",
			"stream": false,
			"messages": []map[string]string{
				{"role": "user", "content": "hello"},
			},
		}
		bodyBytes, _ := json.Marshal(reqBody)
		resp, err := http.Post(server.URL+"/v1/chat/completions", "application/json", bytes.NewBuffer(bodyBytes))
		if err != nil {
			t.Fatalf("failed to post: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("expected status 502, got %d", resp.StatusCode)
		}

		var errResp map[string]interface{}
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		errObj, _ := errResp["error"].(map[string]interface{})
		if errObj["message"] != "Upstream node failure description" {
			t.Fatalf("expected error message from node, got %v", errObj["message"])
		}
	})

	// Case 2: Node channel closed unexpectedly (non-streaming)
	t.Run("node_channel_closed", func(t *testing.T) {
		resetGatewayStateForTest()
		node := dialTestNode(t, wsBaseURL+"?node_label=node-close")

		go func() {
			for {
				var msg map[string]interface{}
				if err := node.ReadJSON(&msg); err != nil {
					return
				}
				if msgType, _ := msg["type"].(string); msgType != "req" {
					continue
				}
				// Close WS connection immediately to simulate crash
				node.Close()
				return
			}
		}()

		time.Sleep(50 * time.Millisecond)

		reqBody := map[string]interface{}{
			"model":  "mimo-v2.5",
			"stream": false,
			"messages": []map[string]string{
				{"role": "user", "content": "hello"},
			},
		}
		bodyBytes, _ := json.Marshal(reqBody)
		resp, err := http.Post(server.URL+"/v1/chat/completions", "application/json", bytes.NewBuffer(bodyBytes))
		if err != nil {
			t.Fatalf("failed to post: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusGatewayTimeout && resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("expected error status code 502/504, got %d", resp.StatusCode)
		}
	})
}

func TestAPIAuthMiddlewareFormats(t *testing.T) {
	resetGatewayStateForTest()

	oldAPIKeys := config.APIKeys
	config.APIKeys = []string{"valid-key"}
	t.Cleanup(func() {
		config.APIKeys = oldAPIKeys
		resetGatewayStateForTest()
	})

	r := SetupRouter()

	// Case 1: OpenAI endpoint unauthorized
	req1, _ := http.NewRequest("POST", "/v1/chat/completions", bytes.NewBufferString(`{"model":"test"}`))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("Authorization", "Bearer invalid-key")
	w1 := httptest.NewRecorder()
	r.ServeHTTP(w1, req1)

	if w1.Code != http.StatusUnauthorized {
		t.Fatalf("expected status 401, got %d", w1.Code)
	}

	var errResp1 map[string]interface{}
	if err := json.Unmarshal(w1.Body.Bytes(), &errResp1); err != nil {
		t.Fatalf("failed to unmarshal OpenAI auth error: %v", err)
	}
	errObj1, ok := errResp1["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected OpenAI error response structure, got %s", w1.Body.String())
	}
	if errObj1["message"] != "Invalid API Key" {
		t.Fatalf("unexpected error message: %v", errObj1["message"])
	}
	if errObj1["type"] != "invalid_request_error" {
		t.Fatalf("unexpected error type: %v", errObj1["type"])
	}

	// Case 2: Anthropic endpoint unauthorized
	req2, _ := http.NewRequest("POST", "/anthropic/v1/messages", bytes.NewBufferString(`{"model":"test"}`))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("x-api-key", "invalid-key")
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)

	if w2.Code != http.StatusUnauthorized {
		t.Fatalf("expected status 401, got %d", w2.Code)
	}

	var errResp2 map[string]interface{}
	if err := json.Unmarshal(w2.Body.Bytes(), &errResp2); err != nil {
		t.Fatalf("failed to unmarshal Anthropic auth error: %v", err)
	}
	if errResp2["type"] != "error" {
		t.Fatalf("expected Anthropic error response structure, got %s", w2.Body.String())
	}
	errObj2, ok := errResp2["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected Anthropic error field, got %s", w2.Body.String())
	}
	if errObj2["message"] != "Invalid API Key" {
		t.Fatalf("unexpected error message: %v", errObj2["message"])
	}
	if errObj2["type"] != "authentication_error" {
		t.Fatalf("unexpected error type: %v", errObj2["type"])
	}
}
