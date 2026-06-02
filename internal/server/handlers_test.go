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
