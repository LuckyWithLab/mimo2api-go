package state

import (
	"testing"

	"github.com/gorilla/websocket"
)

func resetClientStateForTest() {
	ActiveClients = make(map[*websocket.Conn]*TunnelClient)
	PendingQueues = make(map[string]chan map[string]interface{})
	ReqIDToWS = make(map[string]*websocket.Conn)
	WSToReqIDs = make(map[*websocket.Conn]map[string]bool)
	BridgeReady = make(map[*websocket.Conn]bool)
	CurrentClientIdx = 0
	ActiveList = nil
}

func TestGetNextClientNormalizesStaleIndex(t *testing.T) {
	resetClientStateForTest()

	now := int64(0)
	for i := 0; i < 5; i++ {
		ws := &websocket.Conn{}
		ActiveList = append(ActiveList, ws)
		ActiveClients[ws] = &TunnelClient{
			Conn:          ws,
			Host:          "node",
			CooldownUntil: now,
			BanUntil:      now,
		}
		BridgeReady[ws] = true
	}
	CurrentClientIdx = 9

	client := GetNextClient()
	if client == nil {
		t.Fatal("expected an available client")
	}
	if CurrentClientIdx != 0 {
		t.Fatalf("expected normalized next index to be 0, got %d", CurrentClientIdx)
	}
}

func TestUnregisterClientNormalizesCurrentClientIdx(t *testing.T) {
	resetClientStateForTest()

	ws1 := &websocket.Conn{}
	ws2 := &websocket.Conn{}
	ws3 := &websocket.Conn{}

	RegisterClient(ws1, "node-1")
	RegisterClient(ws2, "node-2")
	RegisterClient(ws3, "node-3")
	CurrentClientIdx = 2

	UnregisterClient(ws3)

	if got := len(ActiveList); got != 2 {
		t.Fatalf("expected 2 active clients after unregister, got %d", got)
	}
	if CurrentClientIdx != 0 {
		t.Fatalf("expected current client index to wrap to 0, got %d", CurrentClientIdx)
	}
}
