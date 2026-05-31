package manager

import (
	"encoding/json"
	"testing"

	"mimo2api/internal/models"
)

func TestLooksLikeShutdownConfirmation(t *testing.T) {
	if !looksLikeShutdownConfirmation("请确认是否继续关机") {
		t.Fatalf("expected confirmation prompt to be detected")
	}
	if looksLikeShutdownConfirmation("好的，马上执行关机。") {
		t.Fatalf("did not expect plain execution reply to be treated as confirmation")
	}
}

func TestExtractAssistantReplyFromPayload(t *testing.T) {
	payload := map[string]interface{}{
		"state": "final",
		"message": map[string]interface{}{
			"role": "assistant",
			"content": []interface{}{
				map[string]interface{}{"type": "text", "text": "确认关机"},
			},
		},
	}

	reply, final := extractAssistantReplyFromPayload(payload)
	if reply != "确认关机" {
		t.Fatalf("expected assistant reply, got %q", reply)
	}
	if !final {
		t.Fatalf("expected final state")
	}
}

func TestFirstEventPayloadFallsBackToParams(t *testing.T) {
	params, err := json.Marshal(map[string]interface{}{"state": "final"})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	payload := firstEventPayload(models.ClawWSMessage{Params: params})
	if asString(payload["state"]) != "final" {
		t.Fatalf("expected params payload to be parsed")
	}
}
