package tts

import (
	"encoding/json"
	"testing"
)

func TestNormalizeModel(t *testing.T) {
	cases := map[string]string{
		"mimo-v2.5-tts":            "mimo-v2.5-tts",
		"mimo-v2.5-tts-voicedesign": "mimo-v2.5-tts-voicedesign",
		"mimo-v2.5-tts-voiceclone":  "mimo-v2.5-tts-voiceclone",
		"mimo-v2.5-tts-vd":          "mimo-v2.5-tts-voicedesign",
		"mimo-v2.5-tts-vc":          "mimo-v2.5-tts-voiceclone",
	}
	for in, want := range cases {
		got, ok := NormalizeModel(in)
		if !ok || got != want {
			t.Fatalf("NormalizeModel(%q)=%q,%v want %q,true", in, got, ok, want)
		}
	}
	if IsTTSModel("mimo-v2.5-pro") {
		t.Fatal("pro should not be tts")
	}
	if !IsTTSModel("mimo-v2.5-tts") {
		t.Fatal("expected tts model")
	}
	// preset stays claw; design/clone go webchat
	if IsWebchatTTSModel("mimo-v2.5-tts") {
		t.Fatal("preset tts must not use webchat path")
	}
	if !IsWebchatTTSModel("mimo-v2.5-tts-voicedesign") || !IsWebchatTTSModel("mimo-v2.5-tts-vc") {
		t.Fatal("design/clone should use webchat path")
	}
	if !IsASRModel("mimo-v2.5-asr") || !IsWebchatVoiceModel("mimo-v2.5-asr") {
		t.Fatal("asr should use webchat path")
	}
	if IsWebchatVoiceModel("mimo-v2.5-tts") {
		t.Fatal("preset tts must not use webchat voice path")
	}
}

func TestParseASRRequestOfficialShape(t *testing.T) {
	// data URL form from Speech-Recognition docs
	raw := `{
		"model":"mimo-v2.5-asr",
		"messages":[{
			"role":"user",
			"content":[{
				"type":"input_audio",
				"input_audio":{"data":"data:audio/wav;base64,UklGRg=="}
			}]
		}],
		"asr_options":{"language":"zh"}
	}`
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatal(err)
	}
	req, err := ParseASRRequest(m)
	if err != nil {
		t.Fatal(err)
	}
	if req.Model != ModelASR || req.Language != "zh" {
		t.Fatalf("unexpected: %+v", req)
	}
	if len(req.AudioBytes) == 0 {
		t.Fatal("expected decoded audio bytes")
	}
}

func TestParseASRRequestHTTPURL(t *testing.T) {
	m := map[string]interface{}{
		"model": "mimo-v2.5-asr",
		"messages": []interface{}{
			map[string]interface{}{"role": "user", "content": "https://example.com/a.wav"},
		},
		"language": "en",
	}
	req, err := ParseASRRequest(m)
	if err != nil {
		t.Fatal(err)
	}
	if req.AudioURL != "https://example.com/a.wav" || req.Language != "en" {
		t.Fatalf("unexpected: %+v", req)
	}
}

func TestParseASRRequestMissingAudio(t *testing.T) {
	m := map[string]interface{}{
		"model":    "mimo-v2.5-asr",
		"messages": []interface{}{map[string]interface{}{"role": "user", "content": "hello"}},
	}
	if _, err := ParseASRRequest(m); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseRequestOfficialShape(t *testing.T) {
	raw := `{
		"model":"mimo-v2.5-tts",
		"messages":[
			{"role":"user","content":"自然清晰"},
			{"role":"assistant","content":"你好"}
		],
		"audio":{"format":"wav","voice":"冰糖"}
	}`
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatal(err)
	}
	req, err := ParseRequest(m)
	if err != nil {
		t.Fatal(err)
	}
	if req.Model != "mimo-v2.5-tts" || req.Text != "你好" || req.Style != "自然清晰" {
		t.Fatalf("unexpected parse: %+v", req)
	}
	if req.Voice != "冰糖" || req.Format != "wav" {
		t.Fatalf("audio fields: %+v", req)
	}
	if req.Scene != "BRIEF_DESCRIPTION" {
		t.Fatalf("expected default scene, got %q", req.Scene)
	}
}

func TestParseRequestStreamForcesPcm(t *testing.T) {
	m := map[string]interface{}{
		"model": "mimo-v2.5-tts",
		"messages": []interface{}{
			map[string]interface{}{"role": "assistant", "content": "hi"},
		},
		"audio":  map[string]interface{}{"format": "wav", "voice": "Mia"},
		"stream": true,
	}
	req, err := ParseRequest(m)
	if err != nil {
		t.Fatal(err)
	}
	if !req.Stream || req.Format != "pcm16" {
		t.Fatalf("stream should use pcm16: %+v", req)
	}
}

func TestParseRequestMissingText(t *testing.T) {
	m := map[string]interface{}{
		"model":    "mimo-v2.5-tts",
		"messages": []interface{}{},
	}
	if _, err := ParseRequest(m); err == nil {
		t.Fatal("expected error")
	}
}

func TestWavToPCM16LE(t *testing.T) {
	// minimal PCM16 mono 1 sample wav
	wav := []byte{
		'R', 'I', 'F', 'F', 36, 0, 0, 0, 'W', 'A', 'V', 'E',
		'f', 'm', 't', ' ', 16, 0, 0, 0,
		1, 0, // PCM
		1, 0, // mono
		0xC0, 0x5D, 0, 0, // 24000
		0, 0, 0, 0, // byte rate
		2, 0, // block align
		16, 0, // bits
		'd', 'a', 't', 'a', 2, 0, 0, 0,
		0x00, 0x10,
	}
	pcm, sr, err := WavToPCM16LE(wav)
	if err != nil {
		t.Fatal(err)
	}
	if sr != 24000 || len(pcm) != 2 {
		t.Fatalf("sr=%d len=%d", sr, len(pcm))
	}
}
