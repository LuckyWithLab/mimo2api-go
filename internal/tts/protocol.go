// Package tts implements Studio-path MiMo-V2.5-TTS as an OpenAI-compatible
// chat/completions surface (speech-synthesis-v2.5 request/response shape).
package tts

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Official model IDs + short aliases used by aistudio frontend redirects.
var modelAliases = map[string]string{
	"mimo-v2.5-tts-vd": "mimo-v2.5-tts-voicedesign",
	"mimo-v2.5-tts-vc": "mimo-v2.5-tts-voiceclone",
}

var ttsModels = map[string]string{
	"mimo-v2.5-tts":             "tts",
	"mimo-v2.5-tts-voicedesign":  "tts-vd",
	"mimo-v2.5-tts-voiceclone":   "tts-vc",
}

// IsTTSModel reports whether model is a known MiMo TTS family model.
func IsTTSModel(model string) bool {
	_, ok := NormalizeModel(model)
	return ok
}

// IsWebchatTTSModel is true only for voice design/clone — these go through
// aistudio Studio cookie HTTP (webchat path). Preset `mimo-v2.5-tts` stays on claw.
func IsWebchatTTSModel(model string) bool {
	canon, ok := NormalizeModel(model)
	if !ok {
		return false
	}
	return canon == "mimo-v2.5-tts-voicedesign" || canon == "mimo-v2.5-tts-voiceclone"
}

// NormalizeModel maps aliases to canonical model IDs.
func NormalizeModel(model string) (canonical string, ok bool) {
	m := strings.TrimSpace(model)
	if m == "" {
		return "", false
	}
	if a, hit := modelAliases[m]; hit {
		m = a
	}
	if _, hit := ttsModels[m]; hit {
		return m, true
	}
	// case-insensitive
	lower := strings.ToLower(m)
	for id := range ttsModels {
		if strings.ToLower(id) == lower {
			return id, true
		}
	}
	if a, hit := modelAliases[lower]; hit {
		return a, true
	}
	return "", false
}

// PageType is the aistudio conversation type for the model.
func PageType(model string) string {
	if t, ok := ttsModels[model]; ok {
		return t
	}
	return "tts"
}

// Request is the normalized OpenAI-compatible TTS request.
type Request struct {
	Model  string
	Style  string // user content: style / director / voice design prompt
	Text   string // assistant content: speech text
	Voice  string // preset id, FDS url, or data:audio/...;base64,...
	Format string // wav | pcm16
	Stream bool
	Scene  string // optional Studio scene override
}

// ParseRequest normalizes a raw chat/completions JSON body for TTS.
func ParseRequest(reqData map[string]interface{}) (*Request, error) {
	if reqData == nil {
		return nil, fmt.Errorf("empty request body")
	}
	rawModel, _ := reqData["model"].(string)
	model, ok := NormalizeModel(rawModel)
	if !ok {
		return nil, fmt.Errorf("unsupported tts model: %s", rawModel)
	}

	style, text := extractStyleText(reqData["messages"])
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("assistant message (speech text) is required")
	}

	audioCfg, _ := reqData["audio"].(map[string]interface{})
	format := "wav"
	voice := ""
	if audioCfg != nil {
		if f, ok := audioCfg["format"].(string); ok && strings.TrimSpace(f) != "" {
			format = strings.ToLower(strings.TrimSpace(f))
		}
		if v, ok := audioCfg["voice"].(string); ok {
			voice = v
		}
	}
	stream := false
	if s, ok := reqData["stream"].(bool); ok {
		stream = s
	}
	if stream && format == "wav" {
		// official stream path uses pcm16
		format = "pcm16"
	}
	if !stream && format == "" {
		format = "wav"
	}

	scene := ""
	if sc, ok := reqData["scene"].(string); ok {
		scene = sc
	}

	if model == "mimo-v2.5-tts" && strings.TrimSpace(voice) == "" {
		voice = "冰糖"
	}
	if model == "mimo-v2.5-tts" && scene == "" && strings.TrimSpace(style) != "" {
		scene = "BRIEF_DESCRIPTION"
	}

	return &Request{
		Model:  model,
		Style:  style,
		Text:   text,
		Voice:  voice,
		Format: format,
		Stream: stream,
		Scene:  scene,
	}, nil
}

func extractStyleText(messagesRaw interface{}) (style, text string) {
	messages, ok := messagesRaw.([]interface{})
	if !ok {
		return "", ""
	}
	for _, raw := range messages {
		m, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		role, _ := m["role"].(string)
		content := contentToString(m["content"])
		switch role {
		case "user":
			style = content
		case "assistant":
			text = content
		}
	}
	if strings.TrimSpace(text) == "" {
		// fallback: last non-empty message as speech text
		for i := len(messages) - 1; i >= 0; i-- {
			m, ok := messages[i].(map[string]interface{})
			if !ok {
				continue
			}
			c := contentToString(m["content"])
			if strings.TrimSpace(c) != "" {
				text = c
				break
			}
		}
	}
	return style, text
}

func contentToString(content interface{}) string {
	switch v := content.(type) {
	case string:
		return v
	case []interface{}:
		var b strings.Builder
		for _, part := range v {
			if p, ok := part.(map[string]interface{}); ok {
				if t, _ := p["type"].(string); t == "text" {
					if s, _ := p["text"].(string); s != "" {
						b.WriteString(s)
					}
				}
			} else if s, ok := part.(string); ok {
				b.WriteString(s)
			}
		}
		return b.String()
	default:
		if content == nil {
			return ""
		}
		return fmt.Sprint(content)
	}
}

// BuildCompletion builds non-stream OpenAI-style chat completion with audio.data base64.
func BuildCompletion(model string, audioB64 string, format string, sampleRate int) map[string]interface{} {
	audioID := "audio_" + uuid.New().String()
	audioObj := map[string]interface{}{
		"id":   audioID,
		"data": audioB64,
	}
	if format != "" {
		audioObj["format"] = format
	}
	if sampleRate > 0 {
		audioObj["sample_rate"] = sampleRate
	}
	return map[string]interface{}{
		"id":      "chatcmpl-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:24],
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []interface{}{
			map[string]interface{}{
				"index":         0,
				"finish_reason": "stop",
				"message": map[string]interface{}{
					"role":    "assistant",
					"content": nil,
					"audio":   audioObj,
				},
			},
		},
		"usage": map[string]interface{}{
			"prompt_tokens":     0,
			"completion_tokens": 0,
			"total_tokens":      0,
		},
	}
}

// EncodeAudioBase64 encodes raw audio bytes.
func EncodeAudioBase64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

// DecodeDataURL extracts raw bytes from data:{mime};base64,... or pure base64.
func DecodeDataURL(s string) (mime string, data []byte, err error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", nil, fmt.Errorf("empty data")
	}
	if strings.HasPrefix(s, "data:") {
		// data:audio/wav;base64,XXXX
		comma := strings.IndexByte(s, ',')
		if comma < 0 {
			return "", nil, fmt.Errorf("invalid data url")
		}
		header := s[5:comma]
		payload := s[comma+1:]
		mime = header
		if i := strings.IndexByte(header, ';'); i >= 0 {
			mime = header[:i]
		}
		raw, err := base64.StdEncoding.DecodeString(payload)
		if err != nil {
			// try raw std with whitespace
			raw, err = base64.StdEncoding.DecodeString(strings.Map(func(r rune) rune {
				if r == '\n' || r == '\r' || r == ' ' {
					return -1
				}
				return r
			}, payload))
		}
		return mime, raw, err
	}
	raw, err := base64.StdEncoding.DecodeString(s)
	return "application/octet-stream", raw, err
}
