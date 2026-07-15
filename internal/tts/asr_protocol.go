package tts

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ASR model id (official Speech-Recognition docs).
const ModelASR = "mimo-v2.5-asr"

// IsASRModel reports whether model is Studio webchat ASR.
func IsASRModel(model string) bool {
	_, ok := NormalizeASRModel(model)
	return ok
}

// IsWebchatVoiceModel is true for design/clone TTS or ASR — Studio cookie path.
// Preset mimo-v2.5-tts stays on claw.
func IsWebchatVoiceModel(model string) bool {
	return IsWebchatTTSModel(model) || IsASRModel(model)
}

// NormalizeASRModel returns canonical ASR model id.
func NormalizeASRModel(model string) (string, bool) {
	m := strings.TrimSpace(model)
	if m == "" {
		return "", false
	}
	if strings.EqualFold(m, ModelASR) {
		return ModelASR, true
	}
	switch strings.ToLower(m) {
	case "mimo-v2.5-asr", "mimo-v2-asr", "mimo-asr":
		return ModelASR, true
	}
	return "", false
}

// ASRRequest is the normalized OpenAI-compatible ASR request
// (docs: Speech-Recognition / mimo-v2.5-asr).
type ASRRequest struct {
	Model      string
	Language   string // auto | zh | en
	AudioURL   string
	AudioBytes []byte
	FileName   string
	Stream     bool
}

// ParseASRRequest extracts audio + language from chat/completions body.
func ParseASRRequest(reqData map[string]interface{}) (*ASRRequest, error) {
	if reqData == nil {
		return nil, fmt.Errorf("empty request body")
	}
	rawModel, _ := reqData["model"].(string)
	model, ok := NormalizeASRModel(rawModel)
	if !ok {
		return nil, fmt.Errorf("unsupported asr model: %s", rawModel)
	}

	lang := "auto"
	if opts, ok := reqData["asr_options"].(map[string]interface{}); ok {
		if l, ok := opts["language"].(string); ok && strings.TrimSpace(l) != "" {
			lang = normalizeASRLang(l)
		}
	}
	if l, ok := reqData["language"].(string); ok && strings.TrimSpace(l) != "" {
		lang = normalizeASRLang(l)
	}

	stream := false
	if s, ok := reqData["stream"].(bool); ok {
		stream = s
	}

	audioURL, rawBytes, fileName, err := extractInputAudio(reqData["messages"])
	if err != nil {
		return nil, err
	}
	if audioURL == "" && len(rawBytes) == 0 {
		return nil, fmt.Errorf("input_audio required: pass data URL / base64 or http(s) audio url in messages")
	}

	return &ASRRequest{
		Model:      model,
		Language:   lang,
		AudioURL:   audioURL,
		AudioBytes: rawBytes,
		FileName:   fileName,
		Stream:     stream,
	}, nil
}

func normalizeASRLang(l string) string {
	l = strings.ToLower(strings.TrimSpace(l))
	switch l {
	case "zh", "zh-cn", "chinese", "cn":
		return "zh"
	case "en", "en-us", "english":
		return "en"
	case "auto", "":
		return "auto"
	default:
		return "auto"
	}
}

func extractInputAudio(messagesRaw interface{}) (audioURL string, data []byte, fileName string, err error) {
	messages, ok := messagesRaw.([]interface{})
	if !ok {
		return "", nil, "", fmt.Errorf("messages required")
	}
	for _, raw := range messages {
		m, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		content := m["content"]
		if parts, ok := content.([]interface{}); ok {
			for _, p := range parts {
				part, ok := p.(map[string]interface{})
				if !ok {
					continue
				}
				typ, _ := part["type"].(string)
				if typ == "input_audio" {
					ia, _ := part["input_audio"].(map[string]interface{})
					if ia == nil {
						continue
					}
					u, b, fn, e := parseInputAudioObject(ia)
					if e != nil {
						return "", nil, "", e
					}
					if u != "" || len(b) > 0 {
						return u, b, fn, nil
					}
				}
				if typ == "audio" {
					if u, _ := part["url"].(string); strings.HasPrefix(u, "http") {
						return u, nil, "", nil
					}
				}
			}
		}
		if s, ok := content.(string); ok {
			s = strings.TrimSpace(s)
			if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
				return s, nil, "", nil
			}
			if strings.HasPrefix(s, "data:") {
				mime, b, e := DecodeDataURL(s)
				if e != nil {
					return "", nil, "", e
				}
				return "", b, mimeToFileName(mime), nil
			}
		}
	}
	return "", nil, "", nil
}

func parseInputAudioObject(ia map[string]interface{}) (url string, data []byte, fileName string, err error) {
	raw, _ := ia["data"].(string)
	format, _ := ia["format"].(string)
	if raw == "" {
		if u, _ := ia["url"].(string); strings.HasPrefix(u, "http") {
			return u, nil, "", nil
		}
		return "", nil, "", fmt.Errorf("input_audio.data empty")
	}
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return raw, nil, "", nil
	}
	if strings.HasPrefix(raw, "data:") {
		mime, b, e := DecodeDataURL(raw)
		if e != nil {
			return "", nil, "", fmt.Errorf("invalid data url: %w", e)
		}
		return "", b, mimeToFileName(mime), nil
	}
	_, b, e := DecodeDataURL(raw)
	if e != nil {
		return "", nil, "", fmt.Errorf("invalid base64 audio: %w", e)
	}
	fileName = "audio.wav"
	switch strings.ToLower(format) {
	case "mp3", "mpeg", "audio/mpeg", "audio/mp3":
		fileName = "audio.mp3"
	case "wav", "audio/wav", "wave", "":
		fileName = "audio.wav"
	}
	return "", b, fileName, nil
}

func mimeToFileName(mime string) string {
	mime = strings.ToLower(mime)
	if strings.Contains(mime, "mpeg") || strings.Contains(mime, "mp3") {
		return "audio.mp3"
	}
	return "audio.wav"
}

// BuildASRCompletion builds non-stream chat completion with text transcript.
func BuildASRCompletion(model, text string) map[string]interface{} {
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
					"content": text,
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
