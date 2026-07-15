package tts

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// HandleChatCompletions serves OpenAI-compatible webchat voice (TTS design/clone + ASR).
// Caller must already have authenticated the request.
func HandleChatCompletions(c *gin.Context, reqData map[string]interface{}, bodyText []byte) {
	if reqData == nil {
		if err := json.Unmarshal(bodyText, &reqData); err != nil {
			writeOpenAIError(c, http.StatusBadRequest, "Invalid JSON in request body")
			return
		}
	}
	model, _ := reqData["model"].(string)
	if IsASRModel(model) {
		handleASR(c, reqData)
		return
	}
	handleTTS(c, reqData)
}

func handleTTS(c *gin.Context, reqData map[string]interface{}) {
	start := time.Now()
	reqID := uuid.New().String()

	treq, err := ParseRequest(reqData)
	if err != nil {
		writeOpenAIError(c, http.StatusBadRequest, err.Error())
		return
	}

	const maxAttempts = 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		user, err := GlobalPool.Pick()
		if err != nil {
			log.Printf("[tts] req=%s no account: %v", reqID, err)
			writeOpenAIError(c, http.StatusServiceUnavailable, "TTS Error: "+err.Error())
			return
		}
		client := NewClient(user)
		ctx := c.Request.Context()
		wav, audioURL, err := client.Synthesize(ctx, treq)
		if err != nil {
			lastErr = err
			if apiErr, ok := err.(*APIError); ok && apiErr.IsUnauthorized() {
				log.Printf("[tts] req=%s user=%s attempt=%d auth fail, cooldown: %v", reqID, user.UserID, attempt, err)
				GlobalPool.MarkCooldown(user.UserID)
				continue
			}
			if apiErr, ok := err.(*APIError); ok && (apiErr.Status == 429 || apiErr.Status == 503) {
				log.Printf("[tts] req=%s user=%s attempt=%d status=%d, rotate: %v", reqID, user.UserID, attempt, apiErr.Status, err)
				GlobalPool.MarkCooldown(user.UserID)
				continue
			}
			status := http.StatusBadGateway
			msg := err.Error()
			if apiErr, ok := err.(*APIError); ok {
				if apiErr.Status >= 400 && apiErr.Status < 600 {
					status = apiErr.Status
				}
				if apiErr.Message != "" {
					msg = apiErr.Message
				}
			}
			log.Printf("[tts] req=%s user=%s model=%s status=%d dur=%dms err=%v", reqID, user.UserID, treq.Model, status, time.Since(start).Milliseconds(), err)
			writeOpenAIError(c, status, "TTS Error: "+msg)
			return
		}

		log.Printf("[tts] req=%s user=%s model=%s stream=%v bytes=%d dur=%dms url_ok=%v",
			reqID, user.UserID, treq.Model, treq.Stream, len(wav), time.Since(start).Milliseconds(), audioURL != "")

		if treq.Stream || treq.Format == "pcm16" {
			pcm, sr, werr := WavToPCM16LE(wav)
			if werr != nil {
				if treq.Stream {
					writeOpenAIError(c, http.StatusBadGateway, "TTS Error: cannot stream non-PCM wav: "+werr.Error())
					return
				}
				writeNonStream(c, treq.Model, wav, "wav", 0, audioURL)
				return
			}
			if treq.Stream {
				writePCMStream(c, treq.Model, pcm)
				return
			}
			resp := BuildCompletion(treq.Model, EncodeAudioBase64(pcm), "pcm16", sr)
			c.JSON(http.StatusOK, resp)
			return
		}

		writeNonStream(c, treq.Model, wav, "wav", WavSampleRate(wav), audioURL)
		return
	}

	msg := "all accounts failed"
	if lastErr != nil {
		msg = lastErr.Error()
	}
	writeOpenAIError(c, http.StatusServiceUnavailable, "TTS Error: "+msg)
}

func writeNonStream(c *gin.Context, model string, wav []byte, format string, sr int, audioURL string) {
	resp := BuildCompletion(model, EncodeAudioBase64(wav), format, sr)
	if audioURL != "" {
		if choices, ok := resp["choices"].([]interface{}); ok && len(choices) > 0 {
			if ch, ok := choices[0].(map[string]interface{}); ok {
				if msg, ok := ch["message"].(map[string]interface{}); ok {
					if audio, ok := msg["audio"].(map[string]interface{}); ok {
						audio["url"] = audioURL
					}
				}
			}
		}
	}
	c.JSON(http.StatusOK, resp)
}

func writePCMStream(c *gin.Context, model string, pcm []byte) {
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("X-Accel-Buffering", "no")
	c.Status(http.StatusOK)
	flusher, _ := c.Writer.(http.Flusher)

	cid := "chatcmpl-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:24]
	created := time.Now().Unix()
	writeSSE(c.Writer, flusher, map[string]interface{}{
		"id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
		"choices": []interface{}{
			map[string]interface{}{"index": 0, "delta": map[string]interface{}{"role": "assistant"}, "finish_reason": nil},
		},
	})

	chunkBytes := 9600
	if chunkBytes%2 != 0 {
		chunkBytes++
	}
	for off := 0; off < len(pcm); off += chunkBytes {
		end := off + chunkBytes
		if end > len(pcm) {
			end = len(pcm)
		}
		b64 := base64.StdEncoding.EncodeToString(pcm[off:end])
		writeSSE(c.Writer, flusher, map[string]interface{}{
			"id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
			"choices": []interface{}{
				map[string]interface{}{
					"index": 0,
					"delta": map[string]interface{}{
						"audio": map[string]interface{}{"data": b64},
					},
					"finish_reason": nil,
				},
			},
		})
	}
	writeSSE(c.Writer, flusher, map[string]interface{}{
		"id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
		"choices": []interface{}{
			map[string]interface{}{"index": 0, "delta": map[string]interface{}{}, "finish_reason": "stop"},
		},
	})
	_, _ = io.WriteString(c.Writer, "data: [DONE]\n\n")
	if flusher != nil {
		flusher.Flush()
	}
}

func writeSSE(w io.Writer, flusher http.Flusher, obj map[string]interface{}) {
	b, _ := json.Marshal(obj)
	_, _ = w.Write([]byte("data: "))
	_, _ = w.Write(b)
	_, _ = w.Write([]byte("\n\n"))
	if flusher != nil {
		flusher.Flush()
	}
}

func writeOpenAIError(c *gin.Context, status int, msg string) {
	c.JSON(status, gin.H{
		"error": gin.H{
			"message": msg,
			"type":    "invalid_request_error",
			"param":   nil,
			"code":    "tts_error",
		},
	})
}

// StatusSnapshot for ops.
func StatusSnapshot() map[string]interface{} {
	return map[string]interface{}{
		"available_accounts": GlobalPool.AvailableCount(),
		// models routed via Studio webchat (not claw)
		"models":      []string{"mimo-v2.5-tts-voicedesign", "mimo-v2.5-tts-voiceclone", ModelASR},
		"route":       "webchat",
		"claw_models": []string{"mimo-v2.5-tts"},
	}
}

func handleASR(c *gin.Context, reqData map[string]interface{}) {
	start := time.Now()
	reqID := uuid.New().String()
	areq, err := ParseASRRequest(reqData)
	if err != nil {
		writeOpenAIError(c, http.StatusBadRequest, err.Error())
		return
	}
	const maxAttempts = 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		user, err := GlobalPool.Pick()
		if err != nil {
			log.Printf("[asr] req=%s no account: %v", reqID, err)
			writeOpenAIError(c, http.StatusServiceUnavailable, "ASR Error: "+err.Error())
			return
		}
		client := NewClient(user)
		text, err := client.Recognize(c.Request.Context(), areq)
		if err != nil {
			lastErr = err
			if apiErr, ok := err.(*APIError); ok && apiErr.IsUnauthorized() {
				log.Printf("[asr] req=%s user=%s attempt=%d auth fail: %v", reqID, user.UserID, attempt, err)
				GlobalPool.MarkCooldown(user.UserID)
				continue
			}
			if apiErr, ok := err.(*APIError); ok && (apiErr.Status == 429 || apiErr.Status == 503) {
				log.Printf("[asr] req=%s user=%s attempt=%d status=%d rotate: %v", reqID, user.UserID, attempt, apiErr.Status, err)
				GlobalPool.MarkCooldown(user.UserID)
				continue
			}
			status := http.StatusBadGateway
			msg := err.Error()
			if apiErr, ok := err.(*APIError); ok {
				if apiErr.Status >= 400 && apiErr.Status < 600 {
					status = apiErr.Status
				}
				if apiErr.Message != "" {
					msg = apiErr.Message
				}
			}
			log.Printf("[asr] req=%s user=%s status=%d dur=%dms err=%v", reqID, user.UserID, status, time.Since(start).Milliseconds(), err)
			writeOpenAIError(c, status, "ASR Error: "+msg)
			return
		}
		log.Printf("[asr] req=%s user=%s stream=%v text_len=%d dur=%dms", reqID, user.UserID, areq.Stream, len(text), time.Since(start).Milliseconds())
		if areq.Stream {
			writeTextStream(c, areq.Model, text)
			return
		}
		c.JSON(http.StatusOK, BuildASRCompletion(areq.Model, text))
		return
	}
	msg := "all accounts failed"
	if lastErr != nil {
		msg = lastErr.Error()
	}
	writeOpenAIError(c, http.StatusServiceUnavailable, "ASR Error: "+msg)
}

func writeTextStream(c *gin.Context, model, text string) {
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("X-Accel-Buffering", "no")
	c.Status(http.StatusOK)
	flusher, _ := c.Writer.(http.Flusher)
	cid := "chatcmpl-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:24]
	created := time.Now().Unix()
	writeSSE(c.Writer, flusher, map[string]interface{}{
		"id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
		"choices": []interface{}{
			map[string]interface{}{"index": 0, "delta": map[string]interface{}{"role": "assistant"}, "finish_reason": nil},
		},
	})
	// Studio ASR is async full text; fake stream as one content delta.
	writeSSE(c.Writer, flusher, map[string]interface{}{
		"id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
		"choices": []interface{}{
			map[string]interface{}{"index": 0, "delta": map[string]interface{}{"content": text}, "finish_reason": nil},
		},
	})
	writeSSE(c.Writer, flusher, map[string]interface{}{
		"id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
		"choices": []interface{}{
			map[string]interface{}{"index": 0, "delta": map[string]interface{}{}, "finish_reason": "stop"},
		},
	})
	_, _ = io.WriteString(c.Writer, "data: [DONE]\n\n")
	if flusher != nil {
		flusher.Flush()
	}
}
