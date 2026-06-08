package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"mimo2api/internal/config"
)

type sessionPayload struct {
	Username  string `json:"u"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
}

func generateSessionToken() string {
	payload := sessionPayload{
		Username:  config.WebUIUsername,
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Unix() + int64(config.WebUISessionTTL),
	}
	rawPayload, _ := json.Marshal(payload)
	payloadEncoded := base64.RawURLEncoding.EncodeToString(rawPayload)
	h := hmac.New(sha256.New, []byte(config.WebUISecretKey))
	h.Write([]byte(payloadEncoded))
	return payloadEncoded + "." + hex.EncodeToString(h.Sum(nil))
}

func verifySessionToken(token string) bool {
	if token == "" {
		return false
	}

	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return false
	}

	payloadEncoded := parts[0]
	providedSig := parts[1]
	h := hmac.New(sha256.New, []byte(config.WebUISecretKey))
	h.Write([]byte(payloadEncoded))
	expectedSig := hex.EncodeToString(h.Sum(nil))
	if !hmac.Equal([]byte(providedSig), []byte(expectedSig)) {
		return false
	}

	rawPayload, err := base64.RawURLEncoding.DecodeString(payloadEncoded)
	if err != nil {
		return false
	}

	var payload sessionPayload
	if err := json.Unmarshal(rawPayload, &payload); err != nil {
		return false
	}

	return payload.Username == config.WebUIUsername && payload.ExpiresAt >= time.Now().Unix()
}

func WebUIMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if config.WebUIPassword == "" {
			c.Next()
			return
		}
		token, err := c.Cookie(config.WebUICookieName)
		if err != nil || !verifySessionToken(token) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
			return
		}
		c.Next()
	}
}

func APIAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !HasAnyAPIKey() {
			c.Next()
			return
		}

		authHeader := c.GetHeader("Authorization")
		token := ""
		if strings.HasPrefix(authHeader, "Bearer ") {
			token = authHeader[7:]
		} else if apiKey := c.GetHeader("x-api-key"); apiKey != "" {
			token = apiKey
		} else {
			token = c.GetHeader("api-key")
		}

		valid := ValidateAndRecordAPIKey(token)

		if !valid {
			path := c.Request.URL.Path
			if strings.HasPrefix(path, "/anthropic/") || path == "/v1/messages" {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
					"type": "error",
					"error": gin.H{
						"type":    "authentication_error",
						"message": "Invalid API Key",
					},
				})
				return
			}
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"message": "Invalid API Key",
					"type":    "invalid_request_error",
					"param":   nil,
					"code":    "invalid_api_key",
				},
			})
			return
		}
		c.Next()
	}
}

func LoginHandler(c *gin.Context) {
	if config.WebUIPassword == "" {
		c.JSON(http.StatusOK, gin.H{"ok": true, "enabled": false, "username": config.WebUIUsername})
		return
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request"})
		return
	}

	if req.Username == config.WebUIUsername && req.Password == config.WebUIPassword {
		token := generateSessionToken()
		c.SetCookie(config.WebUICookieName, token, config.WebUISessionTTL, "/", "", config.WebUICookieSecure, true)
		c.JSON(http.StatusOK, gin.H{"ok": true, "enabled": true, "username": config.WebUIUsername})
		return
	}

	c.JSON(http.StatusUnauthorized, gin.H{"detail": "Wrong username or password"})
}

func LogoutHandler(c *gin.Context) {
	c.SetCookie(config.WebUICookieName, "", -1, "/", "", config.WebUICookieSecure, true)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func SessionHandler(c *gin.Context) {
	authenticated := config.WebUIPassword == ""
	if !authenticated {
		token, err := c.Cookie(config.WebUICookieName)
		authenticated = err == nil && verifySessionToken(token)
	}

	c.JSON(http.StatusOK, gin.H{
		"enabled":         len(config.WebUIPassword) > 0,
		"authenticated":   authenticated,
		"username":        config.WebUIUsername,
		"ai_auth_enabled": HasAnyAPIKey(),
	})
}
