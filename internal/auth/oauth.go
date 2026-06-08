package auth

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"

	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/mattn/go-sqlite3"

	"mimo2api/internal/config"
)

const linuxDoProvider = "linux.do"

var ErrOAuthUserBanned = errors.New("oauth user is banned")

type OAuthKeyRecord struct {
	Provider       string `json:"provider"`
	ProviderUserID string `json:"provider_user_id"`
	Username       string `json:"username,omitempty"`
	Name           string `json:"name,omitempty"`
	Email          string `json:"email,omitempty"`
	AvatarURL      string `json:"avatar_url,omitempty"`
	APIKey         string `json:"api_key"`
	CreatedAt      int64  `json:"created_at"`
	UpdatedAt      int64  `json:"updated_at"`
	LastLoginAt    int64  `json:"last_login_at"`
	LastRequestAt  int64  `json:"last_request_at,omitempty"`
	RequestsTotal  int64  `json:"requests_total,omitempty"`
	RequestsToday  int64  `json:"requests_today,omitempty"`
	RequestDay     string `json:"request_day,omitempty"`
	BannedAt       int64  `json:"banned_at,omitempty"`
	BanReason      string `json:"ban_reason,omitempty"`
}

type OAuthUserUsage struct {
	Provider       string `json:"provider"`
	ProviderUserID string `json:"provider_user_id"`
	Username       string `json:"username,omitempty"`
	Name           string `json:"name,omitempty"`
	Email          string `json:"email,omitempty"`
	AvatarURL      string `json:"avatar_url,omitempty"`
	APIKeyPreview  string `json:"api_key_preview"`
	CreatedAt      int64  `json:"created_at"`
	UpdatedAt      int64  `json:"updated_at"`
	LastLoginAt    int64  `json:"last_login_at"`
	LastRequestAt  int64  `json:"last_request_at,omitempty"`
	RequestsTotal  int64  `json:"requests_total"`
	RequestsToday  int64  `json:"requests_today"`
	RequestDay     string `json:"request_day,omitempty"`
	BannedAt       int64  `json:"banned_at,omitempty"`
	BanReason      string `json:"ban_reason,omitempty"`
	Banned         bool   `json:"banned"`
}

type OAuthKeyStore struct {
	mu         sync.RWMutex
	db         *sql.DB
	dbPath     string
	legacyPath string
}

var oauthKeys = &OAuthKeyStore{}

var oauthHTTPClient = &http.Client{Timeout: 15 * time.Second}

func LoadOAuthKeyStore(dbPath, legacyJSONPath string) error {
	if legacyJSONPath == "" {
		legacyJSONPath = "oauth_keys.json"
	}
	return oauthKeys.Load(dbPath, legacyJSONPath)
}

func (s *OAuthKeyStore) Load(dbPath, legacyJSONPath string) error {
	if dbPath == "" {
		dbPath = "oauth_keys.db"
	}
	if dir := filepath.Dir(dbPath); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000")
	if err != nil {
		return err
	}
	if err := initOAuthSchema(db); err != nil {
		_ = db.Close()
		return err
	}

	s.mu.Lock()
	oldDB := s.db
	s.db = db
	s.dbPath = dbPath
	s.legacyPath = legacyJSONPath
	s.mu.Unlock()
	if oldDB != nil {
		_ = oldDB.Close()
	}

	return s.migrateLegacyJSON()
}

func initOAuthSchema(db *sql.DB) error {
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS oauth_keys (
		provider TEXT NOT NULL,
		provider_user_id TEXT NOT NULL,
		username TEXT,
		name TEXT,
		email TEXT,
		avatar_url TEXT,
		api_key TEXT NOT NULL UNIQUE,
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL,
		last_login_at INTEGER NOT NULL,
		last_request_at INTEGER DEFAULT 0,
		requests_total INTEGER DEFAULT 0,
		requests_today INTEGER DEFAULT 0,
		request_day TEXT,
		banned_at INTEGER DEFAULT 0,
		ban_reason TEXT DEFAULT '',
		PRIMARY KEY (provider, provider_user_id)
	);`
	createAPIKeyIndexSQL := `
	CREATE INDEX IF NOT EXISTS idx_oauth_keys_api_key
	ON oauth_keys (api_key);`
	createUsageIndexSQL := `
	CREATE INDEX IF NOT EXISTS idx_oauth_keys_usage
	ON oauth_keys (requests_today DESC, requests_total DESC, last_request_at DESC);`

	if _, err := db.Exec(createTableSQL); err != nil {
		return err
	}
	if _, err := db.Exec(createAPIKeyIndexSQL); err != nil {
		return err
	}
	if _, err := db.Exec(createUsageIndexSQL); err != nil {
		return err
	}
	if err := addOAuthColumnIfMissing(db, "banned_at", "INTEGER DEFAULT 0"); err != nil {
		return err
	}
	if err := addOAuthColumnIfMissing(db, "ban_reason", "TEXT DEFAULT ''"); err != nil {
		return err
	}
	return nil
}

func addOAuthColumnIfMissing(db *sql.DB, name, definition string) error {
	_, err := db.Exec(fmt.Sprintf("ALTER TABLE oauth_keys ADD COLUMN %s %s", name, definition))
	if err == nil {
		return nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
		return nil
	}
	return err
}

func (s *OAuthKeyStore) migrateLegacyJSON() error {
	if s.legacyPath == "" || s.legacyPath == s.dbPath {
		return nil
	}
	data, err := os.ReadFile(s.legacyPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil
	}

	var records []OAuthKeyRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return err
	}
	for _, record := range records {
		if record.Provider == "" {
			record.Provider = linuxDoProvider
		}
		if record.ProviderUserID == "" || record.APIKey == "" {
			continue
		}
		if err := s.insertLegacyRecord(record); err != nil {
			return err
		}
	}
	return nil
}

func (s *OAuthKeyStore) insertLegacyRecord(record OAuthKeyRecord) error {
	if s.db == nil {
		return fmt.Errorf("oauth key store is not initialized")
	}
	now := time.Now().Unix()
	if record.CreatedAt == 0 {
		record.CreatedAt = now
	}
	if record.UpdatedAt == 0 {
		record.UpdatedAt = now
	}
	if record.LastLoginAt == 0 {
		record.LastLoginAt = record.UpdatedAt
	}

	_, err := s.db.Exec(`
	INSERT INTO oauth_keys (
		provider, provider_user_id, username, name, email, avatar_url, api_key,
		created_at, updated_at, last_login_at, last_request_at,
		requests_total, requests_today, request_day
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(provider, provider_user_id) DO UPDATE SET
		username = excluded.username,
		name = excluded.name,
		email = excluded.email,
		avatar_url = excluded.avatar_url,
		updated_at = MAX(oauth_keys.updated_at, excluded.updated_at),
		last_login_at = MAX(oauth_keys.last_login_at, excluded.last_login_at),
		last_request_at = MAX(oauth_keys.last_request_at, excluded.last_request_at),
		requests_total = MAX(oauth_keys.requests_total, excluded.requests_total),
		requests_today = MAX(oauth_keys.requests_today, excluded.requests_today),
		request_day = CASE
			WHEN excluded.request_day != '' THEN excluded.request_day
			ELSE oauth_keys.request_day
		END
	`, record.Provider, record.ProviderUserID, record.Username, record.Name, record.Email, record.AvatarURL, record.APIKey,
		record.CreatedAt, record.UpdatedAt, record.LastLoginAt, record.LastRequestAt,
		record.RequestsTotal, record.RequestsToday, record.RequestDay)
	return err
}

func (s *OAuthKeyStore) HasKeys() bool {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()
	if db == nil {
		return false
	}
	var exists int
	err := db.QueryRow("SELECT 1 FROM oauth_keys LIMIT 1").Scan(&exists)
	return err == nil && exists == 1
}

func (s *OAuthKeyStore) IsValidAPIKey(apiKey string) bool {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()
	if apiKey == "" || db == nil {
		return false
	}
	var exists int
	err := db.QueryRow("SELECT 1 FROM oauth_keys WHERE api_key = ? AND COALESCE(banned_at, 0) = 0 LIMIT 1", apiKey).Scan(&exists)
	return err == nil && exists == 1
}

func (s *OAuthKeyStore) Upsert(record OAuthKeyRecord) (OAuthKeyRecord, bool, error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()
	if db == nil {
		return OAuthKeyRecord{}, false, fmt.Errorf("oauth key store is not initialized")
	}
	if record.Provider == "" {
		record.Provider = linuxDoProvider
	}
	if record.ProviderUserID == "" {
		return OAuthKeyRecord{}, false, fmt.Errorf("missing provider user id")
	}

	generatedAPIKey, err := generateAPIKey()
	if err != nil {
		return OAuthKeyRecord{}, false, err
	}
	record.APIKey = generatedAPIKey
	now := time.Now().Unix()
	record.CreatedAt = now
	record.UpdatedAt = now
	record.LastLoginAt = now

	err = db.QueryRow(`
	INSERT INTO oauth_keys (
		provider, provider_user_id, username, name, email, avatar_url, api_key,
		created_at, updated_at, last_login_at, last_request_at,
		requests_total, requests_today, request_day
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(provider, provider_user_id) DO UPDATE SET
		username = excluded.username,
		name = excluded.name,
		email = excluded.email,
		avatar_url = excluded.avatar_url,
		updated_at = excluded.updated_at,
		last_login_at = excluded.last_login_at
	RETURNING api_key, created_at, last_request_at, requests_total, requests_today, COALESCE(request_day, ''), COALESCE(banned_at, 0), COALESCE(ban_reason, '')
	`, record.Provider, record.ProviderUserID, record.Username, record.Name, record.Email, record.AvatarURL, record.APIKey,
		record.CreatedAt, record.UpdatedAt, record.LastLoginAt, record.LastRequestAt,
		record.RequestsTotal, record.RequestsToday, record.RequestDay).Scan(
		&record.APIKey, &record.CreatedAt, &record.LastRequestAt, &record.RequestsTotal, &record.RequestsToday, &record.RequestDay,
		&record.BannedAt, &record.BanReason)

	if err != nil {
		return OAuthKeyRecord{}, false, err
	}
	if record.BannedAt > 0 {
		return record, false, ErrOAuthUserBanned
	}

	created := record.APIKey == generatedAPIKey
	return record, created, nil
}

func (s *OAuthKeyStore) RotateAPIKey(provider, providerUserID string) (OAuthKeyRecord, error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()
	if db == nil {
		return OAuthKeyRecord{}, fmt.Errorf("oauth key store is not initialized")
	}
	if provider == "" {
		provider = linuxDoProvider
	}
	if providerUserID == "" {
		return OAuthKeyRecord{}, fmt.Errorf("missing provider user id")
	}

	apiKey, err := generateAPIKey()
	if err != nil {
		return OAuthKeyRecord{}, err
	}
	now := time.Now().Unix()
	result, err := db.Exec(`
	UPDATE oauth_keys
	SET api_key = ?, updated_at = ?
	WHERE provider = ? AND provider_user_id = ?
	`, apiKey, now, provider, providerUserID)
	if err != nil {
		return OAuthKeyRecord{}, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return OAuthKeyRecord{}, err
	}
	if affected == 0 {
		return OAuthKeyRecord{}, sql.ErrNoRows
	}

	return s.GetByProviderUser(provider, providerUserID)
}

func (s *OAuthKeyStore) BanUser(provider, providerUserID, reason string) (OAuthKeyRecord, error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()
	if db == nil {
		return OAuthKeyRecord{}, fmt.Errorf("oauth key store is not initialized")
	}
	if provider == "" {
		provider = linuxDoProvider
	}
	if providerUserID == "" {
		return OAuthKeyRecord{}, fmt.Errorf("missing provider user id")
	}

	apiKey, err := generateAPIKey()
	if err != nil {
		return OAuthKeyRecord{}, err
	}
	now := time.Now().Unix()
	result, err := db.Exec(`
	UPDATE oauth_keys
	SET api_key = ?, banned_at = ?, ban_reason = ?, updated_at = ?
	WHERE provider = ? AND provider_user_id = ?
	`, apiKey, now, strings.TrimSpace(reason), now, provider, providerUserID)
	if err != nil {
		return OAuthKeyRecord{}, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return OAuthKeyRecord{}, err
	}
	if affected == 0 {
		return OAuthKeyRecord{}, sql.ErrNoRows
	}
	return s.GetByProviderUser(provider, providerUserID)
}

func (s *OAuthKeyStore) UnbanUser(provider, providerUserID string) (OAuthKeyRecord, error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()
	if db == nil {
		return OAuthKeyRecord{}, fmt.Errorf("oauth key store is not initialized")
	}
	if provider == "" {
		provider = linuxDoProvider
	}
	if providerUserID == "" {
		return OAuthKeyRecord{}, fmt.Errorf("missing provider user id")
	}

	now := time.Now().Unix()
	result, err := db.Exec(`
	UPDATE oauth_keys
	SET banned_at = 0, ban_reason = '', updated_at = ?
	WHERE provider = ? AND provider_user_id = ?
	`, now, provider, providerUserID)
	if err != nil {
		return OAuthKeyRecord{}, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return OAuthKeyRecord{}, err
	}
	if affected == 0 {
		return OAuthKeyRecord{}, sql.ErrNoRows
	}
	return s.GetByProviderUser(provider, providerUserID)
}

func (s *OAuthKeyStore) GetByProviderUser(provider, providerUserID string) (OAuthKeyRecord, error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()
	if db == nil {
		return OAuthKeyRecord{}, fmt.Errorf("oauth key store is not initialized")
	}
	if provider == "" {
		provider = linuxDoProvider
	}

	var record OAuthKeyRecord
	err := db.QueryRow(`
	SELECT provider, provider_user_id, username, name, email, avatar_url, api_key,
		created_at, updated_at, last_login_at, last_request_at,
		requests_total, requests_today, COALESCE(request_day, ''), COALESCE(banned_at, 0), COALESCE(ban_reason, '')
	FROM oauth_keys
	WHERE provider = ? AND provider_user_id = ?
	`, provider, providerUserID).Scan(
		&record.Provider, &record.ProviderUserID, &record.Username, &record.Name,
		&record.Email, &record.AvatarURL, &record.APIKey, &record.CreatedAt,
		&record.UpdatedAt, &record.LastLoginAt, &record.LastRequestAt,
		&record.RequestsTotal, &record.RequestsToday, &record.RequestDay,
		&record.BannedAt, &record.BanReason,
	)
	if err != nil {
		return OAuthKeyRecord{}, err
	}
	return record, nil
}

func (s *OAuthKeyStore) RecordAPIKeyRequest(apiKey string) bool {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()
	if apiKey == "" || db == nil {
		return false
	}

	now := time.Now()
	today := now.Format("2006-01-02")
	result, err := db.Exec(`
	UPDATE oauth_keys
	SET
		request_day = ?,
		requests_today = CASE WHEN request_day = ? THEN requests_today + 1 ELSE 1 END,
		requests_total = requests_total + 1,
		last_request_at = ?,
		updated_at = ?
	WHERE api_key = ? AND COALESCE(banned_at, 0) = 0
	`, today, today, now.Unix(), now.Unix(), apiKey)
	if err != nil {
		return false
	}
	affected, err := result.RowsAffected()
	return err == nil && affected > 0
}

func (s *OAuthKeyStore) ListUsage() []OAuthUserUsage {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()
	if db == nil {
		return nil
	}

	today := time.Now().Format("2006-01-02")
	rows, err := db.Query(`
	SELECT provider, provider_user_id, username, name, email, avatar_url, api_key,
		created_at, updated_at, last_login_at, last_request_at,
		requests_total,
		CASE WHEN COALESCE(request_day, '') = ? THEN requests_today ELSE 0 END AS requests_today,
		COALESCE(request_day, ''), COALESCE(banned_at, 0), COALESCE(ban_reason, '')
	FROM oauth_keys
	`, today)
	if err != nil {
		return nil
	}
	defer rows.Close()

	usages := []OAuthUserUsage{}
	for rows.Next() {
		var record OAuthKeyRecord
		if err := rows.Scan(
			&record.Provider, &record.ProviderUserID, &record.Username, &record.Name,
			&record.Email, &record.AvatarURL, &record.APIKey, &record.CreatedAt,
			&record.UpdatedAt, &record.LastLoginAt, &record.LastRequestAt,
			&record.RequestsTotal, &record.RequestsToday, &record.RequestDay,
			&record.BannedAt, &record.BanReason,
		); err != nil {
			return usages
		}
		usages = append(usages, OAuthUserUsage{
			Provider:       record.Provider,
			ProviderUserID: record.ProviderUserID,
			Username:       record.Username,
			Name:           record.Name,
			Email:          record.Email,
			AvatarURL:      record.AvatarURL,
			APIKeyPreview:  previewAPIKey(record.APIKey),
			CreatedAt:      record.CreatedAt,
			UpdatedAt:      record.UpdatedAt,
			LastLoginAt:    record.LastLoginAt,
			LastRequestAt:  record.LastRequestAt,
			RequestsTotal:  record.RequestsTotal,
			RequestsToday:  record.RequestsToday,
			RequestDay:     record.RequestDay,
			BannedAt:       record.BannedAt,
			BanReason:      record.BanReason,
			Banned:         record.BannedAt > 0,
		})
	}
	sortOAuthUsage(usages)
	return usages
}

func sortOAuthUsage(usages []OAuthUserUsage) {
	sort.Slice(usages, func(i, j int) bool {
		if usages[i].RequestsToday != usages[j].RequestsToday {
			return usages[i].RequestsToday > usages[j].RequestsToday
		}
		if usages[i].RequestsTotal != usages[j].RequestsTotal {
			return usages[i].RequestsTotal > usages[j].RequestsTotal
		}
		return usages[i].LastRequestAt > usages[j].LastRequestAt
	})
}

func generateAPIKey() (string, error) {
	token, err := randomHex(32)
	if err != nil {
		return "", err
	}
	return "sk-mimo-" + token, nil
}

func generateOAuthState() (string, error) {
	return randomHex(24)
}

func previewAPIKey(apiKey string) string {
	if len(apiKey) <= 16 {
		return apiKey
	}
	return apiKey[:12] + "..." + apiKey[len(apiKey)-4:]
}

func randomHex(size int) (string, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func IsConfiguredAPIKey(token string) bool {
	for _, key := range config.APIKeys {
		if subtle.ConstantTimeCompare([]byte(token), []byte(key)) == 1 {
			return true
		}
	}
	return false
}

func HasAnyAPIKey() bool {
	return len(config.APIKeys) > 0 || oauthKeys.HasKeys() || oauthConfigReady()
}

func IsValidAPIKey(token string) bool {
	return IsConfiguredAPIKey(token) || oauthKeys.IsValidAPIKey(token)
}

func ValidateAndRecordAPIKey(token string) bool {
	if IsConfiguredAPIKey(token) {
		return true
	}
	return oauthKeys.RecordAPIKeyRequest(token)
}

func OAuthUsersHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"users": oauthKeys.ListUsage(),
	})
}

func OAuthRotateKeyHandler(c *gin.Context) {
	var req struct {
		Provider       string `json:"provider"`
		ProviderUserID string `json:"provider_user_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	record, err := oauthKeys.RotateAPIKey(req.Provider, req.ProviderUserID)
	if errors.Is(err, sql.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "oauth user not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to rotate api key", "detail": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"api_key": record.APIKey,
		"user": gin.H{
			"provider":         record.Provider,
			"provider_user_id": record.ProviderUserID,
			"username":         record.Username,
			"name":             record.Name,
			"email":            record.Email,
			"avatar_url":       record.AvatarURL,
		},
	})
}

func OAuthBanUserHandler(c *gin.Context) {
	var req struct {
		Provider       string `json:"provider"`
		ProviderUserID string `json:"provider_user_id"`
		Reason         string `json:"reason"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	record, err := oauthKeys.BanUser(req.Provider, req.ProviderUserID, req.Reason)
	if errors.Is(err, sql.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "oauth user not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to ban oauth user", "detail": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":               true,
		"provider":         record.Provider,
		"provider_user_id": record.ProviderUserID,
		"banned_at":        record.BannedAt,
		"ban_reason":       record.BanReason,
	})
}

func OAuthUnbanUserHandler(c *gin.Context) {
	var req struct {
		Provider       string `json:"provider"`
		ProviderUserID string `json:"provider_user_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	record, err := oauthKeys.UnbanUser(req.Provider, req.ProviderUserID)
	if errors.Is(err, sql.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "oauth user not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to unban oauth user", "detail": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":               true,
		"provider":         record.Provider,
		"provider_user_id": record.ProviderUserID,
		"banned_at":        record.BannedAt,
	})
}

func OAuthLoginHandler(c *gin.Context) {
	if !oauthConfigReady() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "OAuth is not configured"})
		return
	}

	state, err := generateOAuthState()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create oauth state"})
		return
	}

	redirectURI := oauthRedirectURI(c)
	authURL, err := url.Parse(config.OAuthAuthorizationURL)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "invalid authorization endpoint"})
		return
	}

	q := authURL.Query()
	q.Set("response_type", "code")
	q.Set("client_id", config.OAuthClientID)
	q.Set("redirect_uri", redirectURI)
	q.Set("state", state)
	if config.OAuthScopes != "" {
		q.Set("scope", config.OAuthScopes)
	}
	authURL.RawQuery = q.Encode()

	c.SetCookie(config.OAuthStateCookieName, state, 600, "/", "", config.WebUICookieSecure, true)
	c.Redirect(http.StatusFound, authURL.String())
}

func OAuthCallbackHandler(c *gin.Context) {
	if !oauthConfigReady() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "OAuth is not configured"})
		return
	}
	if detail := c.Query("error"); detail != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": detail, "error_description": c.Query("error_description")})
		return
	}

	code := strings.TrimSpace(c.Query("code"))
	state := strings.TrimSpace(c.Query("state"))
	if code == "" || state == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing oauth code or state"})
		return
	}
	expectedState, err := c.Cookie(config.OAuthStateCookieName)
	if err != nil || expectedState == "" || subtle.ConstantTimeCompare([]byte(state), []byte(expectedState)) != 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid oauth state"})
		return
	}
	c.SetCookie(config.OAuthStateCookieName, "", -1, "/", "", config.WebUICookieSecure, true)

	accessToken, err := exchangeOAuthCode(c.Request.Context(), code, oauthRedirectURI(c))
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "token exchange failed", "detail": err.Error()})
		return
	}

	user, err := fetchOAuthUser(c.Request.Context(), accessToken)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "failed to fetch oauth user", "detail": err.Error()})
		return
	}

	record, _, err := oauthKeys.Upsert(OAuthKeyRecord{
		Provider:       linuxDoProvider,
		ProviderUserID: user.ID,
		Username:       user.Username,
		Name:           user.Name,
		Email:          user.Email,
		AvatarURL:      user.AvatarURL,
	})
	if errors.Is(err, ErrOAuthUserBanned) {
		c.SetCookie("mimo_user_key", "", -1, "/", "", config.WebUICookieSecure, true)
		c.JSON(http.StatusForbidden, gin.H{"error": "oauth user is banned"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save api key", "detail": err.Error()})
		return
	}

	c.SetCookie("mimo_user_key", record.APIKey, 365*86400, "/", "", config.WebUICookieSecure, true)
	c.Redirect(http.StatusTemporaryRedirect, "/?mode=user")
}

func oauthConfigReady() bool {
	return config.OAuthClientID != "" && config.OAuthClientSecret != ""
}

func oauthRedirectURI(c *gin.Context) string {
	if config.OAuthRedirectURI != "" {
		return config.OAuthRedirectURI
	}

	scheme := c.GetHeader("X-Forwarded-Proto")
	if scheme == "" {
		if c.Request.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	host := c.GetHeader("X-Forwarded-Host")
	if host == "" {
		host = c.Request.Host
	}
	return scheme + "://" + host + "/api/oauth/callback"
}

type oauthTokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	Error       string `json:"error"`
	Description string `json:"error_description"`
}

func exchangeOAuthCode(ctx context.Context, code string, redirectURI string) (string, error) {
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", redirectURI)
	form.Set("client_id", config.OAuthClientID)
	form.Set("client_secret", config.OAuthClientSecret)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, config.OAuthTokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := oauthHTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("oauth token endpoint returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var tokenResp oauthTokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", err
	}
	if tokenResp.Error != "" {
		if tokenResp.Description != "" {
			return "", fmt.Errorf("%s: %s", tokenResp.Error, tokenResp.Description)
		}
		return "", fmt.Errorf("%s", tokenResp.Error)
	}
	if tokenResp.AccessToken == "" {
		return "", fmt.Errorf("missing access_token in token response")
	}
	return tokenResp.AccessToken, nil
}

type oauthUser struct {
	ID        string
	Username  string
	Name      string
	Email     string
	AvatarURL string
}

func fetchOAuthUser(ctx context.Context, accessToken string) (oauthUser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, config.OAuthUserURL, nil)
	if err != nil {
		return oauthUser{}, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := oauthHTTPClient.Do(req)
	if err != nil {
		return oauthUser{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return oauthUser{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return oauthUser{}, fmt.Errorf("oauth user endpoint returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var raw map[string]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		return oauthUser{}, err
	}

	user := oauthUser{
		ID:        firstString(raw, "id", "user_id", "sub"),
		Username:  firstString(raw, "username", "login", "name"),
		Name:      firstString(raw, "name", "display_name"),
		Email:     firstString(raw, "email"),
		AvatarURL: firstString(raw, "avatar_url", "avatar", "picture"),
	}
	if user.ID == "" {
		return oauthUser{}, fmt.Errorf("missing stable user id in user response")
	}
	return user, nil
}

func firstString(raw map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := raw[key]; ok {
			if text := stringify(value); text != "" {
				return text
			}
		}
	}
	return ""
}

func stringify(value interface{}) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case float64:
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case json.Number:
		return v.String()
	default:
		return ""
	}
}

// GetByAPIKey 依据 APIKey 查询用户账号与用量详情
func (s *OAuthKeyStore) GetByAPIKey(apiKey string) (OAuthKeyRecord, error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()
	if db == nil {
		return OAuthKeyRecord{}, fmt.Errorf("oauth key store is not initialized")
	}
	var r OAuthKeyRecord
	err := db.QueryRow(`
	SELECT provider, provider_user_id, username, name, email, avatar_url, api_key,
		created_at, updated_at, last_login_at, last_request_at,
		requests_total, COALESCE(requests_today, 0), COALESCE(request_day, ''), COALESCE(banned_at, 0), COALESCE(ban_reason, '')
	FROM oauth_keys
	WHERE api_key = ? AND COALESCE(banned_at, 0) = 0
	`, apiKey).Scan(
		&r.Provider, &r.ProviderUserID, &r.Username, &r.Name,
		&r.Email, &r.AvatarURL, &r.APIKey, &r.CreatedAt,
		&r.UpdatedAt, &r.LastLoginAt, &r.LastRequestAt,
		&r.RequestsTotal, &r.RequestsToday, &r.RequestDay, &r.BannedAt, &r.BanReason,
	)
	return r, err
}

func OAuthMeHandler(c *gin.Context) {
	apiKey := getRequestAPIKey(c)
	if apiKey == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing api key"})
		return
	}

	record, err := oauthKeys.GetByAPIKey(apiKey)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid api key"})
		return
	}

	today := time.Now().Format("2006-01-02")
	requestsToday := int64(0)
	if record.RequestDay == today {
		requestsToday = record.RequestsToday
	}

	c.JSON(http.StatusOK, gin.H{
		"provider":         record.Provider,
		"provider_user_id": record.ProviderUserID,
		"username":         record.Username,
		"name":             record.Name,
		"email":            record.Email,
		"avatar_url":       record.AvatarURL,
		"api_key":          record.APIKey,
		"requests_total":   record.RequestsTotal,
		"requests_today":   requestsToday,
		"last_request_at":  record.LastRequestAt,
		"created_at":       record.CreatedAt,
	})
}

// OAuthMeRotateKeyHandler 普通用户自助重置自身的 API Key
func OAuthMeRotateKeyHandler(c *gin.Context) {
	apiKey := getRequestAPIKey(c)
	if apiKey == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing api key"})
		return
	}

	oldRecord, err := oauthKeys.GetByAPIKey(apiKey)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid api key"})
		return
	}

	record, err := oauthKeys.RotateAPIKey(oldRecord.Provider, oldRecord.ProviderUserID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to rotate key", "detail": err.Error()})
		return
	}

	c.SetCookie("mimo_user_key", record.APIKey, 365*86400, "/", "", config.WebUICookieSecure, true)

	c.JSON(http.StatusOK, gin.H{
		"api_key": record.APIKey,
	})
}

func getRequestAPIKey(c *gin.Context) string {
	if cookieKey, err := c.Cookie("mimo_user_key"); err == nil && cookieKey != "" {
		return cookieKey
	}
	authHeader := c.GetHeader("Authorization")
	if authHeader != "" {
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) == 2 && strings.ToLower(parts[0]) == "bearer" {
			return strings.TrimSpace(parts[1])
		}
	}
	if customHeader := c.GetHeader("x-api-key"); customHeader != "" {
		return customHeader
	}
	return ""
}

func OAuthMeLogoutHandler(c *gin.Context) {
	c.SetCookie("mimo_user_key", "", -1, "/", "", config.WebUICookieSecure, true)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}
