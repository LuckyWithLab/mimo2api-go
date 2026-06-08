package auth

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"

	"mimo2api/internal/config"
)

func TestOAuthKeyStoreReusesKeyForSameProviderUser(t *testing.T) {
	store := &OAuthKeyStore{}
	tmp := t.TempDir()
	if err := store.Load(filepath.Join(tmp, "oauth_keys.db"), filepath.Join(tmp, "missing_oauth_keys.json")); err != nil {
		t.Fatalf("failed to load empty store: %v", err)
	}

	first, created, err := store.Upsert(OAuthKeyRecord{
		Provider:       linuxDoProvider,
		ProviderUserID: "42",
		Username:       "first-name",
	})
	if err != nil {
		t.Fatalf("failed to create key: %v", err)
	}
	if !created {
		t.Fatal("expected first upsert to create a key")
	}

	second, created, err := store.Upsert(OAuthKeyRecord{
		Provider:       linuxDoProvider,
		ProviderUserID: "42",
		Username:       "updated-name",
	})
	if err != nil {
		t.Fatalf("failed to update key: %v", err)
	}
	if created {
		t.Fatal("expected second upsert to reuse existing key")
	}
	if second.APIKey != first.APIKey {
		t.Fatalf("expected API key to be reused, got %q then %q", first.APIKey, second.APIKey)
	}
	if second.Username != "updated-name" {
		t.Fatalf("expected user profile to be updated, got %q", second.Username)
	}
}

func TestOAuthKeyStoreRecordsUsageAndPreservesItAcrossLogin(t *testing.T) {
	store := &OAuthKeyStore{}
	tmp := t.TempDir()
	if err := store.Load(filepath.Join(tmp, "oauth_keys.db"), filepath.Join(tmp, "missing_oauth_keys.json")); err != nil {
		t.Fatalf("failed to load empty store: %v", err)
	}

	record, _, err := store.Upsert(OAuthKeyRecord{
		Provider:       linuxDoProvider,
		ProviderUserID: "42",
		Username:       "linuxdo-user",
	})
	if err != nil {
		t.Fatalf("failed to create key: %v", err)
	}
	for i := 0; i < 3; i++ {
		if !store.RecordAPIKeyRequest(record.APIKey) {
			t.Fatal("expected oauth key request to be recorded")
		}
	}

	updated, created, err := store.Upsert(OAuthKeyRecord{
		Provider:       linuxDoProvider,
		ProviderUserID: "42",
		Username:       "renamed-user",
	})
	if err != nil {
		t.Fatalf("failed to update user: %v", err)
	}
	if created {
		t.Fatal("expected existing key to be reused")
	}
	if updated.RequestsTotal != 3 {
		t.Fatalf("expected request count to survive login, got %d", updated.RequestsTotal)
	}

	usages := store.ListUsage()
	if len(usages) != 1 {
		t.Fatalf("expected one usage row, got %d", len(usages))
	}
	if usages[0].RequestsTotal != 3 || usages[0].RequestsToday != 3 {
		t.Fatalf("unexpected usage totals: %+v", usages[0])
	}
	if usages[0].APIKeyPreview == record.APIKey {
		t.Fatal("expected usage API key to be masked")
	}
}

func TestOAuthKeyStoreRotatesKeyAndInvalidatesOldKey(t *testing.T) {
	store := &OAuthKeyStore{}
	tmp := t.TempDir()
	if err := store.Load(filepath.Join(tmp, "oauth_keys.db"), filepath.Join(tmp, "missing_oauth_keys.json")); err != nil {
		t.Fatalf("failed to load empty store: %v", err)
	}

	record, _, err := store.Upsert(OAuthKeyRecord{
		Provider:       linuxDoProvider,
		ProviderUserID: "42",
		Username:       "linuxdo-user",
	})
	if err != nil {
		t.Fatalf("failed to create key: %v", err)
	}
	if !store.RecordAPIKeyRequest(record.APIKey) {
		t.Fatal("expected request to be recorded before rotation")
	}

	rotated, err := store.RotateAPIKey(linuxDoProvider, "42")
	if err != nil {
		t.Fatalf("failed to rotate key: %v", err)
	}
	if rotated.APIKey == record.APIKey {
		t.Fatal("expected rotated key to differ from old key")
	}
	if store.IsValidAPIKey(record.APIKey) {
		t.Fatal("expected old key to be invalidated")
	}
	if !store.IsValidAPIKey(rotated.APIKey) {
		t.Fatal("expected new key to be valid")
	}
	if rotated.RequestsTotal != 1 {
		t.Fatalf("expected usage to be preserved, got %d", rotated.RequestsTotal)
	}
}

func TestOAuthKeyStoreBansAndUnbansUser(t *testing.T) {
	store := &OAuthKeyStore{}
	tmp := t.TempDir()
	if err := store.Load(filepath.Join(tmp, "oauth_keys.db"), filepath.Join(tmp, "missing_oauth_keys.json")); err != nil {
		t.Fatalf("failed to load empty store: %v", err)
	}

	record, _, err := store.Upsert(OAuthKeyRecord{
		Provider:       linuxDoProvider,
		ProviderUserID: "42",
		Username:       "linuxdo-user",
	})
	if err != nil {
		t.Fatalf("failed to create key: %v", err)
	}

	banned, err := store.BanUser(linuxDoProvider, "42", "abuse")
	if err != nil {
		t.Fatalf("failed to ban user: %v", err)
	}
	if banned.BannedAt == 0 || banned.BanReason != "abuse" {
		t.Fatalf("expected ban metadata to be set, got %+v", banned)
	}
	if store.IsValidAPIKey(record.APIKey) {
		t.Fatal("expected old key to be invalid after ban")
	}
	if store.IsValidAPIKey(banned.APIKey) {
		t.Fatal("expected banned user's rotated key to be rejected")
	}
	if store.RecordAPIKeyRequest(banned.APIKey) {
		t.Fatal("expected banned key request not to be recorded")
	}

	_, _, err = store.Upsert(OAuthKeyRecord{
		Provider:       linuxDoProvider,
		ProviderUserID: "42",
		Username:       "linuxdo-user",
	})
	if !errors.Is(err, ErrOAuthUserBanned) {
		t.Fatalf("expected banned user login to be rejected, got %v", err)
	}

	unbanned, err := store.UnbanUser(linuxDoProvider, "42")
	if err != nil {
		t.Fatalf("failed to unban user: %v", err)
	}
	if unbanned.BannedAt != 0 || unbanned.BanReason != "" {
		t.Fatalf("expected ban metadata to be cleared, got %+v", unbanned)
	}
	if !store.IsValidAPIKey(unbanned.APIKey) {
		t.Fatal("expected key to become valid after unban")
	}
}

func TestOAuthKeyStoreMigratesLegacyJSON(t *testing.T) {
	tmp := t.TempDir()
	legacyPath := filepath.Join(tmp, "oauth_keys.json")
	dbPath := filepath.Join(tmp, "oauth_keys.db")
	legacyRecords := []OAuthKeyRecord{
		{
			Provider:       linuxDoProvider,
			ProviderUserID: "42",
			Username:       "legacy-user",
			APIKey:         "sk-mimo-legacy",
			CreatedAt:      10,
			UpdatedAt:      20,
			LastLoginAt:    20,
			LastRequestAt:  30,
			RequestsTotal:  7,
			RequestsToday:  3,
			RequestDay:     "2099-01-01",
		},
	}
	data, err := json.Marshal(legacyRecords)
	if err != nil {
		t.Fatalf("failed to marshal legacy records: %v", err)
	}
	if err := os.WriteFile(legacyPath, data, 0600); err != nil {
		t.Fatalf("failed to write legacy records: %v", err)
	}

	store := &OAuthKeyStore{}
	if err := store.Load(dbPath, legacyPath); err != nil {
		t.Fatalf("failed to load store with legacy migration: %v", err)
	}

	if !store.IsValidAPIKey("sk-mimo-legacy") {
		t.Fatal("expected migrated API key to be valid")
	}
	usages := store.ListUsage()
	if len(usages) != 1 {
		t.Fatalf("expected one migrated usage row, got %d", len(usages))
	}
	if usages[0].RequestsTotal != 7 {
		t.Fatalf("expected migrated request total 7, got %d", usages[0].RequestsTotal)
	}
}

func TestAPIAuthMiddlewareAcceptsOAuthKey(t *testing.T) {
	gin.SetMode(gin.TestMode)

	oldAPIKeys := config.APIKeys
	oldDBPath := config.OAuthDBPath
	oldStorePath := config.OAuthKeyStorePath
	tmp := t.TempDir()
	emptyStorePath := filepath.Join(tmp, "empty_oauth_keys.json")
	resetDBPath := filepath.Join(tmp, "reset_oauth_keys.db")
	resetLegacyPath := filepath.Join(tmp, "reset_oauth_keys.json")
	config.APIKeys = nil
	config.OAuthDBPath = filepath.Join(tmp, "oauth_keys.db")
	config.OAuthKeyStorePath = emptyStorePath
	t.Cleanup(func() {
		config.APIKeys = oldAPIKeys
		config.OAuthDBPath = oldDBPath
		config.OAuthKeyStorePath = oldStorePath
		_ = LoadOAuthKeyStore(resetDBPath, resetLegacyPath)
	})

	if err := LoadOAuthKeyStore(config.OAuthDBPath, config.OAuthKeyStorePath); err != nil {
		t.Fatalf("failed to load empty oauth key store: %v", err)
	}
	record, _, err := oauthKeys.Upsert(OAuthKeyRecord{
		Provider:       linuxDoProvider,
		ProviderUserID: "42",
		Username:       "linuxdo-user",
	})
	if err != nil {
		t.Fatalf("failed to create oauth key: %v", err)
	}

	r := gin.New()
	r.Use(APIAuthMiddleware())
	r.GET("/protected", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	unauthorizedReq := httptest.NewRequest(http.MethodGet, "/protected", nil)
	unauthorizedResp := httptest.NewRecorder()
	r.ServeHTTP(unauthorizedResp, unauthorizedReq)
	if unauthorizedResp.Code != http.StatusUnauthorized {
		t.Fatalf("expected missing key to be rejected, got %d", unauthorizedResp.Code)
	}

	authorizedReq := httptest.NewRequest(http.MethodGet, "/protected", nil)
	authorizedReq.Header.Set("Authorization", "Bearer "+record.APIKey)
	authorizedResp := httptest.NewRecorder()
	r.ServeHTTP(authorizedResp, authorizedReq)
	if authorizedResp.Code != http.StatusNoContent {
		t.Fatalf("expected oauth key to be accepted, got %d", authorizedResp.Code)
	}

	usages := oauthKeys.ListUsage()
	if len(usages) != 1 || usages[0].RequestsTotal != 1 {
		t.Fatalf("expected middleware to record one oauth request, got %+v", usages)
	}
}

func TestAPIAuthMiddlewareRequiresKeyWhenOAuthConfigured(t *testing.T) {
	gin.SetMode(gin.TestMode)

	oldAPIKeys := config.APIKeys
	oldClientID := config.OAuthClientID
	oldClientSecret := config.OAuthClientSecret
	tmp := t.TempDir()
	emptyDBPath := filepath.Join(tmp, "empty_oauth_keys.db")
	emptyStorePath := filepath.Join(tmp, "empty_oauth_keys.json")
	config.APIKeys = nil
	config.OAuthClientID = "client-id"
	config.OAuthClientSecret = "client-secret"
	t.Cleanup(func() {
		config.APIKeys = oldAPIKeys
		config.OAuthClientID = oldClientID
		config.OAuthClientSecret = oldClientSecret
		_ = LoadOAuthKeyStore(emptyDBPath, emptyStorePath)
	})

	if err := LoadOAuthKeyStore(emptyDBPath, emptyStorePath); err != nil {
		t.Fatalf("failed to load empty oauth key store: %v", err)
	}

	r := gin.New()
	r.Use(APIAuthMiddleware())
	r.GET("/protected", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("expected OAuth config to enable API auth, got %d", resp.Code)
	}
}
