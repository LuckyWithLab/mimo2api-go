package config

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

var (
	ServerHost              string
	ServerPort              int
	GinMode                 string
	TrustedProxies          []string
	ManagerLogPath          string
	ShutdownTaskTimeout     int
	APIKeys                 []string
	WSAuthToken             string
	WebUIUsername           string
	WebUIPassword           string
	WebUISecretKey          string
	WebUICookieName         string
	WebUISessionTTL         int
	WebUICookieSecure       bool
	NodeResponseIdleTimeout int
	Node401Cooldown         int
	MetricsDBPath           string
	MetricsRetentionDays    int
	MetricsBucketSeconds    int
	AistudioConnectIPs      []string
	AistudioHost            = "aistudio.xiaomimimo.com"
	AistudioBaseURL         = "https://aistudio.xiaomimimo.com"
	AistudioWSURL           = "wss://aistudio.xiaomimimo.com/ws/proxy"
)

func Load() {
	_ = godotenv.Load() // ignore error if .env doesn't exist

	ServerHost = getEnv("SERVER_HOST", "0.0.0.0")
	ServerPort = getEnvAsInt("SERVER_PORT", 8000)
	GinMode = getFirstEnv([]string{"MIMO_GIN_MODE", "GIN_MODE"}, "release")
	TrustedProxies = getEnvAsSliceFromKeys([]string{"MIMO_TRUSTED_PROXIES", "TRUSTED_PROXIES"}, nil)
	ManagerLogPath = getEnv("MIMO_MANAGER_LOG_PATH", "logs/manager.log")
	ShutdownTaskTimeout = getEnvAsInt("MIMO_SHUTDOWN_TASK_TIMEOUT", 5)

	APIKeys = getEnvAsSliceFromKeys([]string{"MIMO_RELAY_OPENAI_KEY", "MIMO_API_KEYS"}, nil)
	WSAuthToken = getEnv("MIMO_WS_AUTH_TOKEN", "")
	WebUIUsername = getEnv("MIMO_WEBUI_USERNAME", "admin")
	WebUIPassword = getEnv("MIMO_WEBUI_PASSWORD", "")
	WebUISecretKey = getFirstEnv([]string{"MIMO_WEBUI_SECRET", "MIMO_WEBUI_SECRET_KEY"}, "")
	if WebUISecretKey == "" {
		if WebUIPassword != "" {
			WebUISecretKey = WebUIPassword
		} else if len(APIKeys) > 0 {
			WebUISecretKey = APIKeys[0]
		} else {
			WebUISecretKey = "mimo2-webui-fallback-secret"
		}
	}
	WebUICookieName = getEnv("MIMO_WEBUI_COOKIE_NAME", "mimo_webui_session")
	WebUISessionTTL = getEnvAsInt("MIMO_WEBUI_SESSION_TTL_SECONDS", 43200)
	if WebUISessionTTL < 300 {
		WebUISessionTTL = 300
	}
	WebUICookieSecure = getEnvAsBool("MIMO_WEBUI_COOKIE_SECURE", false)

	NodeResponseIdleTimeout = getEnvAsIntFromKeys([]string{
		"MIMO_NODE_RESPONSE_IDLE_TIMEOUT",
		"MIMO_BRIDGE_IDLE_TIMEOUT",
	}, 90)
	Node401Cooldown = getEnvAsInt("MIMO_NODE_401_COOLDOWN_SECONDS", 3600)
	MetricsDBPath = getEnv("MIMO_METRICS_DB_PATH", "gateway_metrics.db")
	MetricsRetentionDays = getEnvAsInt("MIMO_METRICS_RETENTION_DAYS", 90)
	MetricsBucketSeconds = getEnvAsInt("MIMO_METRICS_BUCKET_SECONDS", 1800)

	defaultIPs := []string{
		"220.181.104.191",
		"202.69.4.23",
		"39.101.90.223",
		"220.181.104.192",
		"124.251.34.64",
		"111.13.213.63",
		"202.69.4.22",
		"111.13.213.62",
	}
	AistudioConnectIPs = getEnvAsSliceFromKeys([]string{"MIMO2API_AISTUDIO_IP", "AISTUDIO_CONNECT_IPS"}, defaultIPs)
	if len(AistudioConnectIPs) == 0 {
		AistudioConnectIPs = defaultIPs
	}
}

func getEnv(key, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return strings.TrimSpace(value)
	}
	return defaultVal
}

func getFirstEnv(keys []string, defaultVal string) string {
	for _, key := range keys {
		if value, exists := os.LookupEnv(key); exists {
			trimmed := strings.TrimSpace(value)
			if trimmed != "" {
				return trimmed
			}
		}
	}
	return defaultVal
}

func getEnvAsInt(key string, defaultVal int) int {
	valStr := getEnv(key, "")
	if valStr == "" {
		return defaultVal
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		log.Printf("Invalid int value for %s: %v, using default %d", key, valStr, defaultVal)
		return defaultVal
	}
	return val
}

func getEnvAsIntFromKeys(keys []string, defaultVal int) int {
	valStr := getFirstEnv(keys, "")
	if valStr == "" {
		return defaultVal
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		log.Printf("Invalid int value for %v: %v, using default %d", keys, valStr, defaultVal)
		return defaultVal
	}
	return val
}

func getEnvAsSlice(key string, defaultVal []string) []string {
	valStr := getEnv(key, "")
	if valStr == "" {
		return defaultVal
	}
	return parseSlice(valStr)
}

func getEnvAsSliceFromKeys(keys []string, defaultVal []string) []string {
	valStr := getFirstEnv(keys, "")
	if valStr == "" {
		return defaultVal
	}
	return parseSlice(valStr)
}

func getEnvAsBool(key string, defaultVal bool) bool {
	valStr := strings.ToLower(getEnv(key, ""))
	if valStr == "" {
		return defaultVal
	}
	return valStr == "1" || valStr == "true" || valStr == "yes" || valStr == "on"
}

func parseSlice(valStr string) []string {
	parts := strings.Split(valStr, ",")
	var result []string
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
