package manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"mimo2api/internal/config"
	"mimo2api/internal/models"
)

type AccountManager struct {
	Users          map[string]models.UserRecord
	LifecycleStops map[string]chan struct{}
	rebuildCh      chan struct{}
	rebuildVersion uint64
	mu             sync.RWMutex
	UserOrder      []string // ordered user IDs for round-robin scheduling
	nextUserIndex  int
}

var GlobalManager = &AccountManager{
	Users:          make(map[string]models.UserRecord),
	LifecycleStops: make(map[string]chan struct{}),
	rebuildCh:      make(chan struct{}),
}

// config.MaxActiveLifecycleSlots 从 config.MaxActiveLifecycleSlots 读取，默认 4
// 环境变量: MIMO_MAX_ACTIVE_LIFECYCLE_SLOTS

var bjLoc = time.FixedZone("CST", 8*3600)

type waitSignal int

const (
	waitSignalExpired waitSignal = iota
	waitSignalRebuild
	waitSignalStop
)

func (m *AccountManager) GetUsersCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.Users)
}

func (m *AccountManager) GetActiveUsersCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.LifecycleStops)
}

func (m *AccountManager) AddUser(rawText string) (string, error) {
	re := regexp.MustCompile(`([a-zA-Z0-9_]+)="?([^;"]+)"?`)
	matches := re.FindAllStringSubmatch(rawText, -1)
	parsed := make(map[string]string)
	for _, match := range matches {
		if len(match) == 3 {
			parsed[match[1]] = match[2]
		}
	}

	uid := parsed["userId"]
	st := parsed["serviceToken"]
	ph := parsed["xiaomichatbot_ph"]

	if uid == "" || st == "" || ph == "" {
		// Fallback to JSON parsing for testing
		var user models.UserRecord
		if err := json.Unmarshal([]byte(rawText), &user); err != nil {
			return "", fmt.Errorf("missing required fields in cookie string")
		}
		uid = user.UserID
		st = user.ServiceToken
		ph = user.PH
		if uid == "" || st == "" || ph == "" {
			return "", fmt.Errorf("missing required fields")
		}
	}

	user := models.UserRecord{
		UserID:       uid,
		ServiceToken: st,
		PH:           ph,
		AddedAt:      float64(time.Now().Unix()),
		Status:       "QUEUED",
		RemainSec:    0,
	}

	m.mu.Lock()
	if oldStopCh, ok := m.LifecycleStops[user.UserID]; ok {
		close(oldStopCh)
	}
	m.Users[user.UserID] = user

	found := false
	for _, id := range m.UserOrder {
		if id == user.UserID {
			found = true
			break
		}
	}
	if !found {
		m.UserOrder = append(m.UserOrder, user.UserID)
	}
	m.mu.Unlock()

	os.MkdirAll("users", 0755)
	filePath := filepath.Join("users", fmt.Sprintf("user_%s.json", user.UserID))
	data, _ := json.MarshalIndent(user, "", "  ")
	os.WriteFile(filePath, data, 0644)

	m.ensureActiveSlots()

	return user.UserID, nil
}

type lifecycleLaunch struct {
	user   models.UserRecord
	stopCh chan struct{}
}

func (m *AccountManager) ensureActiveSlots() {
	launches := m.reserveLifecycleSlots()
	for _, launch := range launches {
		go m.runLifecycle(launch.user, launch.stopCh)
	}
}

func (m *AccountManager) reserveLifecycleSlots() []lifecycleLaunch {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.LifecycleStops == nil {
		m.LifecycleStops = make(map[string]chan struct{})
	}
	if len(m.UserOrder) == 0 || len(m.LifecycleStops) >= config.MaxActiveLifecycleSlots {
		return nil
	}

	now := float64(time.Now().Unix())
	launches := make([]lifecycleLaunch, 0, config.MaxActiveLifecycleSlots-len(m.LifecycleStops))
	for scanned := 0; len(m.LifecycleStops) < config.MaxActiveLifecycleSlots && scanned < len(m.UserOrder); scanned++ {
		if m.nextUserIndex >= len(m.UserOrder) {
			m.nextUserIndex = 0
		}
		userID := m.UserOrder[m.nextUserIndex]
		m.nextUserIndex = (m.nextUserIndex + 1) % len(m.UserOrder)

		if _, active := m.LifecycleStops[userID]; active {
			continue
		}
		user, ok := m.Users[userID]
		if !ok {
			continue
		}
		// 跳过今日已触发429限额的账号（北京时间0点后自动恢复）
		if user.DailyLimitAt > 0 {
			if user.DailyLimitAt >= beijingMidnightToday() {
				continue
			}
			// 清除过期的限额标记（已过午夜）
			user.DailyLimitAt = 0
			if user.Status == "DAILY_LIMIT" {
				user.Status = "QUEUED"
			}
			m.Users[userID] = user
		}

		stopCh := make(chan struct{})
		m.LifecycleStops[userID] = stopCh
		user.Status = "SCHEDULED"
		user.RemainSec = 0
		user.LastRefresh = now
		m.Users[userID] = user
		launches = append(launches, lifecycleLaunch{user: user, stopCh: stopCh})
	}

	return launches
}

func (m *AccountManager) releaseLifecycleSlot(userID string, stopCh chan struct{}) {
	m.mu.Lock()
	if current, ok := m.LifecycleStops[userID]; ok && current == stopCh {
		delete(m.LifecycleStops, userID)
		if user, exists := m.Users[userID]; exists {
			user.Status = "QUEUED"
			user.RemainSec = 0
			user.LastRefresh = float64(time.Now().Unix())
			m.Users[userID] = user
		}
	}
	m.mu.Unlock()

	m.ensureActiveSlots()
}

func (m *AccountManager) TriggerRebuild() {
	m.mu.Lock()
	m.rebuildVersion++
	close(m.rebuildCh)
	m.rebuildCh = make(chan struct{})
	m.mu.Unlock()

	m.ensureActiveSlots()
}

func (m *AccountManager) currentRebuildState() (chan struct{}, uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.rebuildCh, m.rebuildVersion
}

func (m *AccountManager) currentRebuildVersion() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.rebuildVersion
}

func isStopRequested(stopCh <-chan struct{}) bool {
	select {
	case <-stopCh:
		return true
	default:
		return false
	}
}

// waitForSignal blocks until the timer expires, a rebuild is requested, or the lifecycle is stopped.
// rebuildVersion keeps rebuild requests sticky so workers do not miss them while busy.
func (m *AccountManager) waitForSignal(stopCh <-chan struct{}, lastSeenRebuild uint64, waitTime time.Duration) (waitSignal, uint64) {
	if isStopRequested(stopCh) {
		return waitSignalStop, lastSeenRebuild
	}

	rebuildCh, rebuildVersion := m.currentRebuildState()
	if rebuildVersion > lastSeenRebuild {
		return waitSignalRebuild, rebuildVersion
	}

	timer := time.NewTimer(waitTime)
	defer timer.Stop()

	select {
	case <-timer.C:
		return waitSignalExpired, lastSeenRebuild
	case <-rebuildCh:
		_, nextVersion := m.currentRebuildState()
		return waitSignalRebuild, nextVersion
	case <-stopCh:
		return waitSignalStop, lastSeenRebuild
	}
}

func buildExecPrompt(user models.UserRecord) string {
	return fmt.Sprintf(
		"%s。运行前需要设置环境变量 NODE_ID=%q 和 NODE_LABEL=%q。",
		config.MimoExecPrompt,
		user.UserID,
		"user:"+user.UserID,
	)
}

// isRefusalReply 检测AI是否回复了拒绝性语句
// 如果AI拒绝执行，说明文件注入没有成功，当前账号会释放调度 slot，等待之后轮询。
func isRefusalReply(reply string) bool {
	refusalKeywords := []string{
		"抱歉",
		"无法",
		"违反",
		"安全",
		"不允许执行",
		"不能执行",
		"拒绝",
	}

	for _, keyword := range refusalKeywords {
		if strings.Contains(reply, keyword) {
			return true
		}
	}
	return false
}

// isDailyLimitError 检测创建实例时返回的不可恢复错误（需释放 slot）
// 新 API 返回 CreateApiError，通过 code/msg 判断
// 旧版返回 429 HTTP 状态码 + "今日创建次数已达上限"
func isDailyLimitError(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *CreateApiError
	if errors.As(err, &apiErr) {
		return apiErr.IsDailyLimit() || apiErr.IsAccountRisk()
	}
	return strings.Contains(err.Error(), "今日创建次数已达上限")
}

// beijingMidnightToday 返回北京时间今天0点的 Unix 时间戳
func beijingMidnightToday() float64 {
	now := time.Now().In(bjLoc)
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, bjLoc)
	return float64(midnight.Unix())
}

// markDailyLimit 标记用户今日已触发限额，并更新持久化文件
func (m *AccountManager) markDailyLimit(userID string) {
	m.mu.Lock()
	u, ok := m.Users[userID]
	if !ok {
		m.mu.Unlock()
		return
	}
	u.DailyLimitAt = float64(time.Now().Unix())
	u.Status = "DAILY_LIMIT"
	m.Users[userID] = u
	m.mu.Unlock()

	// 锁外持久化到文件
	os.MkdirAll("users", 0755)
	filePath := filepath.Join("users", fmt.Sprintf("user_%s.json", userID))
	if data, err := json.MarshalIndent(u, "", "  "); err == nil {
		os.WriteFile(filePath, data, 0644)
	}
}

func (m *AccountManager) updateUserRuntime(userID, status string, remainSec int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if u, ok := m.Users[userID]; ok {
		u.Status = status
		u.RemainSec = float64(remainSec)
		u.LastRefresh = float64(time.Now().Unix())
		m.Users[userID] = u
	}
}

func (m *AccountManager) runLifecycle(user models.UserRecord, stopCh chan struct{}) {
	payloadPath := "bridge/node-metrics-agent-linux-amd64.gif"
	lastSeenRebuild := m.currentRebuildVersion()
	userLogf := func(format string, args ...interface{}) {
		managerLogf("[manager:%s] %s", user.UserID, fmt.Sprintf(format, args...))
	}
	defer m.releaseLifecycleSlot(user.UserID, stopCh)

	for {
		if isStopRequested(stopCh) {
			return
		}

		userLogf("--- 占用调度 slot，检查实例状态 ---")
		client := NewNativeClawClient(user)

		st, remainSec, err := client.GetInstanceStatus()
		if err != nil {
			userLogf("获取实例状态失败: %v", err)
			signal, _ := m.waitForSignal(stopCh, lastSeenRebuild, 30*time.Second)
			if signal == waitSignalStop {
				return
			}
			continue
		}
		userLogf("status: %s, remain_sec: %d", st, remainSec)
		m.updateUserRuntime(user.UserID, st, remainSec)

		if st == "CREATING" {
			userLogf("实例仍在创建中，30s 后重试状态检查...")
			signal, version := m.waitForSignal(stopCh, lastSeenRebuild, 30*time.Second)
			if signal == waitSignalStop || signal == waitSignalRebuild {
				if signal == waitSignalRebuild {
					lastSeenRebuild = version
					userLogf("收到重建/重排信号，释放当前 slot...")
				}
				return
			}
			continue
		}

		if isStopRequested(stopCh) {
			return
		}

		if st == "AVAILABLE" {
			if remainSec <= config.SlotReleaseBufferSeconds {
				waitTime := time.Duration(remainSec+30) * time.Second
				if waitTime < 30*time.Second {
					waitTime = 30 * time.Second
				}
				userLogf("实例即将自然到期 (remain=%ds)，等待 %v 后重新检查，不主动销毁...", remainSec, waitTime)
				signal, version := m.waitForSignal(stopCh, lastSeenRebuild, waitTime)
				if signal == waitSignalStop || signal == waitSignalRebuild {
					if signal == waitSignalRebuild {
						lastSeenRebuild = version
						userLogf("收到重建/重排信号，释放当前 slot...")
					}
					return
				}
				continue
			}
			userLogf("发现可用实例 (remain=%ds)，开始连接与部署...", remainSec)
		} else {
			userLogf("正在创建新实例...")
			if err := client.CreateAndWait(); err != nil {
				client.Close()
				// 429 每日限额：标记账号并立即释放 slot，让其他账号轮询
				if isDailyLimitError(err) {
					userLogf("触发每日创建限额 (429)，标记账号并释放 slot...")
					m.markDailyLimit(user.UserID)
					return
				}
				userLogf("实例创建失败: %v，等待 60s 后重试...", err)
				signal, version := m.waitForSignal(stopCh, lastSeenRebuild, 60*time.Second)
				if signal == waitSignalStop || signal == waitSignalRebuild {
					if signal == waitSignalRebuild {
						lastSeenRebuild = version
						userLogf("收到重建/重排信号，释放当前 slot...")
					}
					return
				}
				continue
			}
			m.updateUserRuntime(user.UserID, "AVAILABLE", 0)
		}

		connected := false
		for retry := 0; retry < 5; retry++ {
			if isStopRequested(stopCh) {
				client.Close()
				return
			}
			if client.Connect() {
				connected = true
				break
			}

			userLogf("实例连接失败 (尝试 %d/5)，5s 后重试...", retry+1)
			client.Close()
			if retry == 4 {
				break
			}

			signal, version := m.waitForSignal(stopCh, lastSeenRebuild, 5*time.Second)
			switch signal {
			case waitSignalStop:
				return
			case waitSignalRebuild:
				lastSeenRebuild = version
				userLogf("收到重建/重排信号，释放当前 slot...")
				return
			case waitSignalExpired:
			}

			client = NewNativeClawClient(user)
		}
		if !connected {
			userLogf("实例连接全部失败，等待 60s 后重试整个生命周期...")
			signal, version := m.waitForSignal(stopCh, lastSeenRebuild, 60*time.Second)
			if signal == waitSignalStop || signal == waitSignalRebuild {
				if signal == waitSignalRebuild {
					lastSeenRebuild = version
					userLogf("收到重建/重排信号，释放当前 slot...")
				}
				return
			}
			continue
		}

		userLogf("发送初始化探活消息...")
		reply, err := client.SendChatAndWaitReply(config.MimoProbePrompt, 60*time.Second, nil)
		if err != nil {
			userLogf("failed to receive probe reply: %v", err)
		} else if reply != "" {
			userLogf("probe reply: %s", reply)
		}

		userLogf("开始上传载荷文件...")
		uploadData, err := client.UploadFile(payloadPath)
		if err != nil {
			userLogf("failed to upload payload: %v", err)
			client.Close()
			signal, version := m.waitForSignal(stopCh, lastSeenRebuild, 60*time.Second)
			if signal == waitSignalStop || signal == waitSignalRebuild {
				if signal == waitSignalRebuild {
					lastSeenRebuild = version
					userLogf("收到重建/重排信号，释放当前 slot...")
				}
				return
			}
			continue
		}

		userLogf("下发载荷执行指令...")
		reply, err = client.SendFileMessage(uploadData, buildExecPrompt(user))
		if err != nil {
			userLogf("下发载荷执行失败: %v", err)
			client.Close()
			signal, version := m.waitForSignal(stopCh, lastSeenRebuild, 60*time.Second)
			if signal == waitSignalStop || signal == waitSignalRebuild {
				if signal == waitSignalRebuild {
					lastSeenRebuild = version
					userLogf("收到重建/重排信号，释放当前 slot...")
				}
				return
			}
			continue
		} else {
			userLogf("deployment complete. AI reply: %s", reply)
			// 检测AI是否拒绝执行，如果拒绝说明文件注入没有成功，释放 slot 给下一个账号。
			if isRefusalReply(reply) {
				userLogf("检测到AI拒绝执行，文件注入可能未成功；不主动销毁，释放当前 slot...")
				client.Close()
				return
			}
		}

		if currentVersion := m.currentRebuildVersion(); currentVersion > lastSeenRebuild {
			lastSeenRebuild = currentVersion
			userLogf("检测到新的重建/重排请求，释放当前 slot...")
			client.Close()
			return
		}

		_, remainSecAfter, err := client.GetInstanceStatus()
		if err != nil {
			userLogf("部署后刷新实例状态失败: %v，10m 后重试检查...", err)
			client.Close()
			signal, version := m.waitForSignal(stopCh, lastSeenRebuild, 10*time.Minute)
			if signal == waitSignalStop || signal == waitSignalRebuild {
				if signal == waitSignalRebuild {
					lastSeenRebuild = version
					userLogf("收到重建/重排信号，释放当前 slot...")
				}
				return
			}
			continue
		}
		m.updateUserRuntime(user.UserID, "AVAILABLE", remainSecAfter)

		waitSec := remainSecAfter - config.SlotReleaseBufferSeconds
		if waitSec < 60 {
			waitSec = 60
		}
		waitTime := time.Duration(waitSec) * time.Second

		userLogf("部署完成 (remain=%ds)，休眠 %v 后释放 slot，轮询下一个账号...", remainSecAfter, waitTime)
		client.Close()
		signal, version := m.waitForSignal(stopCh, lastSeenRebuild, waitTime)
		if signal == waitSignalRebuild {
			lastSeenRebuild = version
			userLogf("收到重建/重排信号，释放当前 slot...")
		}
		if signal == waitSignalStop || signal == waitSignalRebuild || signal == waitSignalExpired {
			return
		}
	}
}

func (m *AccountManager) RemoveUser(userID string) {
	m.mu.Lock()

	if stopCh, ok := m.LifecycleStops[userID]; ok {
		close(stopCh)
		delete(m.LifecycleStops, userID)
	}

	delete(m.Users, userID)

	for i, id := range m.UserOrder {
		if id == userID {
			m.UserOrder = append(m.UserOrder[:i], m.UserOrder[i+1:]...)
			break
		}
	}
	if m.nextUserIndex > len(m.UserOrder) {
		m.nextUserIndex = 0
	}
	m.mu.Unlock()

	filePath := filepath.Join("users", fmt.Sprintf("user_%s.json", userID))
	os.Remove(filePath)

	m.ensureActiveSlots()
}

func (m *AccountManager) GetUsersList() []models.UserRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []models.UserRecord
	seen := make(map[string]struct{}, len(m.Users))
	for _, userID := range m.UserOrder {
		if user, ok := m.Users[userID]; ok {
			list = append(list, user)
			seen[userID] = struct{}{}
		}
	}
	for userID, user := range m.Users {
		if _, ok := seen[userID]; !ok {
			list = append(list, user)
		}
	}
	return list
}

func (m *AccountManager) LoadUsersFromDir(dirPath string) {
	files, err := os.ReadDir(dirPath)
	if err != nil {
		managerLogf("Failed to read users directory: %v", err)
		return
	}

	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".json" {
			path := filepath.Join(dirPath, file.Name())
			data, err := os.ReadFile(path)
			if err != nil {
				managerLogf("Failed to read user file %s: %v", path, err)
				continue
			}
			uid, err := m.AddUser(string(data))
			if err != nil {
				managerLogf("Failed to add user from %s: %v", path, err)
			} else {
				managerLogf("Loaded user %s from %s", uid, path)
			}
		}
	}
}
