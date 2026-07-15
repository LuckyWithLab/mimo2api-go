package tts

import (
	"fmt"
	"sync"
	"time"

	"mimo2api/internal/manager"
	"mimo2api/internal/models"
)

// AccountPool round-robins Studio cookie accounts from manager.GlobalManager.
// On auth failures, accounts enter a cooldown and are skipped.
type AccountPool struct {
	mu         sync.Mutex
	nextIndex  int
	cooldowns  map[string]time.Time // userId -> until
	cooldownFor time.Duration
}

// GlobalPool is the process-wide TTS account pool.
var GlobalPool = &AccountPool{
	cooldowns:   make(map[string]time.Time),
	cooldownFor: 10 * time.Minute,
}

// SetCooldown configures how long a failed account stays out of rotation.
func (p *AccountPool) SetCooldown(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if d > 0 {
		p.cooldownFor = d
	}
}

// MarkCooldown marks an account as temporarily unavailable (e.g. 401).
func (p *AccountPool) MarkCooldown(userID string) {
	if userID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cooldowns == nil {
		p.cooldowns = make(map[string]time.Time)
	}
	p.cooldowns[userID] = time.Now().Add(p.cooldownFor)
}

// ClearCooldown removes a cooldown entry.
func (p *AccountPool) ClearCooldown(userID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.cooldowns, userID)
}

// Pick returns the next usable account (round-robin, skip cooldown/missing tokens).
func (p *AccountPool) Pick() (models.UserRecord, error) {
	users := manager.GlobalManager.GetUsersList()
	if len(users) == 0 {
		return models.UserRecord{}, fmt.Errorf("no studio accounts loaded (users/ empty)")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cooldowns == nil {
		p.cooldowns = make(map[string]time.Time)
	}
	now := time.Now()
	// purge expired
	for id, until := range p.cooldowns {
		if now.After(until) {
			delete(p.cooldowns, id)
		}
	}

	n := len(users)
	if p.nextIndex >= n {
		p.nextIndex = 0
	}
	start := p.nextIndex
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		u := users[idx]
		if u.UserID == "" || u.ServiceToken == "" || u.PH == "" {
			continue
		}
		if until, cool := p.cooldowns[u.UserID]; cool && now.Before(until) {
			continue
		}
		p.nextIndex = (idx + 1) % n
		return u, nil
	}
	return models.UserRecord{}, fmt.Errorf("no available studio accounts (all missing tokens or in cooldown)")
}

// AvailableCount returns how many accounts are currently pickable.
func (p *AccountPool) AvailableCount() int {
	users := manager.GlobalManager.GetUsersList()
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	count := 0
	for _, u := range users {
		if u.UserID == "" || u.ServiceToken == "" || u.PH == "" {
			continue
		}
		if until, cool := p.cooldowns[u.UserID]; cool && now.Before(until) {
			continue
		}
		count++
	}
	return count
}
