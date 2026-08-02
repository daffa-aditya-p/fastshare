package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

var startTime = time.Now()

// ---------- REST rate limiting (per IP) ----------

type ipLimiter struct {
	mu sync.Mutex
	m  map[string]*limEntry
}

type limEntry struct {
	lim  *rate.Limiter
	last time.Time
}

func (l *ipLimiter) get(ip string) *rate.Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.m[ip]
	if !ok {
		e = &limEntry{lim: rate.NewLimiter(rate.Limit(60), 120), last: time.Now()}
		l.m[ip] = e
		// lazy cleanup
		if len(l.m) > 512 {
			now := time.Now()
			for k, v := range l.m {
				if now.Sub(v.last) > 10*time.Minute {
					delete(l.m, k)
				}
			}
		}
	}
	e.last = time.Now()
	return e.lim
}

var restLimiter = &ipLimiter{m: make(map[string]*limEntry)}

// ---------- handlers ----------

func handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"rooms":   hub.Count(),
		"peers":   hub.PeerCount(),
		"uptime":  int(time.Since(startTime).Seconds()),
		"service": "fastshare",
	})
}

func handleCreateRoom(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method tidak diizinkan"})
		return
	}
	var body struct {
		Code string `json:"code"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body)
	code := strings.ToUpper(strings.TrimSpace(body.Code))
	if code != "" {
		if !validCode(code) {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "kode tidak valid (4-8 karakter A-Z/0-9)"})
			return
		}
		if hub.RoomExists(code) {
			writeJSON(w, http.StatusConflict, map[string]any{"error": "room sudah ada, langsung gabung saja"})
			return
		}
	}
	if code == "" {
		code = newCode()
	}
	hub.CreateRoom(code)
	log.Printf("room %s created from %s", code, clientIP(r))
	writeJSON(w, http.StatusOK, map[string]any{"code": code})
}

func handleRoomInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method tidak diizinkan"})
		return
	}
	code := strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/api/rooms/")))
	if !validCode(code) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "kode tidak valid"})
		return
	}
	info, ok := hub.RoomInfo(code)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "room tidak ditemukan"})
		return
	}
	writeJSON(w, http.StatusOK, info)
}

// ---------- middleware ----------

func withCommonHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// REST rate limit (WS has its own per-conn limiter)
		if strings.HasPrefix(r.URL.Path, "/api/") {
			if !restLimiter.get(clientIP(r)).Allow() {
				writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": "terlalu banyak permintaan"})
				return
			}
		}

		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy",
			"default-src 'self'; script-src 'self' https://static.cloudflareinsights.com; img-src 'self' data: blob:; style-src 'self' 'unsafe-inline'; "+
				"connect-src 'self' ws: wss:; frame-ancestors 'none'; base-uri 'self'; form-action 'self'")
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
