package main

import (
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/time/rate"
)

const (
	writeWait    = 10 * time.Second
	pongWait     = 90 * time.Second
	pingPeriod   = 30 * time.Second
	maxMessage   = 1 << 20 // 1 MB per WS message (chunk 256KB base64 ~342KB)
	chunkSize    = 256 << 10
	maxFileSize  = int64(8) << 30 // 8 GB server-side guard, env-overridable
	maxChunks    = maxFileSize/chunkSize + 2
	maxTransfers = 8 // concurrent outbound streams per peer
	wsRate       = 120
	wsBurst      = 240
	maxStrikes   = 5
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  64 << 10,
	WriteBufferSize: 64 << 10,
	CheckOrigin:     checkOrigin,
}

// Enc is an AES-GCM ciphertext envelope. The server never inspects it.
type Enc struct {
	Iv string `json:"iv"`
	Ct string `json:"ct"`
}

// Envelope is the relay message shape used over the WebSocket.
type Envelope struct {
	Type string `json:"type"`

	// hello
	Name      string  `json:"name,omitempty"`
	PublicKey string  `json:"publicKey,omitempty"`
	Device    string  `json:"device,omitempty"`
	Coords    *Coords `json:"coords,omitempty"`

	// routing
	To   string          `json:"to,omitempty"`
	Encs map[string]*Enc `json:"encs,omitempty"`
	Enc  *Enc            `json:"enc,omitempty"`

	// file stream
	TransferID string `json:"transferID,omitempty"`
	Seq        int    `json:"seq,omitempty"`
	Size       int64  `json:"size,omitempty"`
	Chunks     int    `json:"chunks,omitempty"`
	Hash       string `json:"hash,omitempty"`
	Ok         bool   `json:"ok,omitempty"`

	// server-filled on relay
	From     string     `json:"from,omitempty"`
	FromName string     `json:"fromName,omitempty"`
	Ts       int64      `json:"ts,omitempty"`
	Msg      string     `json:"msg,omitempty"`
	Peers    []PeerView `json:"peers,omitempty"`
	ID       string     `json:"id,omitempty"`
	Code     string     `json:"code,omitempty"`
}

func checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true // non-browser client (CLI, tests)
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	oh := strings.ToLower(u.Hostname())
	host := r.Host
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	host = strings.ToLower(host)
	if oh == host || oh == "localhost" || oh == "127.0.0.1" {
		return true
	}
	if oh == "daffadev.my.id" || strings.HasSuffix(oh, ".daffadev.my.id") {
		return true
	}
	return false
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	code := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("code")))
	if !validCode(code) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "kode room wajib (4-8 karakter A-Z/0-9)"})
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	peer := &Peer{
		ID:       newPeerID(),
		send:     make(chan []byte, 512),
		conn:     conn,
		ip:       clientIP(r),
		lim:      rate.NewLimiter(rate.Limit(wsRate), wsBurst),
		lastSeen: time.Now(),
	}
	hub.Join(code, peer)

	conn.SetReadLimit(maxMessage)
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		peer.lastSeen = time.Now()
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	go peer.writeLoop()
	peer.readLoop()
}

func (p *Peer) readLoop() {
	defer func() {
		hub.Leave(p)
		_ = p.conn.Close()
	}()
	for {
		_, data, err := p.conn.ReadMessage()
		if err != nil {
			return
		}
		if !p.lim.Allow() {
			p.strikes++
			if p.strikes >= maxStrikes {
				_ = p.sendJSON(Envelope{Type: "error", Msg: "terlalu banyak permintaan"})
				return
			}
			continue
		}
		p.lastSeen = time.Now()
		var m Envelope
		if err := json.Unmarshal(data, &m); err != nil {
			_ = p.sendJSON(Envelope{Type: "error", Msg: "pesan tidak valid"})
			continue
		}
		if err := p.handle(&m); err != nil {
			_ = p.sendJSON(Envelope{Type: "error", Msg: err.Error()})
			p.strikes++
			if p.strikes >= maxStrikes {
				return
			}
		}
	}
}

func (p *Peer) writeLoop() {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case msg, ok := <-p.send:
			_ = p.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = p.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := p.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = p.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := p.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (p *Peer) handle(m *Envelope) error {
	now := time.Now().UnixMilli()

	switch m.Type {
	case "hello":
		name := strings.TrimSpace(m.Name)
		if name == "" || len(name) > 64 {
			return errMsg("nama wajib diisi (maks 64 karakter)")
		}
		if len(m.PublicKey) == 0 || len(m.PublicKey) > 512 {
			return errMsg("publicKey tidak valid")
		}
		if m.Coords != nil && (m.Coords.Lat < -90 || m.Coords.Lat > 90 || m.Coords.Lng < -180 || m.Coords.Lng > 180) {
			return errMsg("koordinat tidak valid")
		}
		p.Name = name
		p.PublicKey = m.PublicKey
		p.Device = m.Device
		p.Coords = m.Coords
		p.lastSeen = time.Now()
		log.Printf("peer %s (%s, %s) joined room %s from %s", p.Name, p.Device, p.ID, p.room.Code, p.ip)

		peers := p.room.peerViews(p)
		_ = p.sendJSON(Envelope{Type: "welcome", ID: p.ID, Code: p.room.Code, Peers: peers})
		p.room.broadcastPresence()
		return nil

	case "chat":
		if len(m.Encs) == 0 {
			return errMsg("chat kosong")
		}
		for toID, enc := range m.Encs {
			if enc == nil || enc.Ct == "" {
				continue
			}
			p.room.SendTo(p, toID, Envelope{Type: "chat", From: p.ID, FromName: p.Name, Enc: enc, Ts: now})
		}
		return nil

	case "file-meta":
		if m.To == "" || !validTransferID(m.TransferID) || m.Enc == nil || m.Chunks < 1 || int64(m.Chunks) > maxChunks || m.Size < 0 || m.Size > maxFileSize {
			return errMsg("file-meta tidak valid")
		}
		p.mtx.Lock()
		if _, dup := p.transfers[m.TransferID]; dup {
			p.mtx.Unlock()
			return errMsg("transfer sudah ada")
		}
		if len(p.transfers) >= maxTransfers {
			p.mtx.Unlock()
			return errMsg("terlalu banyak transfer bersamaan (maks 8)")
		}
		p.transfers[m.TransferID] = &OutTransfer{Size: m.Size, Chunks: m.Chunks}
		p.mtx.Unlock()

		p.room.SendTo(p, m.To, Envelope{
			Type: "file-meta", From: p.ID, FromName: p.Name,
			TransferID: m.TransferID, Size: m.Size, Chunks: m.Chunks, Enc: m.Enc, Ts: now,
		})
		return nil

	case "file-chunk":
		if m.To == "" || !validTransferID(m.TransferID) || m.Enc == nil || m.Enc.Ct == "" || m.Seq < 0 {
			return errMsg("file-chunk tidak valid")
		}
		p.mtx.Lock()
		t, ok := p.transfers[m.TransferID]
		if !ok || m.Seq != t.NextSeq {
			p.mtx.Unlock()
			return errMsg("urutan chunk tidak valid")
		}
		t.NextSeq++
		if t.NextSeq == t.Chunks {
			delete(p.transfers, m.TransferID)
		}
		p.mtx.Unlock()

		p.room.SendTo(p, m.To, Envelope{
			Type: "file-chunk", From: p.ID,
			TransferID: m.TransferID, Seq: m.Seq, Enc: m.Enc,
		})
		return nil

	case "file-done":
		if m.To == "" || !validTransferID(m.TransferID) {
			return errMsg("file-done tidak valid")
		}
		p.room.SendTo(p, m.To, Envelope{
			Type: "file-done", From: p.ID,
			TransferID: m.TransferID, Ok: m.Ok, Hash: m.Hash,
		})
		return nil

	case "file-ack":
		if m.To == "" || !validTransferID(m.TransferID) || m.Seq < 0 {
			return errMsg("file-ack tidak valid")
		}
		p.room.SendTo(p, m.To, Envelope{
			Type: "file-ack", From: p.ID,
			TransferID: m.TransferID, Seq: m.Seq,
		})
		return nil

	case "typing":
		if m.To == "*" {
			p.room.BroadcastFrom(p, Envelope{Type: "typing", From: p.ID})
			return nil
		}
		if m.To == "" {
			return nil
		}
		p.room.SendTo(p, m.To, Envelope{Type: "typing", From: p.ID})
		return nil

	default:
		return errMsg("tipe pesan tidak dikenal")
	}
}

type protoErr struct{ s string }

func (e protoErr) Error() string { return e.s }

func errMsg(s string) error { return protoErr{s} }

// clientIP prefers Cloudflare's header, then X-Forwarded-For, then remote addr.
func clientIP(r *http.Request) string {
	if cf := r.Header.Get("CF-Connecting-IP"); cf != "" {
		return cf
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	host, _, err := splitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func splitHostPort(addr string) (string, string, error) {
	i := strings.LastIndexByte(addr, ':')
	if i < 0 {
		return addr, "", nil
	}
	return addr[:i], addr[i+1:], nil
}
