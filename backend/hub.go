package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"math"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/time/rate"
)

// ---------- data types ----------

type Coords struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

// Peer is a connected client inside a room.
type Peer struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	PublicKey string   `json:"publicKey"`
	Device    string   `json:"device"`
	Coords    *Coords  `json:"coords,omitempty"`
	Distance  *float64 `json:"distance,omitempty"` // km from viewer, filled per-request

	conn     *websocket.Conn
	send     chan []byte
	room     *Room
	ip       string
	lim      *rate.Limiter
	strikes  int
	lastSeen time.Time

	mtx       sync.Mutex
	transfers map[string]*OutTransfer // outbound file transfers (sender-side validation)
}

// OutTransfer tracks a sender's file stream so chunks must arrive
// strictly in order and fully, blocking garbage floods.
type OutTransfer struct {
	Size    int64
	Chunks  int
	NextSeq int
}

type Room struct {
	Code    string
	mu      sync.RWMutex
	peers   map[string]*Peer
	created time.Time
}

type Hub struct {
	mu    sync.RWMutex
	rooms map[string]*Room
}

var hub = NewHub()

func NewHub() *Hub {
	return &Hub{rooms: make(map[string]*Room)}
}

// ---------- rooms ----------

func (h *Hub) CreateRoom(code string) *Room {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := &Room{Code: code, peers: make(map[string]*Peer), created: time.Now()}
	h.rooms[code] = r
	return r
}

func (h *Hub) RoomExists(code string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.rooms[code]
	return ok
}

// Join returns the room for code, creating it if needed.
func (h *Hub) Join(code string, p *Peer) *Room {
	h.mu.Lock()
	r, ok := h.rooms[code]
	if !ok {
		r = &Room{Code: code, peers: make(map[string]*Peer), created: time.Now()}
		h.rooms[code] = r
	}
	r.mu.Lock()
	r.peers[p.ID] = p
	r.mu.Unlock()
	h.mu.Unlock()

	p.room = r
	p.transfers = make(map[string]*OutTransfer)
	return r
}

func (h *Hub) Leave(p *Peer) {
	if p.room == nil {
		return
	}
	r := p.room
	r.mu.Lock()
	if _, ok := r.peers[p.ID]; ok {
		delete(r.peers, p.ID)
	}
	empty := len(r.peers) == 0
	r.mu.Unlock()

	h.mu.Lock()
	if empty {
		if _, ok := h.rooms[r.Code]; ok {
			delete(h.rooms, r.Code)
			log.Printf("room %s deleted (empty)", r.Code)
		}
	}
	h.mu.Unlock()

	if !empty {
		r.broadcastPresence()
	}
	log.Printf("peer %s (%s) left room %s", p.Name, p.ID, r.Code)
}

func (h *Hub) RoomInfo(code string) (map[string]any, bool) {
	h.mu.RLock()
	r, ok := h.rooms[code]
	h.mu.RUnlock()
	if !ok {
		return nil, false
	}
	r.mu.RLock()
	n := len(r.peers)
	created := r.created
	r.mu.RUnlock()
	return map[string]any{
		"code":    r.Code,
		"peers":   n,
		"created": created.UnixMilli(),
	}, true
}

func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.rooms)
}

func (h *Hub) PeerCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	n := 0
	for _, r := range h.rooms {
		r.mu.RLock()
		n += len(r.peers)
		r.mu.RUnlock()
	}
	return n
}

// Shutdown closes every connection gracefully.
func (h *Hub) Shutdown(ctx context.Context) {
	h.mu.RLock()
	rooms := make([]*Room, 0, len(h.rooms))
	for _, r := range h.rooms {
		rooms = append(rooms, r)
	}
	h.mu.RUnlock()
	for _, r := range rooms {
		r.mu.RLock()
		peers := make([]*Peer, 0, len(r.peers))
		for _, p := range r.peers {
			peers = append(peers, p)
		}
		r.mu.RUnlock()
		for _, p := range peers {
			p.closeWith("server shutting down")
		}
	}
}

// ---------- peer ops ----------

func (p *Peer) closeWith(reason string) {
	msg, _ := json.Marshal(Envelope{Type: "error", Msg: reason})
	select {
	case p.send <- msg:
	default:
	}
	_ = p.conn.Close()
}

func (p *Peer) sendJSON(v any) bool {
	b, err := json.Marshal(v)
	if err != nil {
		return false
	}
	select {
	case p.send <- b:
		return true
	case <-time.After(3 * time.Second):
		log.Printf("slow consumer %s (%s), dropping", p.Name, p.ID)
		_ = p.conn.Close()
		return false
	}
}

// ---------- room ops ----------

func (r *Room) SendTo(from *Peer, toID string, env Envelope) bool {
	r.mu.RLock()
	target, ok := r.peers[toID]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	return target.sendJSON(env)
}

func (r *Room) BroadcastFrom(from *Peer, env Envelope) {
	b, err := json.Marshal(env)
	if err != nil {
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.peers {
		if p == from {
			continue
		}
		select {
		case p.send <- b:
		default:
			log.Printf("slow consumer %s (%s), dropping", p.Name, p.ID)
			_ = p.conn.Close()
		}
	}
}

// PeerView is the wire representation of a peer (no locks/conn).
type PeerView struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	PublicKey string   `json:"publicKey"`
	Device    string   `json:"device"`
	Coords    *Coords  `json:"coords,omitempty"`
	Distance  *float64 `json:"distance,omitempty"` // km from viewer
}

// peerViews builds the roster from the viewer's perspective, with
// distances filled in when both sides share coordinates.
func (r *Room) peerViews(viewer *Peer) []PeerView {
	r.mu.RLock()
	peers := make([]PeerView, 0, len(r.peers))
	for _, p := range r.peers {
		v := PeerView{
			ID:        p.ID,
			Name:      p.Name,
			PublicKey: p.PublicKey,
			Device:    p.Device,
			Coords:    p.Coords,
		}
		if p.Coords != nil && viewer != nil && viewer.Coords != nil {
			d := haversineKm(viewer.Coords.Lat, viewer.Coords.Lng, p.Coords.Lat, p.Coords.Lng)
			v.Distance = &d
		}
		peers = append(peers, v)
	}
	r.mu.RUnlock()
	return peers
}

func (r *Room) broadcastPresence() {
	r.mu.RLock()
	peers := make([]*Peer, 0, len(r.peers))
	for _, p := range r.peers {
		peers = append(peers, p)
	}
	r.mu.RUnlock()
	for _, p := range peers {
		env := Envelope{Type: "presence", Peers: r.peerViews(p)}
		b, _ := json.Marshal(env)
		select {
		case p.send <- b:
		default:
			_ = p.conn.Close()
		}
	}
}

// ---------- helpers ----------

func haversineKm(lat1, lng1, lat2, lng2 float64) float64 {
	const R = 6371.0
	rad := math.Pi / 180
	dLat := (lat2 - lat1) * rad
	dLng := (lng2 - lng1) * rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLng/2)*math.Sin(dLng/2)
	return 2 * R * math.Asin(math.Sqrt(a))
}

const codeAlphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789" // no 0/O/1/I

func newCode() string {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		return "ABCDEF"
	}
	for i := range b {
		b[i] = codeAlphabet[int(b[i])%len(codeAlphabet)]
	}
	return string(b)
}

func newPeerID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "deadbeef00000001"
	}
	return hex.EncodeToString(b)
}

func validCode(code string) bool {
	if len(code) < 4 || len(code) > 8 {
		return false
	}
	for _, c := range code {
		if !((c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
			return false
		}
	}
	return true
}

func validTransferID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_') {
			return false
		}
	}
	return true
}
