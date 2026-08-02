# FastShare ⚡

Kirim file & chat **terenkripsi end-to-end**, tanpa login, tanpa batas.
Backend **Go** (ringan, satu binary) + frontend **React** (Vite), komunikasi via **WebSocket**.

- 📡 **Beacon proximity** — peer terdekat muncul di radar dengan jarak asli (GPS)
- 🌍 **Jarak jauh** — share kode room, siapa pun bisa gabung dari mana saja
- 🔐 **E2E encrypted** — ECDH P-256 + HKDF-SHA256 + AES-256-GCM, server cuma nge-relay ciphertext
- 💬 **Chat kayak WhatsApp** — grup (semua) atau DM per peer, typing indicator, tanpa batasan jumlah pesan
- 📁 **File transfer** — streaming chunk 256KB dengan pipelining (8 in-flight), progress bar, verifikasi SHA-256, auto-download
- 🛡️ **Production-grade** — rate limit per IP, origin check, ping/pong keepalive, slow-consumer protection, urutan chunk ketat, graceful shutdown
- 📱 **Mobile-ready** — layout tab di HP, installable PWA manifest

## Struktur

```
fastshare/
├── backend/          # Go server (relay WebSocket + REST + static)
│   ├── main.go       # bootstrap, static SPA, graceful shutdown
│   ├── hub.go        # room/peer registry, presence, distance (haversine)
│   ├── ws.go         # WebSocket protocol + relay + validasi chunk
│   └── handlers.go   # REST (health, rooms) + rate limit + security headers
├── frontend/         # React + Vite
│   └── src/
│       ├── crypto.js # E2E crypto (WebCrypto)
│       ├── api.js    # WS client
│       └── pages/    # Home, Room + Radar/Chat/Transfers
├── test/             # e2e relay test + UI test peer
├── run.bat           # build (kalau perlu) + start
└── build.bat         # force rebuild
```

## Cara jalanin

```bat
run.bat        :: build + start di http://localhost:9000
```

Atau manual:

```bash
# backend
cd backend && go build -o fastshare.exe . && ./fastshare.exe

# frontend (dev mode, hot reload, proxy ke :9000)
cd frontend && npm install && npm run dev     # http://localhost:5173
```

**Cloudflare Tunnel** (sudah dikonfigurasi): `config.yml` mengarah `app.daffadev.my.id → localhost:9000`.
Tinggal jalankan tunnel manager (`~/cloudflared/tunnel.bat`) setelah FastShare hidup.
WebSocket otomatis jalan via tunnel (wss://).

Env vars backend: `PORT` (default 9000), `FASTSHARE_DIST` (default ../frontend/dist).

## Cara pakai

1. Buka aplikasi → **Buat Room Baru** → izinkan lokasi (opsional, buat beacon jarak)
2. Bagikan kode/link ke teman (share code cukup, ga perlu deketan)
3. Teman gabung → muncul di radar + daftar peer
4. **Chat**: ketik di kolom, target "Semua" atau peer tertentu (klik nama)
5. **Kirim file**: tombol 📎 atau drag & drop, progress bar real-time
6. Penerima dapat file otomatis (auto-download < 512MB) + verifikasi SHA-256

## Protokol WebSocket

Semua JSON. Payload `enc`/`encs` = ciphertext AES-GCM (server ga bisa baca):

| Message | Arah | Fungsi |
|---|---|---|
| `hello` | C→S | daftar (nama, publicKey ECDH, koordinat, device) |
| `welcome` / `presence` | S→C | id sendiri + roster peer (dengan jarak) |
| `chat` | C→S | `encs: {peerId: {iv,ct}}` per-penerima (grup = semua peer) |
| `file-meta` | C→S | awal transfer (nama/mime terenkripsi, size/chunks polos) |
| `file-chunk` | C→S | chunk terenkripsi, harus urut (divalidasi server) |
| `file-ack` | C→S | konfirmasi chunk per-seq (progress sender) |
| `file-done` | C→S | penutup stream + SHA-256; receiver balas konfirmasi |
| `typing` | C→S | indikator mengetik |

## Batasan & keamanan

- Maks 1MB per pesan WS, 8 transfer paralel per peer, file maks 8GB (server), 2GB (memory browser/HP)
- Rate limit: 120 msg/s per koneksi WS, 60 req/s per IP untuk REST
- Room auto-hapus saat kosong; **tidak ada storage** — server ga simpan apa pun (privacy by design)
- Kunci ECDH per room disimpan di localStorage browser (identitas device); ganti browser = ganti identitas
- Chunk harus masuk **berurutan** (NextSeq) — spam/garbage chunk ditolak server
- Rekomendasi: jangan expose port 9000 ke internet langsung; selalu lewat Cloudflare Tunnel (HTTPS + WAF)

## Test

```bash
cd test
node e2e.mjs        # 14 assertion: relay, E2E crypto, file integrity, distance
node ui-peer.mjs KODE [keepalive-s]   # peer tiruan buat tes UI
```
