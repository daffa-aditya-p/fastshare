// E2E relay test — 2 Node WebSocket clients, full encrypted chat + file transfer.
// Reuses the same crypto module the browser uses (frontend/src/crypto.js).
import { createHash } from 'node:crypto'
import {
  genKeyPair, exportPub, importPub, deriveSessionKey,
  encJSON, decJSON, encBytes, decBytes, sha256Hex,
} from '../frontend/src/crypto.js'

const BASE = 'http://localhost:9000'
const code = 'E2E' + Math.random().toString(36).slice(2, 6).toUpperCase()
const CHUNK = 256 * 1024

let passed = 0
let failed = 0
function assert(cond, label) {
  if (cond) { passed++; console.log('  ✓', label) }
  else { failed++; console.log('  ✗ FAIL:', label) }
}

function connect(cid) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://localhost:9000/ws?code=${code}`)
    const pending = {}
    const waiters = {}
    const push = (kind, msg) => {
      const ws2 = waiters[kind] || []
      for (let i = 0; i < ws2.length; i++) {
        const w = ws2[i]
        if (w.pred && !w.pred(msg)) continue
        ws2.splice(i, 1)
        clearTimeout(w.t)
        w.res(msg)
        return
      }
      ;(pending[kind] ||= []).push(msg)
    }
    ws.onopen = () => resolve({
      ws,
      send: (o) => ws.send(JSON.stringify(o)),
      next: (kind) => new Promise((res, rej) => {
        const q = pending[kind] || []
        if (q.length) { res(q.shift()); return }
        const t = setTimeout(() => rej(new Error('timeout ' + kind)), 10000)
        ;(waiters[kind] ||= []).push({ res, rej, t, pred: null })
      }),
      // nextWhere: tunggu message yg memenuhi predikat (skip yg ga cocok)
      nextWhere: (kind, pred) => new Promise((res, rej) => {
        const q = pending[kind] || []
        const i = q.findIndex(pred)
        if (i >= 0) { res(q.splice(i, 1)[0]); return }
        const t = setTimeout(() => rej(new Error('timeout ' + kind)), 10000)
        ;(waiters[kind] ||= []).push({ res, rej, t, pred })
      }),
    })
    ws.onmessage = (e) => {
      const m = JSON.parse(e.data)
      push(m.type, m)
    }
    ws.onerror = () => reject(new Error('ws error ' + cid))
  })
}

async function main() {
  console.log(`\n== FastShare E2E test · room ${code} ==\n`)

  // REST
  const r0 = await fetch(`${BASE}/api/health`)
  const health = await r0.json()
  assert(health.ok === true, 'GET /api/health ok')
  const r1 = await fetch(`${BASE}/api/rooms`, { method: 'POST', body: JSON.stringify({ code }) })
  const room = await r1.json()
  assert(r1.status === 200 && room.code === code, 'POST /api/rooms ok')

  // peers — deterministic: hello A dulu, baru B
  const A = await connect('A')
  const kpA = await genKeyPair()
  const pubA = await exportPub(kpA)
  A.send({ type: 'hello', name: 'Alice', publicKey: pubA, device: 'Laptop', coords: { lat: -6.2, lng: 106.8 } })
  const wA = await A.next('welcome')
  assert(wA.id && wA.code === code, 'A welcome + id')

  const B = await connect('B')
  const kpB = await genKeyPair()
  const pubB = await exportPub(kpB)
  B.send({ type: 'hello', name: 'Bob', publicKey: pubB, device: 'HP', coords: { lat: -6.21, lng: 106.81 } })
  const wB = await B.next('welcome')
  const bobFromB = wB.peers.filter((p) => p.id !== wB.id)
  assert(bobFromB.length === 1 && bobFromB[0].name === 'Alice', 'B sees Alice in roster')

  // A nangkep presence yg berisi Bob (skip presence awal dari hello A sendiri)
  const presA = await A.nextWhere('presence', (m) => m.peers.some((p) => p.id !== wA.id))
  const rosterA = presA.peers.filter((p) => p.id !== wA.id)
  assert(rosterA.length === 1 && rosterA[0].name === 'Bob', 'A sees Bob via presence')
  const bob = rosterA[0]
  const dist = bob.distance
  assert(dist != null && dist > 0.5 && dist < 5, `distance computed (${dist?.toFixed(2)} km)`)

  // roster B via presence-nya sendiri
  await B.next('presence')
  const bob2 = wB.peers.find((p) => p.id !== wB.id)

  // encrypted chat A -> B
  const keyAB = await deriveSessionKey(kpA.privateKey, await importPub(pubB), code)
  const keyBA = await deriveSessionKey(kpB.privateKey, await importPub(pubA), code)
  const enc = await encJSON(keyAB, { t: 'msg', text: 'halo bob! pesan rahasia 🔐' })
  A.send({ type: 'chat', encs: { [bob.id]: enc } })
  const chatB = await B.next('chat')
  const dec = await decJSON(keyBA, chatB.enc)
  assert(dec.t === 'msg' && dec.text === 'halo bob! pesan rahasia 🔐', 'chat decrypted by Bob')
  assert(chatB.fromName === 'Alice', 'chat fromName relayed')

  // typing relay
  A.send({ type: 'typing', to: '*' })
  const typ = await B.next('typing')
  assert(typ.from === wA.id, 'typing relayed')

  // file transfer A -> B (3 chunks)
  const tid = 'tf-' + Math.random().toString(36).slice(2, 10)
  const fsize = 700000
  const chunks = Math.ceil(fsize / CHUNK)
  const metaEnc = await encJSON(keyAB, { name: 'cat-photo.jpg', mime: 'image/jpeg' })
  A.send({ type: 'file-meta', to: bob.id, transferID: tid, size: fsize, chunks, enc: metaEnc })
  const metaB = await B.next('file-meta')
  const metaDec = await decJSON(keyBA, metaB.enc)
  assert(metaDec.name === 'cat-photo.jpg' && metaB.size === fsize, 'file-meta decrypted')

  // stream chunks with hash
  const data = Buffer.alloc(fsize)
  for (let i = 0; i < fsize; i++) data[i] = (i * 31) % 256
  const hash = await sha256Hex(new Uint8Array(data))
  const recv = Buffer.alloc(fsize)
  const t0 = Date.now()

  const chunkP = (async () => {
    for (let i = 0; i < chunks; i++) {
      const m = await B.next('file-chunk')
      const bytes = await decBytes(keyBA, m.enc.iv, m.enc.ct)
      Buffer.from(bytes).copy(recv, m.seq * CHUNK)
      B.send({ type: 'file-ack', to: wA.id, transferID: tid, seq: m.seq })
    }
  })()
  for (let i = 0; i < chunks; i++) {
    const slice = data.subarray(i * CHUNK, Math.min((i + 1) * CHUNK, fsize))
    const iv = crypto.getRandomValues(new Uint8Array(12))
    const encC = await encBytes(keyAB, new Uint8Array(slice), iv)
    A.send({ type: 'file-chunk', to: bob.id, transferID: tid, seq: i, enc: encC })
  }
  await chunkP
  const ms = Date.now() - t0
  const mbps = (fsize / 1024 / 1024) / (ms / 1000)
  console.log(`  📈 throughput: ${fsize / 1024 / 1024} MB in ${ms} ms → ${mbps.toFixed(1)} MB/s`)

  A.send({ type: 'file-done', to: bob.id, transferID: tid, ok: true, hash })
  const doneB = await B.next('file-done')
  assert(doneB.ok === true && doneB.hash === hash, 'file-done + hash received')

  const recvHash = await sha256Hex(new Uint8Array(recv))
  assert(recvHash === hash, 'file content intact (SHA-256 match)')

  // receiver confirms
  B.send({ type: 'file-done', to: wA.id, transferID: tid, ok: true })
  const doneA = await A.next('file-done')
  assert(doneA.ok === true, 'sender got confirmation')

  // server never saw plaintext: chat text not in any relayed field — assert enc.ct != plaintext
  assert(!JSON.stringify(chatB.enc).includes('halo bob'), 'ciphertext does not contain plaintext')

  A.ws.close(); B.ws.close()
  await new Promise((r) => setTimeout(r, 300))

  console.log(`\n== RESULT: ${passed} passed, ${failed} failed ==`)
  process.exit(failed ? 1 : 0)
}

main().catch((e) => { console.error('FATAL:', e.message); process.exit(1) })
