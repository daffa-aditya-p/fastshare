// UI test peer — join room yang dibuka browser, kirim chat + file encrypted.
import { genKeyPair, exportPub, importPub, deriveSessionKey, encJSON, encBytes, sha256Hex } from '../frontend/src/crypto.js'

const code = (process.argv[2] || 'B5HVDG').toUpperCase()
const CHUNK = 256 * 1024

const ws = new WebSocket(`ws://localhost:9000/ws?code=${code}`)
const pending = {}
const waiters = {}
const push = (kind, msg) => {
  const w = (waiters[kind] || []).shift()
  if (w) { clearTimeout(w.t); w.res(msg) } else { (pending[kind] ||= []).push(msg) }
}
const next = (kind) => new Promise((res, rej) => {
  const q = pending[kind] || []
  if (q.length) return res(q.shift())
  const t = setTimeout(() => rej(new Error('timeout ' + kind)), 10000)
  ;(waiters[kind] ||= []).push({ res, rej, t })
})
ws.onmessage = (e) => { const m = JSON.parse(e.data); push(m.type, m) }

ws.onopen = async () => {
  const kp = await genKeyPair()
  const pub = await exportPub(kp)
  ws.send(JSON.stringify({ type: 'hello', name: 'HP Bob 📱', publicKey: pub, device: 'HP', coords: { lat: -6.2, lng: 106.82 } }))
  const welcome = await next('welcome')
  const peer = welcome.peers.filter((p) => p.id !== welcome.id)[0]
  if (!peer) { console.log('NO PEER IN ROOM'); process.exit(1) }
  console.log('peer found:', peer.name, peer.id)

  const key = await deriveSessionKey(kp.privateKey, await importPub(peer.publicKey), code)

  // typing + chat
  ws.send(JSON.stringify({ type: 'typing', to: '*' }))
  await new Promise((r) => setTimeout(r, 400))
  const enc = await encJSON(key, { t: 'msg', text: 'Halo dari HP Bob! 👋 Ini pesan terenkripsi E2E, server ga bisa baca. Aku kirim file juga ya!' })
  ws.send(JSON.stringify({ type: 'chat', encs: { [peer.id]: enc } }))
  console.log('chat sent')

  // file: dari-hp.txt ~512KB (2 chunks)
  const content = 'FastShare E2E file test dari HP Bob 📱\n'.repeat(20000)
  const data = new TextEncoder().encode(content)
  const tid = 'ui-' + Math.random().toString(36).slice(2, 10)
  const chunks = Math.ceil(data.length / CHUNK)
  const hash = await sha256Hex(data)
  const meta = await encJSON(key, { name: 'dari-hp.txt', mime: 'text/plain' })
  ws.send(JSON.stringify({ type: 'file-meta', to: peer.id, transferID: tid, size: data.length, chunks, enc: meta }))

  for (let i = 0; i < chunks; i++) {
    const slice = data.slice(i * CHUNK, Math.min((i + 1) * CHUNK, data.length))
    const iv = crypto.getRandomValues(new Uint8Array(12))
    const encC = await encBytes(key, slice, iv)
    ws.send(JSON.stringify({ type: 'file-chunk', to: peer.id, transferID: tid, seq: i, enc: encC }))
  }
  ws.send(JSON.stringify({ type: 'file-done', to: peer.id, transferID: tid, ok: true, hash }))
  console.log('file sent:', content.length, 'bytes,', chunks, 'chunks')

  // tunggu konfirmasi dari browser
  try {
    const done = await next('file-done')
    console.log('browser confirmed:', done.ok ? '✅ OK' : '❌ FAIL')
  } catch { console.log('no confirmation (browser mungkin ga auto-reply, normal)') }
  const keep = parseInt(process.argv[3] || '0', 10)
  if (keep > 0) {
    console.log('keep-alive', keep, 's...')
    await new Promise((r) => setTimeout(r, keep * 1000))
  }
  ws.close()
  process.exit(0)
}
ws.onerror = () => { console.log('WS ERROR'); process.exit(1) }
