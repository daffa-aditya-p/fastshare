// Listener peer — nunggu chat/file dari browser, decrypt, print.
import { genKeyPair, exportPub, importPub, deriveSessionKey, decJSON } from '../frontend/src/crypto.js'

const code = (process.argv[2] || 'B5HVDG').toUpperCase()
const kp = await genKeyPair()
const ws = new WebSocket(`ws://localhost:9000/ws?code=${code}`)

ws.onopen = async () => {
  const pub = await exportPub(kp)
  ws.send(JSON.stringify({ type: 'hello', name: 'Listener Bob 📡', publicKey: pub, device: 'HP', coords: { lat: -6.2, lng: 106.82 } }))
}
ws.onmessage = async (e) => {
  const m = JSON.parse(e.data)
  if (m.type === 'welcome' || m.type === 'presence') {
    const me = m.id || ''
    const peer = (m.peers || []).find((p) => p.id !== me)
    if (peer) {
      console.log('👀 browser peer:', peer.name, peer.id)
      globalThis.__peer = peer
      globalThis.__key = await deriveSessionKey(kp.privateKey, await importPub(peer.publicKey), code)
      console.log('🔑 listener siap (key derived)')
    }
  }
  if (m.type === 'chat' && globalThis.__key) {
    try {
      const obj = await decJSON(globalThis.__key, m.enc)
      console.log('💬 CHAT DARI BROWSER:', obj.text)
    } catch (err) { console.log('💬 chat decrypt FAIL:', err.message) }
  }
  if (m.type === 'file-meta') console.log('📄 file-meta masuk:', m.transferID, m.size, 'bytes,', m.chunks, 'chunks')
  if (m.type === 'file-chunk') console.log('  chunk', m.seq, 'diterima')
  if (m.type === 'file-done') console.log('✅ file-done:', m.transferID, m.ok)
}
ws.onerror = () => { console.log('WS ERROR'); process.exit(1) }
setTimeout(() => { ws.close(); process.exit(0) }, 90000)
