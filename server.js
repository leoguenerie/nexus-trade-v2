/**
 * NEXUS TRADE v4 — Serveur Backend
 * Refonte complete : architecture propre, positions persistantes,
 * polling robuste, gestion d'erreurs centralisee.
 */

const express   = require('express');
const cors      = require('cors');
const crypto    = require('crypto');
const https     = require('https');
const http      = require('http');
const qs        = require('querystring');
const WebSocket = require('ws');
const path      = require('path');
require('dotenv').config();

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });
const PORT   = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ════════════════════════════════════════════════════════════
// CONFIG
// ════════════════════════════════════════════════════════════
let KRAKEN_KEY       = process.env.KRAKEN_KEY    || '';
let KRAKEN_SECRET    = process.env.KRAKEN_SECRET || '';
let DAILY_LOSS_LIMIT = parseFloat(process.env.DAILY_LOSS_LIMIT) || 50;
let MAX_TRADES       = parseInt(process.env.MAX_TRADES) || 20;

// Paires tradees : { display: 'XBT/USD', kraken: 'XBTUSD' (ticker), order: 'XBTEUR' (ordre) }
const PAIRS = [
  { display: 'XBT/USD',  kraken: 'XBTUSD',  order: 'XBTEUR',  minVol: 0.0001 },
  { display: 'ETH/USD',  kraken: 'ETHUSD',  order: 'ETHEUR',  minVol: 0.001  },
  { display: 'SOL/USD',  kraken: 'SOLUSD',  order: 'SOLEUR',  minVol: 0.01   },
  { display: 'XRP/USD',  kraken: 'XRPUSD',  order: 'XRPEUR',  minVol: 1      },
  { display: 'ADA/USD',  kraken: 'ADAUSD',  order: 'ADAEUR',  minVol: 1      },
  { display: 'DOT/USD',  kraken: 'DOTUSD',  order: 'DOTEUR',  minVol: 0.1    },
  { display: 'LINK/USD', kraken: 'LINKUSD', order: 'LINKEUR', minVol: 0.1    },
  { display: 'AVAX/USD', kraken: 'AVAXUSD', order: 'AVAXEUR', minVol: 0.01   },
];

// Map ticker Kraken (reponse API) -> paire display
const TICKER_MAP = {};
PAIRS.forEach(p => { TICKER_MAP[p.kraken] = p.display; });
// Codes Kraken alternatifs (lettres X/Z) vus dans les reponses Ticker
const TICKER_ALIASES = {
  'XXBTZUSD': 'XBT/USD', 'XETHZUSD': 'ETH/USD', 'XXRPZUSD': 'XRP/USD',
  'XDOTZUSD': 'DOT/USD', 'XLTCZUSD': 'LTC/USD',
};
Object.assign(TICKER_MAP, TICKER_ALIASES);
TICKER_MAP['EURUSD'] = 'EUR/USD';

const TICKER_QUERY = PAIRS.map(p => p.kraken).join(',') + ',EURUSD';

// ════════════════════════════════════════════════════════════
// ETAT SERVEUR (memoire — reset si le serveur redemarre)
// ════════════════════════════════════════════════════════════
let dailyLoss       = 0;
let tradeCount      = 0;
let todayDate       = new Date().toDateString();
let latestPrices    = {};   // { 'XBT/USD': {price,ask,bid,vol,high,low,change} }
let frontendClients = [];
let serverPositions = {};   // { 'XBT/USD': {price, volume, time} } — persiste tant que le process tourne
let pollErrors       = 0;

function checkReset() {
  const today = new Date().toDateString();
  if (today !== todayDate) {
    dailyLoss = 0; tradeCount = 0; todayDate = today;
    console.log('[RESET] Compteurs journaliers remis a zero');
  }
}

// ════════════════════════════════════════════════════════════
// KRAKEN — REST helpers
// ════════════════════════════════════════════════════════════
function krakenGet(endpoint) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'api.kraken.com', port: 443,
      path: endpoint, method: 'GET',
      headers: { 'User-Agent': 'NexusTrade/4.0' }
    };
    const req = https.request(options, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const j = JSON.parse(data);
          if (j.error && j.error.length) reject(new Error(j.error[0]));
          else resolve(j.result);
        } catch (e) { reject(e); }
      });
    });
    req.setTimeout(10000, () => { req.destroy(); reject(new Error('Timeout GET')); });
    req.on('error', reject);
    req.end();
  });
}

function krakenPrivate(endpoint, params = {}) {
  return new Promise((resolve, reject) => {
    if (!KRAKEN_KEY || !KRAKEN_SECRET) { reject(new Error('API non configuree')); return; }
    const nonce = Date.now().toString();
    const body  = qs.stringify({ nonce, ...params });
    const hash  = crypto.createHash('sha256').update(nonce + body).digest('binary');
    const hmac  = crypto.createHmac('sha512', Buffer.from(KRAKEN_SECRET, 'base64'));
    const sign  = hmac.update(endpoint + hash, 'binary').digest('base64');
    const options = {
      hostname: 'api.kraken.com', port: 443,
      path: endpoint, method: 'POST',
      headers: {
        'API-Key': KRAKEN_KEY, 'API-Sign': sign,
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'NexusTrade/4.0'
      }
    };
    const req = https.request(options, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const j = JSON.parse(data);
          if (j.error && j.error.length) reject(new Error(j.error[0]));
          else resolve(j.result);
        } catch (e) { reject(e); }
      });
    });
    req.setTimeout(15000, () => { req.destroy(); reject(new Error('Timeout ordre')); });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

// ════════════════════════════════════════════════════════════
// POLLING PRIX — toutes les 5s, avec backoff si erreurs
// ════════════════════════════════════════════════════════════
async function pollPrices() {
  try {
    const result = await krakenGet('/0/public/Ticker?pair=' + TICKER_QUERY);
    pollErrors = 0;
    for (const [key, ticker] of Object.entries(result)) {
      const display = TICKER_MAP[key];
      if (!display) continue;
      const price  = parseFloat(ticker.c[0]);
      const ask    = parseFloat(ticker.a[0]);
      const bid    = parseFloat(ticker.b[0]);
      const vol    = parseFloat(ticker.v[1]);
      const high   = parseFloat(ticker.h[1]);
      const low    = parseFloat(ticker.l[1]);
      const open   = parseFloat(ticker.o[1]);
      const change = (open > 0) ? ((price - open) / open) * 100 : 0;
      latestPrices[display] = { price, ask, bid, vol, high, low, change };
      const payload = JSON.stringify({ type: 'price', pair: display, price, ask, bid, vol, high, low, change });
      frontendClients = frontendClients.filter(c => c.readyState === WebSocket.OPEN);
      frontendClients.forEach(c => { try { c.send(payload); } catch (e) {} });
    }
  } catch (e) {
    pollErrors++;
    console.error(`[PRIX ERROR #${pollErrors}]`, e.message);
  }
  setTimeout(pollPrices, pollErrors > 3 ? 10000 : 5000);
}

// ════════════════════════════════════════════════════════════
// WEBSOCKET FRONTEND
// ════════════════════════════════════════════════════════════
wss.on('connection', (ws) => {
  frontendClients.push(ws);
  console.log(`[WS] Client connecte (${frontendClients.length} total)`);
  if (Object.keys(latestPrices).length) {
    try { ws.send(JSON.stringify({ type: 'snapshot', prices: latestPrices })); } catch (e) {}
  }
  ws.on('close', () => {
    frontendClients = frontendClients.filter(c => c !== ws);
  });
  ws.on('error', () => {
    frontendClients = frontendClients.filter(c => c !== ws);
  });
});

setInterval(() => {
  frontendClients = frontendClients.filter(c => c.readyState === WebSocket.OPEN);
  frontendClients.forEach(c => { try { c.send(JSON.stringify({ type: 'ping' })); } catch (e) {} });
}, 30000);

// ════════════════════════════════════════════════════════════
// ROUTES
// ════════════════════════════════════════════════════════════
app.get('/api/health', (req, res) => {
  res.json({
    ok: true, version: '4.0',
    pairsActive: Object.keys(latestPrices).length,
    apiConfigured: KRAKEN_KEY.length > 0,
    tradeCount, dailyLoss, pollErrors,
    positionsCount: Object.keys(serverPositions).length,
  });
});

app.post('/api/config', (req, res) => {
  const { key, secret, maxLoss, maxTrades } = req.body;
  if (!key || !secret) return res.status(400).json({ error: 'Cles manquantes' });
  KRAKEN_KEY = key; KRAKEN_SECRET = secret;
  if (maxLoss)   DAILY_LOSS_LIMIT = parseFloat(maxLoss);
  if (maxTrades) MAX_TRADES       = parseInt(maxTrades);
  console.log(`[CONFIG] API configuree. Perte max: $${DAILY_LOSS_LIMIT}`);
  res.json({ ok: true });
});

app.get('/api/prices', (req, res) => res.json(latestPrices));
app.get('/api/pairs', (req, res) => res.json(PAIRS));

app.get('/api/ohlc/:pair/:interval', async (req, res) => {
  try {
    const result  = await krakenGet(`/0/public/OHLC?pair=${req.params.pair}&interval=${req.params.interval}`);
    const key     = Object.keys(result).find(k => k !== 'last');
    const candles = result[key].map(c => ({
      time: c[0], open: parseFloat(c[1]), high: parseFloat(c[2]),
      low: parseFloat(c[3]), close: parseFloat(c[4]), volume: parseFloat(c[6])
    }));
    res.json({ pair: req.params.pair, interval: req.params.interval, candles });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/balance', async (req, res) => {
  try {
    const result = await krakenPrivate('/0/private/Balance');
    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/order', async (req, res) => {
  checkReset();
  try {
    if (!KRAKEN_KEY) return res.status(401).json({ error: 'API non configuree' });
    if (Math.abs(dailyLoss) >= DAILY_LOSS_LIMIT)
      return res.status(403).json({ error: `Perte max $${DAILY_LOSS_LIMIT} atteinte` });
    if (tradeCount >= MAX_TRADES)
      return res.status(403).json({ error: `Max ${MAX_TRADES} trades/jour atteint` });
    const { pair, type, ordertype, volume } = req.body;
    const result = await krakenPrivate('/0/private/AddOrder', { pair, type, ordertype, volume, validate: 'false' });
    tradeCount++;
    console.log(`[ORDER] ${type} ${volume} ${pair} | Trade #${tradeCount}`);
    res.json({ ok: true, txid: result.txid, descr: result.descr, tradeCount });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/cancel-all', async (req, res) => {
  try {
    const result = await krakenPrivate('/0/private/CancelAll');
    console.log('[URGENCE] Tous les ordres annules');
    res.json({ ok: true, result });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/status', (req, res) => {
  checkReset();
  res.json({ dailyLoss, DAILY_LOSS_LIMIT, tradeCount, MAX_TRADES });
});

app.post('/api/pnl', (req, res) => {
  const { pnl } = req.body;
  if (pnl < 0) dailyLoss += pnl;
  res.json({ dailyLoss });
});

// ── Positions persistantes cote serveur ────────────────────
// Permet au bot de retrouver ses positions ouvertes (prix d'achat reel)
// meme si le navigateur est ferme/recharge ou si le process redemarre
// sans avoir vendu (tant que le process Node tourne, c'est garde en memoire).
app.get('/api/positions', (req, res) => res.json(serverPositions));

app.post('/api/positions', (req, res) => {
  const { pair, price, volume, action } = req.body;
  if (!pair) return res.status(400).json({ error: 'pair manquant' });
  if (action === 'open') {
    serverPositions[pair] = { price, volume, time: Date.now() };
    console.log(`[POSITION] Ouverte: ${pair} @ ${price} vol=${volume}`);
  } else if (action === 'close') {
    delete serverPositions[pair];
    console.log(`[POSITION] Fermee: ${pair}`);
  }
  res.json({ ok: true, positions: serverPositions });
});

// ════════════════════════════════════════════════════════════
// DEMARRAGE
// ════════════════════════════════════════════════════════════
server.listen(PORT, () => {
  console.log('');
  console.log('⚡ ══════════════════════════════════════════');
  console.log('   NEXUS TRADE v4 — Serveur demarre');
  console.log(`   Port      →  ${PORT}`);
  console.log(`   Paires    →  ${PAIRS.length} cryptos surveillees`);
  console.log(`   API       →  ${KRAKEN_KEY ? 'configuree' : 'NON configuree'}`);
  console.log('⚡ ══════════════════════════════════════════');
  console.log('');
  pollPrices();
});
