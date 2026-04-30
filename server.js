/**
 * NEXUS TRADE v2 — Serveur Backend
 * - Prix en temps réel via WebSocket Kraken (public, sans clé)
 * - Ordres réels via REST API Kraken (privé, avec clé)
 * - CORS activé pour le frontend local
 */

const express = require('express');
const cors    = require('cors');
const crypto  = require('crypto');
const https   = require('https');
const http    = require('http');
const qs      = require('querystring');
const WebSocket = require('ws');
const path    = require('path');
require('dotenv').config();

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server }); // WebSocket pour le frontend
const PORT   = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── Config ────────────────────────────────────────────────────
let KRAKEN_KEY    = process.env.KRAKEN_KEY    || '';
let KRAKEN_SECRET = process.env.KRAKEN_SECRET || '';
let DAILY_LOSS_LIMIT = parseFloat(process.env.DAILY_LOSS_LIMIT) || 50;
let MAX_TRADES       = parseInt(process.env.MAX_TRADES)         || 5;

// ── État serveur ──────────────────────────────────────────────
let dailyLoss  = 0;
let tradeCount = 0;
let todayDate  = new Date().toDateString();
let latestPrices = {};       // stocke les derniers prix reçus
let frontendClients = [];    // clients WebSocket connectés au frontend

// ── Reset quotidien ───────────────────────────────────────────
function checkReset() {
  const today = new Date().toDateString();
  if (today !== todayDate) {
    dailyLoss = 0; tradeCount = 0; todayDate = today;
    console.log('[RESET] Compteurs remis à zéro');
  }
}

// ════════════════════════════════════════════════════════════
// KRAKEN REST POLLING — Prix toutes les 5s (compatible Railway)
// ════════════════════════════════════════════════════════════
const PAIRS = ['XBTUSD', 'ETHUSD', 'SOLUSD', 'XRPUSD', 'ADAUSD', 'DOTUSD', 'LINKUSD', 'AVAXUSD', 'EURUSD'];
const PAIR_MAP = {'XXBTZUSD':'XBT/USD','XETHZUSD':'ETH/USD','SOLUSD':'SOL/USD','XXRPZUSD':'XRP/USD','ADAUSD':'ADA/USD','DOTUSD':'DOT/USD','XDOTZUSD':'DOT/USD','LINKUSD':'LINK/USD','AVAXUSD':'AVAX/USD','ZEURZUSD':'EUR/USD','EURUSD':'EUR/USD'};

async function pollKrakenPrices() {
  try {
    const result = await krakenGet('/0/public/Ticker?pair=' + PAIRS.join(','));
    for (const [key, ticker] of Object.entries(result)) {
      const pair = PAIR_MAP[key] || key;
      if (!pair.includes('/')) continue;
      const price  = parseFloat(ticker.c[0]);
      const ask    = parseFloat(ticker.a[0]);
      const bid    = parseFloat(ticker.b[0]);
      const vol    = parseFloat(ticker.v[1]);
      const high   = parseFloat(ticker.h[1]);
      const low    = parseFloat(ticker.l[1]);
      const open   = parseFloat(ticker.o[1]);
      const change = open > 0 ? ((price - open) / open) * 100 : 0;
      latestPrices[pair] = { price, ask, bid, vol, high, low, change };
      const payload = JSON.stringify({ type: 'price', pair, price, ask, bid, vol, high, low, change });
      frontendClients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) client.send(payload);
      });
    }
    console.log('[PRIX] BTC/USD =', latestPrices['XBT/USD']?.price);
  } catch(e) {
    console.error('[PRIX ERROR]', e.message);
  }
  setTimeout(pollKrakenPrices, 5000);
}

function connectKrakenWebSocket() {
  console.log('[WS KRAKEN] Connecte - Demarrage polling prix...');
  pollKrakenPrices();
}

// ════════════════════════════════════════════════════════════
// KRAKEN OHLC — Bougies pour le graphique
// ════════════════════════════════════════════════════════════
function krakenGet(endpoint) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'api.kraken.com', port: 443,
      path: endpoint, method: 'GET',
      headers: { 'User-Agent': 'NexusTrade/2.0' }
    };
    const req = https.request(options, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const j = JSON.parse(data);
          if (j.error && j.error.length) reject(new Error(j.error[0]));
          else resolve(j.result);
        } catch(e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

// ════════════════════════════════════════════════════════════
// KRAKEN REST PRIVÉ — Signature HMAC pour les ordres
// ════════════════════════════════════════════════════════════
function krakenSign(path, params, secret) {
  const nonce   = Date.now().toString();
  const body    = qs.stringify({ nonce, ...params });
  const hash    = crypto.createHash('sha256').update(nonce + body).digest('binary');
  const hmac    = crypto.createHmac('sha512', Buffer.from(secret, 'base64'));
  const sign    = hmac.update(path + hash, 'binary').digest('base64');
  return { body, sign, nonce };
}

function krakenPrivate(endpoint, params = {}) {
  return new Promise((resolve, reject) => {
    const { body, sign } = krakenSign(endpoint, params, KRAKEN_SECRET);
    const options = {
      hostname: 'api.kraken.com', port: 443,
      path: endpoint, method: 'POST',
      headers: {
        'API-Key': KRAKEN_KEY, 'API-Sign': sign,
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'NexusTrade/2.0'
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
        } catch(e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

// ════════════════════════════════════════════════════════════
// WEBSOCKET FRONTEND — Le site se connecte ici
// ════════════════════════════════════════════════════════════
wss.on('connection', (ws) => {
  frontendClients.push(ws);
  console.log(`[WS FRONTEND] Client connecté (${frontendClients.length} total)`);

  // Envoie les derniers prix immédiatement
  if (Object.keys(latestPrices).length) {
    ws.send(JSON.stringify({ type: 'snapshot', prices: latestPrices }));
  }

  ws.on('close', () => {
    frontendClients = frontendClients.filter(c => c !== ws);
    console.log(`[WS FRONTEND] Client déconnecté (${frontendClients.length} restants)`);
  });
});

// ════════════════════════════════════════════════════════════
// ROUTES REST
// ════════════════════════════════════════════════════════════

// Santé
app.get('/api/health', (req, res) => {
  res.json({
    ok: true, version: '2.0',
    krakenWs: Object.keys(latestPrices).length > 0,
    apiConfigured: KRAKEN_KEY.length > 0,
    prices: latestPrices,
  });
});

// Config clés API
app.post('/api/config', (req, res) => {
  const { key, secret, maxLoss, maxTrades } = req.body;
  if (!key || !secret) return res.status(400).json({ error: 'Clés manquantes' });
  KRAKEN_KEY = key; KRAKEN_SECRET = secret;
  if (maxLoss)   DAILY_LOSS_LIMIT = parseFloat(maxLoss);
  if (maxTrades) MAX_TRADES       = parseInt(maxTrades);
  console.log(`[CONFIG] Clés API configurées. Perte max: $${DAILY_LOSS_LIMIT}`);
  res.json({ ok: true });
});

// Prix actuels
app.get('/api/prices', (req, res) => res.json(latestPrices));

// Sauvegarder/recuperer positions
app.post('/api/positions', (req, res) => {
  const { pair, price, volume, action } = req.body;
  if(action === 'open'){
    serverPositions[pair] = { price, volume, time: Date.now() };
    console.log('[POSITION] Ouverte:', pair, price, volume);
  } else if(action === 'close'){
    delete serverPositions[pair];
    console.log('[POSITION] Fermee:', pair);
  }
  res.json({ ok: true, positions: serverPositions });
});

app.get('/api/positions', (req, res) => res.json(serverPositions));

// OHLC (bougies graphique) — vraies données Kraken
app.get('/api/ohlc/:pair/:interval', async (req, res) => {
  try {
    const pair     = req.params.pair;     // ex: XBTUSD
    const interval = req.params.interval; // ex: 15
    const result   = await krakenGet(`/0/public/OHLC?pair=${pair}&interval=${interval}`);
    const key      = Object.keys(result).find(k => k !== 'last');
    const candles  = result[key].map(c => ({
      time: c[0], open: parseFloat(c[1]), high: parseFloat(c[2]),
      low: parseFloat(c[3]), close: parseFloat(c[4]), volume: parseFloat(c[6])
    }));
    res.json({ pair, interval, candles });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Solde wallet
app.get('/api/balance', async (req, res) => {
  try {
    if (!KRAKEN_KEY) return res.status(401).json({ error: 'API non configurée' });
    const result = await krakenPrivate('/0/private/Balance');
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Passer un ordre
app.post('/api/order', async (req, res) => {
  checkReset();
  try {
    if (!KRAKEN_KEY) return res.status(401).json({ error: 'API non configurée' });
    if (Math.abs(dailyLoss) >= DAILY_LOSS_LIMIT)
      return res.status(403).json({ error: `🛡️ Perte max journalière $${DAILY_LOSS_LIMIT} atteinte` });
    if (tradeCount >= MAX_TRADES)
      return res.status(403).json({ error: `🛡️ Max ${MAX_TRADES} trades/jour atteint` });

    const { pair, type, ordertype, volume, validate } = req.body;
    const result = await krakenPrivate('/0/private/AddOrder', {
      pair, type, ordertype, volume,
      validate: validate ? 'true' : 'false'
    });
    tradeCount++;
    console.log(`[ORDER] ${type} ${volume} ${pair} | Trade #${tradeCount}`);
    res.json({ ok: true, txid: result.txid, descr: result.descr, tradeCount });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Annuler tous les ordres (urgence)
app.post('/api/cancel-all', async (req, res) => {
  try {
    if (!KRAKEN_KEY) return res.status(401).json({ error: 'API non configurée' });
    const result = await krakenPrivate('/0/private/CancelAll');
    console.log('[URGENCE] Tous les ordres annulés');
    res.json({ ok: true, result });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Statut garde-fous
app.get('/api/status', (req, res) => {
  checkReset();
  res.json({ dailyLoss, DAILY_LOSS_LIMIT, tradeCount, MAX_TRADES, safeToTrade: Math.abs(dailyLoss) < DAILY_LOSS_LIMIT && tradeCount < MAX_TRADES });
});

// Rapport P&L
app.post('/api/pnl', (req, res) => {
  const { pnl } = req.body;
  if (pnl < 0) dailyLoss += pnl;
  res.json({ dailyLoss, safeToTrade: Math.abs(dailyLoss) < DAILY_LOSS_LIMIT });
});

// ════════════════════════════════════════════════════════════
// DÉMARRAGE
// ════════════════════════════════════════════════════════════
server.listen(PORT, () => {
  console.log('');
  console.log('⚡ ══════════════════════════════════════════');
  console.log('   NEXUS TRADE v2 — Serveur démarré');
  console.log(`   Site web  →  http://localhost:${PORT}`);
  console.log(`   WebSocket →  ws://localhost:${PORT}`);
  console.log('   Kraken WebSocket public → connexion...');
  console.log('⚡ ══════════════════════════════════════════');
  console.log('');
  connectKrakenWebSocket(); // Connexion aux prix temps réel
});
