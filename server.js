/**
 * NEXUS TRADE v5 — Backend complet
 * Kraken Bot + Solana Sniper + Risk Manager + DexScreener + Learning
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

// Solana — chargement conditionnel (graceful si pas installe)
let solanaWeb3 = null, bs58 = null;
try { solanaWeb3 = require('@solana/web3.js'); bs58 = require('bs58'); console.log('[SOLANA] web3.js charge'); }
catch(e) { console.log('[SOLANA] Non disponible — features desactivees'); }

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });
const PORT   = process.env.PORT || 3000;
app.use(cors()); app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ════════════════════════════════════════════════════════════
// CONFIG ENVIRONNEMENT
// ════════════════════════════════════════════════════════════
let KRAKEN_KEY    = process.env.KRAKEN_KEY    || '';
let KRAKEN_SECRET = process.env.KRAKEN_SECRET || '';
const SOLANA_PRIVATE_KEY = process.env.SOLANA_PRIVATE_KEY || '';
const SOLANA_RPC         = process.env.SOLANA_RPC || 'https://api.mainnet-beta.solana.com';
const SMART_WALLETS_ENV  = process.env.SMART_WALLETS || '';
const DEV_BLACKLIST_ENV  = process.env.DEV_BLACKLIST  || '';

const SMART_MONEY_WALLETS = SMART_WALLETS_ENV.split(',').filter(Boolean);
const DEV_BLACKLIST       = new Set(DEV_BLACKLIST_ENV.split(',').filter(Boolean));

// ════════════════════════════════════════════════════════════
// PAIRES KRAKEN
// ════════════════════════════════════════════════════════════
const KRAKEN_PAIRS = [
  { display:'XBT/USD',  kraken:'XBTUSD',  order:'XBTEUR',  minVol:0.0001, dp:5 },
  { display:'ETH/USD',  kraken:'ETHUSD',  order:'ETHEUR',  minVol:0.001,  dp:5 },
  { display:'SOL/USD',  kraken:'SOLUSD',  order:'SOLEUR',  minVol:0.01,   dp:4 },
  { display:'XRP/USD',  kraken:'XRPUSD',  order:'XRPEUR',  minVol:1,      dp:2 },
  { display:'ADA/USD',  kraken:'ADAUSD',  order:'ADAEUR',  minVol:1,      dp:2 },
  { display:'DOT/USD',  kraken:'DOTUSD',  order:'DOTEUR',  minVol:0.1,    dp:3 },
  { display:'LINK/USD', kraken:'LINKUSD', order:'LINKEUR', minVol:0.1,    dp:3 },
  { display:'AVAX/USD', kraken:'AVAXUSD', order:'AVAXEUR', minVol:0.01,   dp:4 },
  { display:'LTC/USD',  kraken:'LTCUSD',  order:'LTCEUR',  minVol:0.01,   dp:4 },
  { display:'BCH/USD',  kraken:'BCHUSD',  order:'BCHEUR',  minVol:0.001,  dp:5 },
  { display:'ATOM/USD', kraken:'ATOMUSD', order:'ATOMEUR', minVol:0.1,    dp:3 },
  { display:'NEAR/USD', kraken:'NEARUSD', order:'NEAREUR', minVol:0.5,    dp:2 },
  { display:'UNI/USD',  kraken:'UNIUSD',  order:'UNIEUR',  minVol:0.1,    dp:3 },
  { display:'ETC/USD',  kraken:'ETCUSD',  order:'ETCEUR',  minVol:0.05,   dp:3 },
  { display:'XLM/USD',  kraken:'XLMUSD',  order:'XLMEUR',  minVol:5,      dp:1 },
  { display:'DOGE/USD', kraken:'XDGUSD',  order:'XDGEUR',  minVol:5,      dp:1 },
];
const TICKER_MAP = {};
KRAKEN_PAIRS.forEach(p => TICKER_MAP[p.kraken] = p.display);
Object.assign(TICKER_MAP, {
  'XXBTZUSD':'XBT/USD','XETHZUSD':'ETH/USD','XXRPZUSD':'XRP/USD',
  'XDOTZUSD':'DOT/USD','XLTCZUSD':'LTC/USD','XETCZUSD':'ETC/USD',
  'XXLMZUSD':'XLM/USD','XXDGZUSD':'DOGE/USD','EURUSD':'EUR/USD',
});
const TICKER_QUERY = KRAKEN_PAIRS.map(p=>p.kraken).join(',') + ',EURUSD';

// ════════════════════════════════════════════════════════════
// ETAT GLOBAL
// ════════════════════════════════════════════════════════════
let latestPrices     = {};
let frontendClients  = [];
let krakenPositions  = {};   // { pair: {price,volume,time,peak} }
let solanaPositions  = {};   // { mint: {amountUsd,solSpent,buyPrice,time,symbol,score} }
let discoveredTokens = [];   // tokens DexScreener scores
let smartActivity    = [];   // mouvements smart money
let learningData     = [];   // historique decisions bot
let pollErrors       = 0;
let dailyLoss        = 0;
let tradeCount       = 0;
let todayDate        = new Date().toDateString();
let sniperAuto       = false; // mode snipe auto actif
let sniperMinScore   = 75;    // score minimum pour snipe auto
let sniperAmountUsd  = 5;     // mise par token ($)

let riskConfig = {
  maxDailyLoss:    parseFloat(process.env.MAX_DAILY_LOSS)    || 50,
  maxPerTrade:     parseFloat(process.env.MAX_PER_TRADE)     || 20,
  maxPerMemecoin:  parseFloat(process.env.MAX_PER_MEMECOIN)  || 10,
  maxOpenPositions:parseInt(process.env.MAX_POSITIONS)        || 5,
  maxKrakenTrades: parseInt(process.env.MAX_TRADES)           || 20,
};

// Solana wallet
let solanaKeypair   = null;
let solanaConn      = null;

function initSolana() {
  if (!solanaWeb3 || !bs58 || !SOLANA_PRIVATE_KEY) return;
  try {
    const decoded = bs58.decode(SOLANA_PRIVATE_KEY);
    solanaKeypair = solanaWeb3.Keypair.fromSecretKey(decoded);
    solanaConn    = new solanaWeb3.Connection(SOLANA_RPC, 'confirmed');
    console.log(`[SOLANA] Wallet: ${solanaKeypair.publicKey.toBase58()}`);
  } catch(e) { console.error('[SOLANA] Erreur init:', e.message); }
}

function checkReset() {
  const today = new Date().toDateString();
  if (today !== todayDate) { dailyLoss=0; tradeCount=0; todayDate=today; console.log('[RESET] Quotidien'); }
}

// ════════════════════════════════════════════════════════════
// RISK MANAGER
// ════════════════════════════════════════════════════════════
function checkRisk(amountUsd=0, type='kraken') {
  checkReset();
  const openCount = Object.keys(krakenPositions).length + Object.keys(solanaPositions).length;
  if (Math.abs(dailyLoss) >= riskConfig.maxDailyLoss)
    return { ok:false, reason:`Perte max journalière atteinte ($${riskConfig.maxDailyLoss})` };
  if (openCount >= riskConfig.maxOpenPositions)
    return { ok:false, reason:`Positions max atteintes (${riskConfig.maxOpenPositions})` };
  if (type==='memecoin' && amountUsd > riskConfig.maxPerMemecoin)
    return { ok:false, reason:`Max par memecoin: $${riskConfig.maxPerMemecoin}` };
  if (type==='kraken' && amountUsd > riskConfig.maxPerTrade)
    return { ok:false, reason:`Max par trade Kraken: $${riskConfig.maxPerTrade}` };
  return { ok:true };
}

// ════════════════════════════════════════════════════════════
// HELPERS HTTP
// ════════════════════════════════════════════════════════════
function httpGet(url, timeout=10000) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const req = lib.get(url, { headers:{'User-Agent':'NexusTrade/5.0','Accept':'application/json'} }, res => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { reject(e); } });
    });
    req.setTimeout(timeout, () => { req.destroy(); reject(new Error('Timeout '+url)); });
    req.on('error', reject);
  });
}
function httpPost(url, body) {
  return new Promise((resolve, reject) => {
    const s = JSON.stringify(body);
    const u = new URL(url);
    const req = https.request({
      hostname:u.hostname, port:443, path:u.pathname+u.search, method:'POST',
      headers:{'Content-Type':'application/json','Content-Length':Buffer.byteLength(s),'User-Agent':'NexusTrade/5.0'}
    }, res => {
      let d=''; res.on('data',c=>d+=c);
      res.on('end',()=>{ try{resolve(JSON.parse(d));}catch(e){reject(e);} });
    });
    req.setTimeout(15000,()=>{req.destroy();reject(new Error('Timeout POST'));});
    req.on('error',reject); req.write(s); req.end();
  });
}

// ════════════════════════════════════════════════════════════
// KRAKEN REST
// ════════════════════════════════════════════════════════════
function krakenGet(endpoint) {
  return new Promise((resolve, reject) => {
    const req = https.request({ hostname:'api.kraken.com',port:443,path:endpoint,method:'GET',headers:{'User-Agent':'NexusTrade/5.0'} }, res => {
      let d=''; res.on('data',c=>d+=c);
      res.on('end',()=>{ try{ const j=JSON.parse(d); if(j.error&&j.error.length)reject(new Error(j.error[0]));else resolve(j.result); }catch(e){reject(e);} });
    });
    req.setTimeout(10000,()=>{req.destroy();reject(new Error('Timeout Kraken GET'));});
    req.on('error',reject); req.end();
  });
}
function krakenPrivate(endpoint, params={}) {
  return new Promise((resolve, reject) => {
    if (!KRAKEN_KEY||!KRAKEN_SECRET){reject(new Error('API non configuree'));return;}
    const nonce=Date.now().toString(), body=qs.stringify({nonce,...params});
    const hash=crypto.createHash('sha256').update(nonce+body).digest('binary');
    const sign=crypto.createHmac('sha512',Buffer.from(KRAKEN_SECRET,'base64')).update(endpoint+hash,'binary').digest('base64');
    const req=https.request({hostname:'api.kraken.com',port:443,path:endpoint,method:'POST',headers:{'API-Key':KRAKEN_KEY,'API-Sign':sign,'Content-Type':'application/x-www-form-urlencoded','User-Agent':'NexusTrade/5.0'}},res=>{
      let d=''; res.on('data',c=>d+=c);
      res.on('end',()=>{ try{const j=JSON.parse(d);if(j.error&&j.error.length)reject(new Error(j.error[0]));else resolve(j.result);}catch(e){reject(e);} });
    });
    req.setTimeout(15000,()=>{req.destroy();reject(new Error('Timeout Kraken POST'));});
    req.on('error',reject); req.write(body); req.end();
  });
}

// ════════════════════════════════════════════════════════════
// POLLING PRIX KRAKEN
// ════════════════════════════════════════════════════════════
async function pollKrakenPrices() {
  try {
    const result = await krakenGet('/0/public/Ticker?pair='+TICKER_QUERY);
    pollErrors = 0;
    for (const [key,ticker] of Object.entries(result)) {
      const display = TICKER_MAP[key]; if(!display) continue;
      const price=parseFloat(ticker.c[0]),ask=parseFloat(ticker.a[0]),bid=parseFloat(ticker.b[0]),
            vol=parseFloat(ticker.v[1]),high=parseFloat(ticker.h[1]),low=parseFloat(ticker.l[1]),open=parseFloat(ticker.o[1]);
      const change=open>0?((price-open)/open)*100:0;
      latestPrices[display]={price,ask,bid,vol,high,low,change};
      const msg=JSON.stringify({type:'price',pair:display,price,ask,bid,vol,high,low,change});
      frontendClients.filter(c=>c.readyState===WebSocket.OPEN).forEach(c=>{try{c.send(msg);}catch(e){}});
    }
  } catch(e) { pollErrors++; console.error(`[PRIX #${pollErrors}]`,e.message); }
  setTimeout(pollKrakenPrices, pollErrors>3?10000:5000);
}

// ════════════════════════════════════════════════════════════
// SCORING TOKEN DEXSCREENER (0-100)
// ════════════════════════════════════════════════════════════
function scoreToken(pair) {
  let score = 50; // base neutre
  const flags = [];
  try {
    const liq      = parseFloat(pair.liquidity?.usd||0);
    const vol1h    = parseFloat(pair.volume?.h1||0);
    const mcap     = parseFloat(pair.marketCap||pair.fdv||0);
    const ageMin   = (Date.now()-(pair.pairCreatedAt||Date.now()))/60000;
    const p5m      = parseFloat(pair.priceChange?.m5||0);
    const p1h      = parseFloat(pair.priceChange?.h1||0);
    const buys1h   = pair.txns?.h1?.buys||0;
    const sells1h  = pair.txns?.h1?.sells||0;
    const txns1h   = buys1h+sells1h;
    const buyRatio = txns1h>0?buys1h/txns1h:0.5;

    // Liquidite (critique)
    if(liq>=100000){score+=20;flags.push('💧 Liquidite excellente');}
    else if(liq>=30000){score+=12;}
    else if(liq>=5000){score+=4;}
    else if(liq<1000){score-=25;flags.push('🚨 Liquidite dangereuse');}
    else{score-=10;flags.push('⚠️ Faible liquidite');}

    // Age
    if(ageMin<5){score+=15;flags.push('🆕 Tres recent (<5min)');}
    else if(ageMin<15){score+=10;flags.push('🆕 Nouveau (<15min)');}
    else if(ageMin<30){score+=5;}
    else if(ageMin>120){score-=8;}

    // Volume 1h
    if(vol1h>=200000){score+=15;flags.push('📊 Volume exceptionnel');}
    else if(vol1h>=50000){score+=10;flags.push('📊 Bon volume');}
    else if(vol1h>=5000){score+=4;}
    else{score-=8;flags.push('⚠️ Volume faible');}

    // Momentum prix
    if(p5m>30&&p5m<500){score+=12;flags.push(`📈 +${p5m.toFixed(0)}% en 5min`);}
    else if(p5m>10){score+=7;}
    else if(p5m>5){score+=4;}
    else if(p5m<-30){score-=15;flags.push('📉 Chute -30% 5min');}
    else if(p5m<-15){score-=8;}

    // Pression achat
    if(buyRatio>0.7){score+=10;flags.push(`🟢 ${Math.round(buyRatio*100)}% acheteurs`);}
    else if(buyRatio>0.55){score+=5;}
    else if(buyRatio<0.35){score-=8;flags.push('🔴 Vendeurs dominent');}

    // Activite
    if(txns1h>1000){score+=8;flags.push('⚡ Tres actif');}
    else if(txns1h>200){score+=4;}
    else if(txns1h<20){score-=12;flags.push('💤 Peu actif');}

    // Market cap (potentiel x100)
    if(mcap>0&&mcap<50000){score+=12;flags.push('🚀 Micro cap — potentiel x100+');}
    else if(mcap<200000){score+=7;}
    else if(mcap<1000000){score+=2;}
    else if(mcap>5000000){score-=5;}

    // Anti-manipulation basique
    if(!pair.info?.socials?.length){score-=5;flags.push('⚠️ Pas de reseaux sociaux');}
    if(pair.dexId==='pump.fun'){score+=5;flags.push('🎯 Pump.fun');}

    // Blacklist dev
    if(DEV_BLACKLIST.has(pair.baseToken?.address||'')||DEV_BLACKLIST.has(pair.pairAddress||'')){
      score=0;flags.push('🚫 BLACKLISTE');
    }
  } catch(e) {}
  return { score: Math.max(0,Math.min(100,Math.round(score))), flags };
}

// ════════════════════════════════════════════════════════════
// POLLING DEXSCREENER — nouveaux tokens Solana
// ════════════════════════════════════════════════════════════
async function pollDexScreener() {
  try {
    // Fetch profils recents (tokens avec boost/attention)
    const profiles = await httpGet('https://api.dexscreener.com/token-profiles/latest/v1');
    const solProfiles = (Array.isArray(profiles)?profiles:[])
      .filter(t=>t.chainId==='solana').slice(0,30);

    if (solProfiles.length>0) {
      const addrs = solProfiles.map(t=>t.tokenAddress).filter(Boolean).join(',');
      if (!addrs) { setTimeout(pollDexScreener,60000); return; }
      const pairData = await httpGet(`https://api.dexscreener.com/latest/dex/tokens/${addrs}`);
      const pairs = (pairData.pairs||[]).filter(p=>p.chainId==='solana');

      const scored = pairs.map(p => {
        const {score,flags} = scoreToken(p);
        const smartPresent = SMART_MONEY_WALLETS.length>0 && Math.random()<0.05; // placeholder
        return {
          address:    p.baseToken?.address||'',
          name:       p.baseToken?.name||'Unknown',
          symbol:     p.baseToken?.symbol||'?',
          pairAddress:p.pairAddress||'',
          dexId:      p.dexId||'',
          url:        p.url||'',
          price:      parseFloat(p.priceUsd||0),
          liquidity:  parseFloat(p.liquidity?.usd||0),
          volume1h:   parseFloat(p.volume?.h1||0),
          volume24h:  parseFloat(p.volume?.h24||0),
          marketCap:  parseFloat(p.marketCap||p.fdv||0),
          p5m:        parseFloat(p.priceChange?.m5||0),
          p1h:        parseFloat(p.priceChange?.h1||0),
          buys1h:     p.txns?.h1?.buys||0,
          sells1h:    p.txns?.h1?.sells||0,
          ageMin:     (Date.now()-(p.pairCreatedAt||Date.now()))/60000,
          pairCreatedAt: p.pairCreatedAt||0,
          score, flags,
          smartMoney: smartPresent,
          scannedAt:  Date.now(),
          status:     'new',
        };
      }).filter(t=>t.address).sort((a,b)=>b.score-a.score);

      // Merge avec existants
      const existFiltered = discoveredTokens.filter(t=>!scored.find(s=>s.address===t.address));
      discoveredTokens = [...scored,...existFiltered].slice(0,100);

      // Broadcast
      const msg = JSON.stringify({type:'tokens',tokens:discoveredTokens.slice(0,30)});
      frontendClients.filter(c=>c.readyState===WebSocket.OPEN).forEach(c=>{try{c.send(msg);}catch(e){}});

      // Snipe auto
      if (sniperAuto && solanaKeypair) {
        const candidate = scored.find(t=>t.score>=sniperMinScore && !solanaPositions[t.address]);
        if (candidate) {
          const risk = checkRisk(sniperAmountUsd,'memecoin');
          if (risk.ok) {
            console.log(`[AUTO-SNIPE] ${candidate.symbol} score=${candidate.score} $${sniperAmountUsd}`);
            buyTokenJupiter(candidate.address, sniperAmountUsd, candidate.symbol, candidate.score)
              .then(sig=>console.log(`[AUTO-SNIPE] OK sig=${sig}`))
              .catch(e=>console.error('[AUTO-SNIPE] ERR',e.message));
          }
        }
      }

      if(scored.length>0) console.log(`[DEX] ${scored.length} tokens. Top: ${scored[0].symbol} score=${scored[0].score}`);
    }
  } catch(e) { console.error('[DEX ERROR]',e.message); }
  setTimeout(pollDexScreener, 60000);
}

// ════════════════════════════════════════════════════════════
// SOLANA — Jupiter swap
// ════════════════════════════════════════════════════════════
const SOL_MINT='So11111111111111111111111111111111111111112';

async function getSolBalance() {
  if(!solanaConn||!solanaKeypair) return 0;
  try { return (await solanaConn.getBalance(solanaKeypair.publicKey))/1e9; } catch(e){return 0;}
}

async function buyTokenJupiter(tokenMint, amountUsd, symbol='?', score=0) {
  if(!solanaConn||!solanaKeypair) throw new Error('Wallet Solana non configure');
  const solPrice = latestPrices['SOL/USD']?.price||100;
  const lamports = Math.floor((amountUsd/solPrice)*1e9);
  if(lamports<5000) throw new Error('Montant trop faible');

  const quote = await httpGet(`https://quote-api.jup.ag/v6/quote?inputMint=${SOL_MINT}&outputMint=${tokenMint}&amount=${lamports}&slippageBps=1500`);
  if(!quote||quote.error) throw new Error('Jupiter quote: '+(quote?.error||'fail'));

  const swap = await httpPost('https://api.jup.ag/swap/v1/swap',{
    quoteResponse:quote, userPublicKey:solanaKeypair.publicKey.toBase58(), wrapAndUnwrapSol:true
  });
  if(!swap||swap.error) throw new Error('Jupiter swap: '+(swap?.error||'fail'));

  const tx = solanaWeb3.VersionedTransaction.deserialize(Buffer.from(swap.swapTransaction,'base64'));
  tx.sign([solanaKeypair]);
  const sig = await solanaConn.sendRawTransaction(tx.serialize(),{skipPreflight:false,maxRetries:3});
  await solanaConn.confirmTransaction(sig,'confirmed');

  solanaPositions[tokenMint]={amountUsd,solSpent:lamports/1e9,buyPrice:null,time:Date.now(),symbol,score,sig,status:'open',tokensReceived:parseFloat(quote.outAmount||0)/1e9};
  learningData.push({tokenMint,score,bought:true,result:'pending',pnlPct:0,ts:Date.now()});
  broadcastAll();
  return sig;
}

async function sellTokenJupiter(tokenMint, tokensAmount) {
  if(!solanaConn||!solanaKeypair) throw new Error('Wallet Solana non configure');
  const amountRaw = Math.floor(tokensAmount*1e6); // assume 6 decimals for most tokens
  const quote = await httpGet(`https://quote-api.jup.ag/v6/quote?inputMint=${tokenMint}&outputMint=${SOL_MINT}&amount=${amountRaw}&slippageBps=2000`);
  if(!quote||quote.error) throw new Error('Jupiter quote sell: '+(quote?.error||'fail'));
  const swap = await httpPost('https://api.jup.ag/swap/v1/swap',{
    quoteResponse:quote, userPublicKey:solanaKeypair.publicKey.toBase58(), wrapAndUnwrapSol:true
  });
  if(!swap||swap.error) throw new Error('Jupiter swap sell: '+(swap?.error||'fail'));
  const tx=solanaWeb3.VersionedTransaction.deserialize(Buffer.from(swap.swapTransaction,'base64'));
  tx.sign([solanaKeypair]);
  const sig=await solanaConn.sendRawTransaction(tx.serialize(),{skipPreflight:false});
  await solanaConn.confirmTransaction(sig,'confirmed');
  return sig;
}

// ════════════════════════════════════════════════════════════
// LEARNING — analyse des patterns
// ════════════════════════════════════════════════════════════
function getLearningStats() {
  const bought = learningData.filter(d=>d.bought&&d.result!=='pending');
  const wins   = bought.filter(d=>d.pnlPct>0);
  const avgPnl = bought.length?bought.reduce((s,d)=>s+d.pnlPct,0)/bought.length:0;
  const winRate= bought.length?wins.length/bought.length:0;
  // Analyse par bracket de score
  const brackets={};
  bought.forEach(d=>{
    const b=Math.floor(d.score/10)*10+'';
    if(!brackets[b]) brackets[b]={total:0,wins:0};
    brackets[b].total++;
    if(d.pnlPct>0) brackets[b].wins++;
  });
  // Recommandation auto
  let bestBracket=0,bestWr=0;
  Object.entries(brackets).forEach(([b,v])=>{ if(v.total>=3&&v.wins/v.total>bestWr){bestWr=v.wins/v.total;bestBracket=parseInt(b);} });
  return { totalTrades:bought.length, winRate, avgPnl, brackets, recommendedMinScore:bestBracket||sniperMinScore };
}

// ════════════════════════════════════════════════════════════
// WEBSOCKET
// ════════════════════════════════════════════════════════════
function broadcastAll() {
  const risk = JSON.stringify({type:'risk',config:riskConfig,dailyLoss,tradeCount,
    openKraken:Object.keys(krakenPositions).length,openSolana:Object.keys(solanaPositions).length,
    solanaPositions});
  frontendClients.filter(c=>c.readyState===WebSocket.OPEN).forEach(c=>{try{c.send(risk);}catch(e){}});
}

wss.on('connection', ws=>{
  frontendClients.push(ws);
  // Snapshot initial
  try{
    ws.send(JSON.stringify({type:'snapshot',prices:latestPrices}));
    if(discoveredTokens.length) ws.send(JSON.stringify({type:'tokens',tokens:discoveredTokens.slice(0,30)}));
    ws.send(JSON.stringify({type:'risk',config:riskConfig,dailyLoss,tradeCount,openKraken:Object.keys(krakenPositions).length,openSolana:Object.keys(solanaPositions).length,solanaPositions}));
  }catch(e){}
  ws.on('close',()=>{ frontendClients=frontendClients.filter(c=>c!==ws); });
  ws.on('error',()=>{ frontendClients=frontendClients.filter(c=>c!==ws); });
});
setInterval(()=>{
  frontendClients=frontendClients.filter(c=>c.readyState===WebSocket.OPEN);
  frontendClients.forEach(c=>{try{c.send(JSON.stringify({type:'ping'}));}catch(e){}});
},30000);

// ════════════════════════════════════════════════════════════
// ROUTES
// ════════════════════════════════════════════════════════════
app.get('/api/health',(req,res)=>res.json({
  ok:true,version:'5.0',
  pairsActive:Object.keys(latestPrices).length,
  apiConfigured:KRAKEN_KEY.length>0,
  solanaConfigured:!!solanaKeypair,
  solanaWallet:solanaKeypair?.publicKey.toBase58()||null,
  tokensDiscovered:discoveredTokens.length,
  sniperAuto,sniperMinScore,
  dailyLoss,tradeCount,pollErrors,
}));

app.post('/api/config',(req,res)=>{
  const{key,secret,maxLoss,maxTrades}=req.body;
  if(!key||!secret) return res.status(400).json({error:'Cles manquantes'});
  KRAKEN_KEY=key; KRAKEN_SECRET=secret;
  if(maxLoss) riskConfig.maxDailyLoss=parseFloat(maxLoss);
  if(maxTrades) riskConfig.maxKrakenTrades=parseInt(maxTrades);
  res.json({ok:true});
});

app.get('/api/prices',(req,res)=>res.json(latestPrices));

app.get('/api/ohlc/:pair/:interval',async(req,res)=>{
  try{
    const r=await krakenGet(`/0/public/OHLC?pair=${req.params.pair}&interval=${req.params.interval}`);
    const k=Object.keys(r).find(k=>k!=='last');
    res.json({candles:r[k].map(c=>({time:c[0],open:+c[1],high:+c[2],low:+c[3],close:+c[4],volume:+c[6]}))});
  }catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/balance',async(req,res)=>{
  try{res.json(await krakenPrivate('/0/private/Balance'));}
  catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/order',async(req,res)=>{
  checkReset();
  try{
    if(!KRAKEN_KEY) return res.status(401).json({error:'API non configuree'});
    const risk=checkRisk(req.body.amountUsd||20,'kraken');
    if(!risk.ok) return res.status(403).json({error:risk.reason});
    if(tradeCount>=riskConfig.maxKrakenTrades) return res.status(403).json({error:'Max trades/jour'});
    const{pair,type,ordertype,volume}=req.body;
    const result=await krakenPrivate('/0/private/AddOrder',{pair,type,ordertype,volume,validate:'false'});
    tradeCount++;
    broadcastAll();
    res.json({ok:true,txid:result.txid,descr:result.descr});
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/cancel-all',async(req,res)=>{
  try{res.json({ok:true,result:await krakenPrivate('/0/private/CancelAll')});}
  catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/positions',(req,res)=>res.json(krakenPositions));
app.post('/api/positions',(req,res)=>{
  const{pair,price,volume,peak,action}=req.body;
  if(!pair) return res.status(400).json({error:'pair manquant'});
  if(action==='open') krakenPositions[pair]={price,volume,peak:peak||price,time:Date.now()};
  else if(action==='close') delete krakenPositions[pair];
  broadcastAll();
  res.json({ok:true,positions:krakenPositions});
});

app.get('/api/risk',(req,res)=>{
  checkReset();
  res.json({config:riskConfig,dailyLoss,tradeCount,openKraken:Object.keys(krakenPositions).length,openSolana:Object.keys(solanaPositions).length});
});
app.post('/api/risk',(req,res)=>{
  Object.assign(riskConfig,req.body);
  broadcastAll();
  res.json({ok:true,config:riskConfig});
});

app.get('/api/solana/status',async(req,res)=>{
  const bal=await getSolBalance();
  res.json({configured:!!solanaKeypair,wallet:solanaKeypair?.publicKey.toBase58()||null,solBalance:bal,positions:solanaPositions,sniperAuto,sniperMinScore,sniperAmountUsd});
});

app.post('/api/solana/buy',async(req,res)=>{
  try{
    const{tokenMint,amountUsd,symbol,score}=req.body;
    if(!tokenMint||!amountUsd) return res.status(400).json({error:'tokenMint et amountUsd requis'});
    const risk=checkRisk(amountUsd,'memecoin');
    if(!risk.ok) return res.status(403).json({error:risk.reason});
    const sig=await buyTokenJupiter(tokenMint,amountUsd,symbol||'?',score||0);
    res.json({ok:true,sig});
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/solana/sell',async(req,res)=>{
  try{
    const{tokenMint,tokensAmount,pnlPct}=req.body;
    if(!tokenMint||!tokensAmount) return res.status(400).json({error:'tokenMint et tokensAmount requis'});
    const sig=await sellTokenJupiter(tokenMint,parseFloat(tokensAmount));
    const pos=solanaPositions[tokenMint];
    if(pos){
      const pnl=(pnlPct||0)/100*(pos.amountUsd||0);
      if(pnl<0) dailyLoss+=pnl;
      const ld=learningData.find(d=>d.tokenMint===tokenMint&&d.result==='pending');
      if(ld){ld.result=pnl>=0?'win':'loss';ld.pnlPct=pnlPct||0;}
    }
    delete solanaPositions[tokenMint];
    broadcastAll();
    res.json({ok:true,sig});
  }catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/tokens',(req,res)=>res.json(discoveredTokens.slice(0,50)));

app.get('/api/learning',(req,res)=>res.json({stats:getLearningStats(),recent:learningData.slice(-30),sniperConfig:{sniperAuto,sniperMinScore,sniperAmountUsd}}));

app.post('/api/sniper/config',(req,res)=>{
  if(req.body.auto!==undefined) sniperAuto=req.body.auto;
  if(req.body.minScore!==undefined) sniperMinScore=parseInt(req.body.minScore);
  if(req.body.amountUsd!==undefined) sniperAmountUsd=parseFloat(req.body.amountUsd);
  res.json({ok:true,sniperAuto,sniperMinScore,sniperAmountUsd});
});

app.get('/api/smartmoney',(req,res)=>res.json({wallets:SMART_MONEY_WALLETS,activity:smartActivity.slice(-30)}));

app.post('/api/pnl',(req,res)=>{
  if(req.body.pnl<0) dailyLoss+=req.body.pnl;
  res.json({dailyLoss});
});

// ════════════════════════════════════════════════════════════
// DEMARRAGE
// ════════════════════════════════════════════════════════════
server.listen(PORT,()=>{
  console.log('');
  console.log('⚡ ══════════════════════════════════════════');
  console.log('   NEXUS TRADE v5 — Hub de trading demarre');
  console.log(`   Port     → ${PORT}`);
  console.log(`   Kraken   → ${KRAKEN_KEY?'configure':'NON configure'}`);
  console.log(`   Solana   → ${SOLANA_PRIVATE_KEY?'configure':'NON configure'}`);
  console.log(`   Paires   → ${KRAKEN_PAIRS.length} Kraken + DexScreener Solana`);
  console.log('⚡ ══════════════════════════════════════════');
  console.log('');
  initSolana();
  pollKrakenPrices();
  setTimeout(pollDexScreener, 5000);
});
