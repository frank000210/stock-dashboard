const express = require('express');
const axios = require('axios');
const NodeCache = require('node-cache');
const path = require('path');
const { MongoClient } = require('mongodb');

const app = express();
const cache = new NodeCache({ stdTTL: 3600 });
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// âââ MongoDB ââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
const MONGODB_URI = process.env.MONGODB_URI || '';
let db = null;

async function connectMongo() {
  if (!MONGODB_URI) {
    console.warn('â ï¸  MONGODB_URI not set â revenue & financial endpoints will return empty data');
    return;
  }
  try {
    const client = new MongoClient(MONGODB_URI, {
      serverSelectionTimeoutMS: 5000,
      connectTimeoutMS: 5000,
    });
    await client.connect();
    db = client.db('stock_dashboard');
    console.log('â Connected to MongoDB');
  } catch (err) {
    console.error('â MongoDB connection failed:', err.message);
  }
}

// âââ Shared helpers âââââââââââââââââââââââââââââââââââââââââââââââââ
const BASE_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Accept': 'application/json, text/plain, */*',
  'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8',
  'Origin': 'https://mops.twse.com.tw',
  'Referer': 'https://mops.twse.com.tw/mops/',
};

function formatDate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}${m}${day}`;
}

function toNum(str) {
  if (!str) return 0;
  return parseInt(String(str).replace(/,/g, '')) || 0;
}

// âââ æçæ¶ (from MongoDB) ââââââââââââââââââââââââââââââââââââââââââ
app.get('/api/revenue/:stockId', async (req, res) => {
  const { stockId } = req.params;
  const cacheKey = `revenue_${stockId}`;
  if (cache.has(cacheKey)) return res.json(cache.get(cacheKey));

  try {
    if (!db) {
      return res.json({ stockId, data: [], source: 'no_db' });
    }

    const docs = await db.collection('revenue')
      .find({ stock_id: stockId })
      .sort({ tw_year: -1, tw_month: -1 })
      .limit(13)
      .toArray();

    const rows = docs.map(d => ({
      period: d.period,
      revenue: d.revenue,
      cumRevenue: d.cum_revenue || 0,
      yoy: d.yoy || 0,
    }));

    const data = { stockId, data: rows, source: 'mongodb' };
    cache.set(cacheKey, data);
    res.json(data);
  } catch (err) {
    console.error(`[revenue/${stockId}] Error:`, err.message);
    res.status(500).json({ error: err.message });
  }
});

// âââ ä¸å¤§æ³äºº (åè¡) â from TWSE (works from cloud) âââââââââââââââââ
app.get('/api/institutional/:stockId', async (req, res) => {
  const { stockId } = req.params;
  const cacheKey = `inst_${stockId}`;
  if (cache.has(cacheKey)) return res.json(cache.get(cacheKey));

  try {
    const results = [];
    const now = new Date();

    for (let back = 0; back < 90 && results.length < 30; back++) {
      const d = new Date(now);
      d.setDate(d.getDate() - back);
      if (d.getDay() === 0 || d.getDay() === 6) continue;

      const dateStr = formatDate(d);
      try {
        const resp = await axios.get(
          `https://www.twse.com.tw/fund/TWT38U?response=json&date=${dateStr}&stockNo=${stockId}`,
          {
            headers: {
              ...BASE_HEADERS,
              'Referer': 'https://www.twse.com.tw/zh/fund/TWT38U',
            },
            timeout: 10000,
          }
        );
        if (resp.data?.stat === 'OK' && resp.data?.data?.length > 0) {
          const row = resp.data.data[0];
          results.push({
            date: `${dateStr.slice(0, 4)}/${dateStr.slice(4, 6)}/${dateStr.slice(6, 8)}`,
            foreign: toNum(row[4]),
            investment: toNum(row[9]),
            dealer: toNum(row[11]),
            total: toNum(row[4]) + toNum(row[9]) + toNum(row[11]),
          });
        }
      } catch (_) {}
    }

    const data = { stockId, data: results.reverse() };
    cache.set(cacheKey, data);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// âââ è²¡åå ±è¡¨ (from MongoDB) ââââââââââââââââââââââââââââââââââââââââ
app.get('/api/financial/:stockId', async (req, res) => {
  const { stockId } = req.params;
  const cacheKey = `fin_${stockId}`;
  if (cache.has(cacheKey)) return res.json(cache.get(cacheKey));

  try {
    if (!db) {
      return res.json({ stockId, data: [], source: 'no_db' });
    }

    const docs = await db.collection('financial')
      .find({ stock_id: stockId })
      .sort({ tw_year: -1, season: -1 })
      .limit(8)
      .toArray();

    const rows = docs.map(d => ({
      period: d.period,
      revenue: d.revenue,
      opIncome: d.op_income || 0,
      netIncome: d.net_income || 0,
      eps: d.eps || 0,
    }));

    const data = { stockId, data: rows, source: 'mongodb' };
    cache.set(cacheKey, data);
    res.json(data);
  } catch (err) {
    console.error(`[financial/${stockId}] Error:`, err.message);
    res.status(500).json({ error: err.message });
  }
});

// âââ å¤§ç¤ä¸å¤§æ³äººåè¨ â from TWSE ââââââââââââââââââââââââââââââââââ
app.get('/api/market', async (req, res) => {
  const cacheKey = 'market_overview';
  if (cache.has(cacheKey)) return res.json(cache.get(cacheKey));

  try {
    const now = new Date();
    let result = null;

    for (let back = 0; back < 5 && !result; back++) {
      const d = new Date(now);
      d.setDate(d.getDate() - back);
      if (d.getDay() === 0 || d.getDay() === 6) continue;

      const dateStr = formatDate(d);
      try {
        const resp = await axios.get(
          `https://www.twse.com.tw/fund/T86?response=json&date=${dateStr}&selectType=ALLBUT0999`,
          {
            headers: { ...BASE_HEADERS, 'Referer': 'https://www.twse.com.tw/' },
            timeout: 10000,
          }
        );
        const dd = resp.data;
        if (dd?.stat === 'OK' && dd?.data?.length > 0) {
          const last = dd.data[dd.data.length - 1] || [];
          const foreign = toNum(last[17]);
          const investment = toNum(last[18]);
          const dealer = toNum(last[19]);
          if (foreign !== 0 || investment !== 0 || dealer !== 0) {
            result = { date: dd.date || dateStr, foreign, investment, dealer };
          }
        }
      } catch (_) {}
    }

    const finalResult = result || { date: formatDate(now), foreign: 0, investment: 0, dealer: 0 };
    cache.set(cacheKey, finalResult, 1800);
    res.json(finalResult);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// âââ Kç·è³æ â from TWSE ââââââââââââââââââââââââââââââââââââââââââââ
app.get('/api/kline/:stockId', async (req, res) => {
  const { stockId } = req.params;
  const cacheKey = `kline_${stockId}`;
  if (cache.has(cacheKey)) return res.json(cache.get(cacheKey));

  try {
    const allData = [];
    const now = new Date();

    // Try TWSE (TSE listed stocks) - fetch 6 months
    for (let i = 5; i >= 0; i--) {
      const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
      const dateStr = formatDate(d);
      try {
        const resp = await axios.get(
          `https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=${dateStr}&stockNo=${stockId}`,
          { headers: { ...BASE_HEADERS, 'Referer': 'https://www.twse.com.tw/' }, timeout: 10000 }
        );
        if (resp.data?.stat === 'OK' && resp.data?.data?.length > 0) {
          for (const row of resp.data.data) {
            const parts = row[0].split('/');
            if (parts.length !== 3) continue;
            const dateISO = `${parseInt(parts[0]) + 1911}-${parts[1]}-${parts[2]}`;
            const open  = parseFloat(String(row[3]).replace(/,/g, '')) || 0;
            const high  = parseFloat(String(row[4]).replace(/,/g, '')) || 0;
            const low   = parseFloat(String(row[5]).replace(/,/g, '')) || 0;
            const close = parseFloat(String(row[6]).replace(/,/g, '')) || 0;
            const vol   = parseInt(String(row[1]).replace(/,/g, '')) || 0;
            if (open > 0 && close > 0 && high > 0 && low > 0) {
              allData.push({ date: dateISO, open, high, low, close, volume: vol });
            }
          }
        }
      } catch (_) {}
    }

    // If no TSE data, try TPEX (OTC listed stocks)
    if (allData.length === 0) {
      for (let i = 5; i >= 0; i--) {
        const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
        const twY = d.getFullYear() - 1911;
        const twM = String(d.getMonth() + 1).padStart(2, '0');
        try {
          const resp = await axios.get(
            `https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d=${twY}/${twM}&stkno=${stockId}&s=0,asc,0`,
            { headers: { ...BASE_HEADERS, 'Referer': 'https://www.tpex.org.tw/', 'Origin': 'https://www.tpex.org.tw' }, timeout: 10000 }
          );
          const rows = resp.data?.aaData;
          if (rows && rows.length > 0) {
            for (const row of rows) {
              const parts = row[0].split('/');
              if (parts.length !== 3) continue;
              const dateISO = `${parseInt(parts[0]) + 1911}-${parts[1]}-${parts[2]}`;
              const open  = parseFloat(String(row[3]).replace(/,/g, '')) || 0;
              const high  = parseFloat(String(row[4]).replace(/,/g, '')) || 0;
              const low   = parseFloat(String(row[5]).replace(/,/g, '')) || 0;
              const close = parseFloat(String(row[6]).replace(/,/g, '')) || 0;
              const vol   = parseInt(String(row[1]).replace(/,/g, '')) * 1000 || 0;
              if (open > 0 && close > 0 && high > 0 && low > 0) {
                allData.push({ date: dateISO, open, high, low, close, volume: vol });
              }
            }
          }
        } catch (_) {}
      }
    }

    const data = { stockId, data: allData };
    cache.set(cacheKey, data, 3600);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// âââ å³æå ±å¹ â from TWSE âââââââââââââââââââââââââââââââââââââââââââ
app.get('/api/quote/:stockId', async (req, res) => {
  const { stockId } = req.params;
  const cacheKey = `quote_${stockId}`;
  if (cache.has(cacheKey)) return res.json(cache.get(cacheKey));

  try {
    const resp = await axios.get(
      `https://mis.twse.com.tw/stock/api/getStockInfo.jsp?json=1&delay=0&ex_ch=tse_${stockId}.tw%7Cotc_${stockId}.tw`,
      {
        headers: {
          ...BASE_HEADERS,
          'Origin': 'https://mis.twse.com.tw',
          'Referer': 'https://mis.twse.com.tw/stock/fibest.html',
        },
        timeout: 8000,
      }
    );
    const item = resp.data?.msgArray?.find(s => s.c === stockId);
    if (item) {
      const price = parseFloat(item.z) || parseFloat(item.y) || 0;
      const prevClose = parseFloat(item.y) || 0;
      const change = price - prevClose;
      const changePct = prevClose > 0 ? change / prevClose * 100 : 0;
      const data = {
        stockId,
        name: item.n || stockId,
        price,
        prevClose,
        change: +change.toFixed(2),
        changePct: +changePct.toFixed(2),
        open: parseFloat(item.o) || 0,
        high: parseFloat(item.h) || 0,
        low: parseFloat(item.l) || 0,
        volume: parseInt(String(item.v || '0').replace(/,/g, '')) * 1000 || 0,
      };
      cache.set(cacheKey, data, 60);
      return res.json(data);
    }
    res.json({ stockId, name: stockId, price: 0, change: 0, changePct: 0 });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// âââ Scrape log (check when data was last updated) ââââââââââââââââââ
app.get('/api/scrape-status', async (req, res) => {
  try {
    if (!db) return res.json({ connected: false, logs: [] });
    const logs = await db.collection('scrape_log').find({}).toArray();
    res.json({ connected: true, logs });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// âââ æ¸é¤å¿«å âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
app.delete('/api/cache', (_, res) => {
  cache.flushAll();
  res.json({ message: 'Cache cleared' });
});

// âââ Start ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
// Start listening FIRST, then connect to MongoDB in background
app.listen(PORT, () => {
  console.log(`ð Stock Dashboard running on port ${PORT}`);
  // Connect to MongoDB in background (don't block server startup)
  connectMongo();
});
