HELLO_TEST_LINE_1
HELLO_TEST_LINE_2
const express = require('express');
const axios = require('axios');
const NodeCache = require('node-cache');
const path = require('path');

const app = express();
const cache = new NodeCache({ stdTTL: 3600 });
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const BASE_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Accept': 'application/json, text/plain, */*',
  'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8',
  'Origin': 'https://mops.twse.com.tw',
  'Referer': 'https://mops.twse.com.tw/mops/',
};

function getTWYear() {
  return new Date().getFullYear() - 1911;
}

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

// ─── 月營收 ────────────────────────────────────────────────
app.get('/api/revenue/:stockId', async (req, res) => {
  const { stockId } = req.params;
  const cacheKey = `revenue_${stockId}`;
  if (cache.has(cacheKey)) return res.json(cache.get(cacheKey));

  try {
    const rows = [];
    const now = new Date();

    for (let i = 0; i < 15 && rows.length < 13; i++) {
      const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
      const year = d.getFullYear() - 1911;
      const month = String(d.getMonth() + 1).padStart(2, '0');

      try {
        const resp = await axios.post(
          'https://mops.twse.com.tw/mops/api/t05st10_ifrs',
          { companyId: stockId, dataType: '2', month, year: String(year), subsidiaryCompanyId: '' },
          {
            headers: { ...BASE_HEADERS, 'Content-Type': 'application/json' },
            timeout: 12000,
          }
        );

        const result = resp.data?.result;
        if (resp.data?.code === 200 && result?.data) {
          const yymm = result.yymm || '';
          const monthRow = result.data.find(r => r[0] === '本月');
          const cumRow = result.data.find(r => r[0] === '累計');
          const yoyRow = result.data.find(r => r[0] && r[0].includes('去年同期'));

          if (monthRow && toNum(monthRow[1]) > 0) {
            const twY = yymm.slice(0, yymm.length - 2);
            const twM = yymm.slice(-2);
            rows.push({
              period: `${twY}/${twM}`,
              revenue: toNum(monthRow[1]) * 1000,
              cumRevenue: cumRow ? toNum(cumRow[1]) * 1000 : 0,
              yoy: yoyRow ? parseFloat(yoyRow[1]) || 0 : 0,
            });
          }
        }
      } catch (_) {}
    }

    const data = { stockId, data: rows.slice(0, 13) };
    cache.set(cacheKey, data);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── 三大法人 (個股) ─────────────────────────────────────
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

// ─── 財務報表 (季報) ─────────────────────────────────────
app.get('/api/financial/:stockId', async (req, res) => {
  const { stockId } = req.params;
  const cacheKey = `fin_${stockId}`;
  if (cache.has(cacheKey)) return res.json(cache.get(cacheKey));

  try {
    const twYear = getTWYear();
    const results = [];

    const periods = [];
    for (let y = twYear; y >= twYear - 3; y--) {
      for (let s = 4; s >= 1; s--) {
        periods.push({ year: y, season: s });
      }
    }

    for (const { year, season } of periods) {
      if (results.length >= 8) break;
      try {
        const resp = await axios.post(
          'https://mops.twse.com.tw/mops/api/t164sb04',
          {
            companyId: stockId,
            dataType: '2',
            year: String(year),
            season: String(season).padStart(2, '0'),
            subsidiaryCompanyId: '',
          },
          {
            headers: { ...BASE_HEADERS, 'Content-Type': 'application/json' },
            timeout: 15000,
          }
        );

        const result = resp.data?.result;
        if (resp.data?.code === 200 && result?.reportList?.length > 0) {
          const rows = result.reportList;
          let revenue = 0, opIncome = 0, netIncome = 0, eps = 0;

          for (const row of rows) {
            const label = row[0] ? row[0].trim() : '';
            const val = row[1] ? String(row[1]).replace(/,/g, '') : '0';

            if (label === '營業收入合計') revenue = toNum(val) * 1000;
            if (label === '營業利益（損失）') opIncome = toNum(val) * 1000;
            if (label.includes('本期淨利') && !label.includes('歸')) netIncome = toNum(val) * 1000;
            if (label === '基本每股盈餘') eps = parseFloat(val) || 0;
            if (label.replace(/\s/g, '') === '基本每股盈餘' && eps === 0) {
              eps = parseFloat(val) || 0;
            }
          }

          if (eps === 0) {
            const epsRow = rows.find(r => r[0] && r[0].replace(/\s/g, '') === '基本每股盈餘' && r[1] && parseFloat(r[1]) !== 0);
            if (epsRow) eps = parseFloat(epsRow[1]) || 0;
          }

          const qMap = { 1: 'Q1', 2: 'Q2', 3: 'Q3', 4: 'Q4' };
          if (revenue > 0 || eps !== 0) {
            results.push({
              period: `${year + 1911}${qMap[season]}`,
              revenue,
              opIncome,
              netIncome,
              eps,
            });
          }
        }
      } catch (_) {}
    }

    const data = { stockId, data: results };
    cache.set(cacheKey, data);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── 大盤三大法人合計 ─────────────────────────────────────
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

// ─── K線資料 ─────────────────────────────────────────────
app.get('/api/kline/:stockId', async (req, res) => {
  const { stockId } = req.params;
  const cacheKey = `kline_${stockId}`;
  if (cache.has(cacheKey)) return res.json(cache.get(cacheKey));

  try {
    const allData = [];
    const now = new Date();

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

// ─── 即時報價 ─────────────────────────────────────────────
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

// ─── 清除快取 ─────────────────────────────────────────────
app.delete('/api/cache', (_, res) => {
  cache.flushAll();
  res.json({ message: 'Cache cleared' });
});

app.listen(PORT, () => {
  console.log(`🚀 股市儀錶板運行於 port ${PORT}`);
});
