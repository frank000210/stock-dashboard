const express = require('express');
const axios = require('axios');
const cheerio = require('cheerio');
const NodeCache = require('node-cache');
const path = require('path');

const app = express();
const cache = new NodeCache({ stdTTL: 3600 });
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const BASE_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8',
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
    const twYear = getTWYear();
    const rows = [];

    for (let yearOffset = 0; yearOffset <= 1 && rows.length < 13; yearOffset++) {
      const year = twYear - yearOffset;
      const resp = await axios.post(
        'https://mops.twse.com.tw/mops/web/ajax_t05st10_ifrs',
        `co_id=${encodeURIComponent(stockId)}&TYPEK=sii&year=${year}&month=`,
        {
          headers: {
            ...BASE_HEADERS,
            'Content-Type': 'application/x-www-form-urlencoded',
            'Referer': 'https://mops.twse.com.tw/mops/web/t05st10_ifrs',
          },
          timeout: 15000,
        }
      );

      const $ = cheerio.load(resp.data);
      $('table.hasBorder tr').each((i, row) => {
        if (i === 0) return;
        const tds = $(row).find('td');
        if (tds.length < 5) return;
        const period = $(tds[0]).text().trim();
        const revenue = $(tds[1]).text().trim();
        const cumRevenue = $(tds[2]).text().trim();
        const yoy = $(tds[4]).text().trim();
        if (period && revenue) {
          rows.push({
            period,
            revenue: toNum(revenue),
            cumRevenue: toNum(cumRevenue),
            yoy: parseFloat(yoy) || 0,
          });
        }
      });
    }

    const data = { stockId, data: rows.reverse().slice(0, 13) };
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
            headers: { ...BASE_HEADERS, 'Referer': 'https://www.twse.com.tw/zh/fund/TWT38U' },
            timeout: 10000,
          }
        );
        if (resp.data?.stat === 'OK' && resp.data?.data?.length > 0) {
          const row = resp.data.data[0];
          results.push({
            date: `${dateStr.slice(0,4)}/${dateStr.slice(4,6)}/${dateStr.slice(6,8)}`,
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

    // Build list of (year, season) to try, newest first
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
          'https://mops.twse.com.tw/mops/web/ajax_t163sb04',
          `co_id=${encodeURIComponent(stockId)}&year=${year}&season=0${season}&TYPEK=sii&step=1`,
          {
            headers: {
              ...BASE_HEADERS,
              'Content-Type': 'application/x-www-form-urlencoded',
              'Referer': 'https://mops.twse.com.tw/mops/web/t163sb04',
            },
            timeout: 15000,
          }
        );

        const $ = cheerio.load(resp.data);
        let revenue = 0, opIncome = 0, netIncome = 0, eps = 0;

        $('table tr').each((_, row) => {
          const tds = $(row).find('td');
          if (tds.length < 2) return;
          const label = $(tds[0]).text().trim();
          const val = $(tds[1]).text().trim().replace(/,/g, '');

          if (/^營業收入合計/.test(label)) revenue = toNum(val);
          if (/^營業利益/.test(label) && !label.includes('率')) opIncome = toNum(val);
          if (/^本期淨利|本期損益/.test(label) && !label.includes('歸')) netIncome = toNum(val);
          if (/^e��本每股盈餘/.test(label)) eps = parseFloat(val) || 0;
        });

        if (revenue > 0 || eps !== 0) {
          const qMap = { 1: 'Q1', 2: 'Q2', 3: 'Q3', 4: 'Q4' };
          results.push({
            period: `${year + 1911}${qMap[season]}`,
            revenue,
            opIncome,
            netIncome,
            eps,
          });
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
    const dateStr = formatDate(new Date());
    const resp = await axios.get(
      `https://www.twse.com.tw/fund/T86?response=json&date=${dateStr}&selectType=ALLBUT0999`,
      { headers: BASE_HEADERS, timeout: 10000 }
    );
    const d = resp.data;
    if (d?.stat === 'OK' && d?.data) {
      const last = d.data[d.data.length - 1] || [];
      const result = {
        date: d.date || dateStr,
        foreign: toNum(last[17]),
        investment: toNum(last[18]),
        dealer: toNum(last[19]),
      };
      cache.set(cacheKey, result, 1800);
      res.json(result);
    } else {
      res.json({ date: dateStr, foreign: 0, investment: 0, dealer: 0 });
    }
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
