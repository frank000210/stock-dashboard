"""
MOPS Scraper - Fetches monthly revenue & quarterly financial statements
from mops.twse.com.tw (HTML endpoints) and stores them in MongoDB.

Uses the traditional HTML scraping approach (not JSON API) which is more reliable.

Usage:
    python mops_scraper.py             # one-time run
    python mops_scraper.py --watch     # run on schedule
"""

import os
import re
import sys
import time
import logging
from datetime import datetime, timedelta

import requests
import pandas as pd
from bs4 import BeautifulSoup
from pymongo import MongoClient, UpdateOne

# --------------- config ---------------
MONGODB_URI = os.getenv("MONGODB_URI", "")
STOCK_IDS = [s.strip() for s in os.getenv("STOCK_IDS", "2330").split(",") if s.strip()]
SCHEDULE_HOUR = int(os.getenv("SCHEDULE_HOUR", "17"))  # default 5PM Taiwan time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("mops")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
})


# --------------- DB ---------------
def connect_db():
    if not MONGODB_URI:
        log.error("MONGODB_URI not set")
        sys.exit(1)
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10000)
    db = client["stock_dashboard"]
    db.command("ping")
    log.info("Connected to MongoDB")
    return db


# --------------- Monthly Revenue ---------------
def scrape_monthly_revenue(db, stock_ids, months=15):
    log.info(f"Scraping monthly revenue for {len(stock_ids)} stocks (last {months} months)...")
    now = datetime.now()
    ops = []
    stock_set = set(stock_ids)

    for i in range(months):
        d = datetime(now.year, now.month, 1) - timedelta(days=i * 30)
        d = datetime(d.year, d.month, 1)
        tw_year = d.year - 1911
        month = d.month

        urls = [
            f"https://mops.twse.com.tw/nas/t21/sii/t21sc03_{tw_year}_{month}_0.html",
            f"https://mops.twse.com.tw/nas/t21/sii/t21sc03_{tw_year}_{month}_1.html",
        ]

        for url in urls:
            try:
                resp = SESSION.get(url, timeout=15)
                resp.encoding = "big5"
                log.info(f"[DEBUG] Revenue GET {url} => status={resp.status_code}, len={len(resp.text)}")
                if resp.status_code != 200:
                    log.warning(f"[DEBUG] Non-200 response: {resp.text[:300]}")
                    continue
                html_dfs = pd.read_html(resp.text)
                log.info(f"[DEBUG] Found {len(html_dfs)} tables, shapes: {[df.shape for df in html_dfs[:5]]}")
                valid_dfs = [df for df in html_dfs if df.shape[1] == 11]
                if not valid_dfs:
                    log.info(f"[DEBUG] No 11-col tables found")
                    continue

                df = pd.concat(valid_dfs)
                df.columns = df.columns.get_level_values(1) if isinstance(df.columns, pd.MultiIndex) else df.columns
                df = df[df.iloc[:, 1] != "合計"]
                df = df.reset_index(drop=True)
                col_names = list(df.columns)
                stock_no_col = col_names[0]
                log.info(f"[DEBUG] Revenue {tw_year}/{month}: {len(df)} rows, cols={list(df.columns)[:3]}")

                for _, row in df.iterrows():
                    sid = str(row[stock_no_col]).strip()
                    if sid not in stock_set:
                        continue

                    try:
                        revenue = parse_num(row.iloc[2])
                        last_month_rev = parse_num(row.iloc[3])
                        last_year_rev = parse_num(row.iloc[4])
                        mom_pct = parse_num(row.iloc[5])
                        yoy_pct = parse_num(row.iloc[6])
                        cum_revenue = parse_num(row.iloc[7])
                        last_year_cum = parse_num(row.iloc[8])

                        period = f"{tw_year}/{month:02d}"
                        doc = {
                            "stock_id": sid,
                            "period": period,
                            "revenue": revenue * 1000,
                            "last_month_revenue": last_month_rev * 1000,
                            "last_year_revenue": last_year_rev * 1000,
                            "mom_pct": mom_pct,
                            "yoy": yoy_pct,
                            "cum_revenue": cum_revenue * 1000,
                            "last_year_cum_revenue": last_year_cum * 1000,
                            "tw_year": tw_year,
                            "tw_month": month,
                            "updated_at": datetime.utcnow(),
                        }
                        ops.append(UpdateOne(
                            {"stock_id": sid, "period": period},
                            {"$set": doc},
                            upsert=True,
                        ))
                        log.info(f"[{sid}] Revenue {period}: {revenue * 1000:,.0f}")
                    except Exception as e:
                        log.debug(f"[{sid}] Row parse error: {e}")

            except Exception as e:
                log.warning(f"Revenue {tw_year}/{month} url={url} error: {e}")

            time.sleep(0.5)

    if ops:
        result = db["revenue"].bulk_write(ops)
        log.info(f"Revenue: upserted={result.upserted_count}, modified={result.modified_count}")
    else:
        log.warning("No revenue data scraped")


# --------------- Quarterly Financial (EPS, NAV) ---------------
def scrape_financial(db, stock_ids, quarters=8):
    log.info(f"Scraping quarterly financial for {len(stock_ids)} stocks (last {quarters} quarters)...")
    now = datetime.now()
    tw_year_now = now.year - 1911
    stock_set = set(stock_ids)
    ops = []

    quarter_list = []
    current_q = (now.month - 1) // 3
    if current_q == 0:
        current_q = 4
        start_year = tw_year_now - 1
    else:
        start_year = tw_year_now

    for i in range(quarters):
        q = current_q - i
        y = start_year
        while q <= 0:
            q += 4
            y -= 1
        quarter_list.append((y, q))

    for tw_year, season in quarter_list:
        try:
            eps_data = _scrape_income_statement(tw_year, season, stock_set)
            for sid, eps_val in eps_data.items():
                q_map = {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}
                period = f"{tw_year + 1911}{q_map[season]}"
                ops.append(UpdateOne(
                    {"stock_id": sid, "period": period},
                    {"$set": {
                        "stock_id": sid,
                        "period": period,
                        "eps": eps_val,
                        "year": tw_year + 1911,
                        "season": season,
                        "updated_at": datetime.utcnow(),
                    },
                     "$setOnInsert": {
                         "revenue": 0,
                         "operating_income": 0,
                         "net_income": 0,
                     }},
                    upsert=True,
                ))
                log.info(f"[{sid}] EPS {period}: {eps_val}")
        except Exception as e:
            log.warning(f"EPS {tw_year}/Q{season} error: {e}")

        time.sleep(3)

        try:
            nav_data = _scrape_balance_sheet(tw_year, season, stock_set)
            for sid, nav_val in nav_data.items():
                q_map = {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}
                period = f"{tw_year + 1911}{q_map[season]}"
                ops.append(UpdateOne(
                    {"stock_id": sid, "period": period},
                    {"$set": {
                        "nav_per_share": nav_val,
                        "updated_at": datetime.utcnow(),
                    }},
                    upsert=True,
                ))
                log.info(f"[{sid}] NAV {period}: {nav_val}")
        except Exception as e:
            log.warning(f"NAV {tw_year}/Q{season} error: {e}")

        time.sleep(3)

    if ops:
        result = db["financial"].bulk_write(ops)
        log.info(f"Financial: upserted={result.upserted_count}, modified={result.modified_count}")
    else:
        log.warning("No financial data scraped")


def _scrape_income_statement(tw_year, season, stock_set):
    url = "https://mops.twse.com.tw/mops/web/ajax_t163sb04"
    form_data = {
        "encodeURIComponent": 1,
        "isQuery": "Y",
        "step": 1,
        "TYPEK": "sii",
        "year": tw_year,
        "firstin": 1,
        "off": 1,
        "season": season,
    }

    resp = SESSION.post(url, data=form_data, timeout=30)
    resp.encoding = "utf8"
    log.info(f"[DEBUG] Income POST {tw_year}/Q{season}: status={resp.status_code}, len={len(resp.text)}, first300={resp.text[:300]}")
    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table", {"class": "hasBorder"})
    log.info(f"[DEBUG] Income {tw_year}/Q{season}: found {len(tables)} hasBorder tables")

    results = {}
    data = _parse_html_tables(tables)
    log.info(f"[DEBUG] Income {tw_year}/Q{season}: parsed {len(data)} rows from tables")

    header_row = None
    eps_col_idx = -1

    for row_data in data:
        if row_data and row_data[0] == "公司代號":
            header_row = row_data
            for j, col_name in enumerate(header_row):
                if "基本每股盈餘" in col_name:
                    eps_col_idx = j
                    break
            log.info(f"[DEBUG] Found header, eps_col_idx={eps_col_idx}, cols={header_row[:3]}")
            continue

        if header_row and eps_col_idx > 0 and row_data:
            sid = str(row_data[0]).strip()
            if sid in stock_set and len(row_data) > eps_col_idx:
                eps_str = row_data[eps_col_idx]
                eps_val = parse_num(eps_str)
                if eps_val != 0 or eps_str.strip() == "0" or eps_str.strip() == "0.00":
                    results[sid] = eps_val

    log.info(f"Income statement {tw_year}/Q{season}: found {len(results)} stocks")
    return results


def _scrape_balance_sheet(tw_year, season, stock_set):
    url = "https://mops.twse.com.tw/mops/web/ajax_t163sb05"
    form_data = {
        "encodeURIComponent": 1,
        "isQuery": "Y",
        "step": 1,
        "TYPEK": "sii",
        "year": tw_year,
        "firstin": 1,
        "off": 1,
        "season": season,
    }

    resp = SESSION.post(url, data=form_data, timeout=30)
    resp.encoding = "utf8"
    log.info(f"[DEBUG] Balance POST {tw_year}/Q{season}: status={resp.status_code}, len={len(resp.text)}")
    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table", {"class": "hasBorder"})

    results = {}
    data = _parse_html_tables(tables)

    header_row = None
    nav_col_idx = -1

    for row_data in data:
        if row_data and row_data[0] == "公司代號":
            header_row = row_data
            for j, col_name in enumerate(header_row):
                if "每股參考淨值" in col_name:
                    nav_col_idx = j
                    break
            continue

        if header_row and nav_col_idx > 0 and row_data:
            sid = str(row_data[0]).strip()
            if sid in stock_set and len(row_data) > nav_col_idx:
                nav_str = row_data[nav_col_idx]
                nav_val = parse_num(nav_str)
                if nav_val != 0:
                    results[sid] = nav_val

    log.info(f"Balance sheet {tw_year}/Q{season}: found {len(results)} stocks")
    return results


def _parse_html_tables(tables):
    data = []
    for tb in tables:
        for row in tb.find_all("tr"):
            tempdata = []
            for col in row.find_all("th"):
                tempdata.append(col.text.strip().replace("　", ""))
            for col in row.find_all("td"):
                tempdata.append(col.text.strip().replace("　", ""))
            if len(tempdata) > 1:
                data.append(tempdata)
    return data


# --------------- Utilities ---------------
def parse_num(s):
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return 0.0
    try:
        cleaned = re.sub(r"[,\s%]", "", str(s).strip())
        if not cleaned or cleaned in ("-", "不適用", "N/A", "--"):
            return 0.0
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0


# --------------- Orchestration ---------------
def run_all(db):
    log.info(f"Starting scrape for stocks: {STOCK_IDS}")
    scrape_monthly_revenue(db, STOCK_IDS, months=15)
    scrape_financial(db, STOCK_IDS, quarters=8)
    db["scrape_log"].insert_one({
        "timestamp": datetime.utcnow(),
        "stocks": STOCK_IDS,
        "status": "completed",
    })
    log.info("Scrape complete!")


def main():
    db = connect_db()

    if "--watch" in sys.argv:
        import schedule as sched

        log.info(f"Watch mode: running now, then daily at {SCHEDULE_HOUR}:00 Taiwan time")
        run_all(db)

        utc_hour = (SCHEDULE_HOUR - 8) % 24
        sched.every().day.at(f"{utc_hour:02d}:00").do(run_all, db)

        while True:
            sched.run_pending()
            time.sleep(60)
    else:
        run_all(db)


if __name__ == "__main__":
    main()
