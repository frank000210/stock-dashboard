"""
MOPS / TWSE OpenAPI Scraper
============================
Fetches monthly revenue & quarterly financial statements from the
Taiwan Stock Exchange (TWSE) OpenAPI and stores them in MongoDB.

Endpoints used (JSON, no anti-bot blocking):
  - Revenue:          /v1/opendata/t187ap05_L   (上市公司每月營業收入彙總表)
  - Income statement: /v1/opendata/t187ap06_L_* (上市公司綜合損益表, 各產業)
  - Balance sheet:    /v1/opendata/t187ap07_L_* (上市公司資產負債表, 各產業)

Note: OpenAPI only returns the LATEST period. Run this scraper regularly
      (e.g. daily) to accumulate historical data in MongoDB.

Usage:
    python mops_scraper.py             # one-time run
    python mops_scraper.py --watch     # run on schedule (daily)
"""

import os
import re
import sys
import time
import logging
from datetime import datetime

import requests
import urllib3
import pandas as pd
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

# Suppress SSL warnings — government certs sometimes lack Subject Key Identifier
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load .env file from the same directory as this script
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# --------------- config ---------------
MONGODB_URI = os.getenv("MONGODB_URI", "")
STOCK_IDS = [s.strip() for s in os.getenv("STOCK_IDS", "2330").split(",") if s.strip()]
SCHEDULE_HOUR = int(os.getenv("SCHEDULE_HOUR", "17"))  # default 5 PM Taiwan time

TWSE_BASE = "https://openapi.twse.com.tw/v1"
REQUEST_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) DataPipeline/1.0",
}

# Industry suffixes for income statement & balance sheet endpoints
INDUSTRY_SUFFIXES = ["ci", "fh", "basi", "bd", "ins", "mim"]
#  ci=一般業, fh=金控業, basi=金融業, bd=證券期貨業, ins=保險業, mim=異業

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("mops")


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


# --------------- API helpers ---------------
def fetch_json(endpoint: str) -> list | None:
    """Fetch a TWSE OpenAPI JSON endpoint. Returns list of dicts or None."""
    url = f"{TWSE_BASE}{endpoint}"
    try:
        resp = requests.get(url, headers=REQUEST_HEADERS, timeout=30, verify=False)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            log.warning(f"{endpoint}: returned empty data")
            return None
        log.info(f"{endpoint}: got {len(data)} records")
        return data
    except requests.exceptions.RequestException as e:
        log.error(f"{endpoint}: request failed — {e}")
        return None
    except ValueError as e:
        log.error(f"{endpoint}: JSON parse failed — {e}")
        return None


def parse_num(s) -> float:
    """Parse a number string like '317,656,613' or '35.92' to float."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return 0.0
    try:
        cleaned = re.sub(r"[,\s%]", "", str(s).strip())
        if not cleaned or cleaned in ("-", "不適用", "N/A", "--", ""):
            return 0.0
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0


# --------------- Monthly Revenue (月營收) ---------------
def scrape_monthly_revenue(db, stock_ids):
    """
    Fetch latest monthly revenue from TWSE OpenAPI t187ap05_L.
    Returns all listed companies' revenue for the most recent month.
    """
    log.info("=== Scraping monthly revenue (t187ap05_L) ===")
    stock_set = set(stock_ids)

    data = fetch_json("/opendata/t187ap05_L")
    if not data:
        log.warning("No revenue data returned from API")
        return

    ops = []
    for row in data:
        sid = str(row.get("公司代號", "")).strip()
        if sid not in stock_set:
            continue

        period_str = str(row.get("資料年月", "")).strip()  # e.g. "11402" → 民國114年2月
        revenue = parse_num(row.get("營業收入-當月營收"))
        last_month_rev = parse_num(row.get("營業收入-上月營收"))
        last_year_rev = parse_num(row.get("營業收入-去年當月營收"))
        mom_pct = parse_num(row.get("營業收入-上月比較增減(%)"))
        yoy_pct = parse_num(row.get("營業收入-去年同月增減(%)"))
        cum_revenue = parse_num(row.get("累計營業收入-當月累計營收"))
        last_year_cum = parse_num(row.get("累計營業收入-去年累計營收"))

        # Parse period: "11402" → tw_year=114, month=02
        if len(period_str) >= 4:
            tw_year = int(period_str[:-2]) if len(period_str) > 2 else 0
            tw_month = int(period_str[-2:]) if len(period_str) >= 2 else 0
        else:
            tw_year, tw_month = 0, 0

        period = f"{tw_year}/{tw_month:02d}"

        doc = {
            "stock_id": sid,
            "company_name": row.get("公司名稱", ""),
            "industry": row.get("產業別", ""),
            "period": period,
            "revenue": revenue * 1000,          # 千元 → 元
            "last_month_revenue": last_month_rev * 1000,
            "last_year_revenue": last_year_rev * 1000,
            "mom_pct": mom_pct,
            "yoy": yoy_pct,
            "cum_revenue": cum_revenue * 1000,
            "last_year_cum_revenue": last_year_cum * 1000,
            "tw_year": tw_year,
            "tw_month": tw_month,
            "updated_at": datetime.now(),
        }

        ops.append(UpdateOne(
            {"stock_id": sid, "period": period},
            {"$set": doc},
            upsert=True,
        ))
        log.info(f"  [{sid}] {row.get('公司名稱', '')} Revenue {period}: {revenue * 1000:,.0f}")

    if ops:
        result = db["revenue"].bulk_write(ops)
        log.info(f"Revenue: upserted={result.upserted_count}, modified={result.modified_count}")
    else:
        log.warning("No matching revenue records for target stocks")


# --------------- Quarterly Financial (EPS + 每股淨值) ---------------
def scrape_financial(db, stock_ids):
    """
    Fetch latest quarterly financial data from TWSE OpenAPI:
      - t187ap06_L_* (綜合損益表) → EPS
      - t187ap07_L_* (資產負債表) → 每股參考淨值
    Must query multiple industry-specific endpoints to cover all stocks.
    """
    log.info("=== Scraping quarterly financials ===")
    stock_set = set(stock_ids)
    ops = []

    # --- EPS from 綜合損益表 ---
    log.info("--- Fetching income statements (EPS) ---")
    eps_found = {}

    for suffix in INDUSTRY_SUFFIXES:
        endpoint = f"/opendata/t187ap06_L_{suffix}"
        data = fetch_json(endpoint)
        if not data:
            continue

        for row in data:
            sid = str(row.get("公司代號", "")).strip()
            if sid not in stock_set or sid in eps_found:
                continue

            year_str = str(row.get("年度", "")).strip()
            season_str = str(row.get("季別", "")).strip()
            eps_val = parse_num(row.get("基本每股盈餘（元）"))

            if not year_str or not season_str:
                continue

            tw_year = int(year_str)
            season = int(season_str)
            ad_year = tw_year + 1911
            q_map = {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}
            period = f"{ad_year}{q_map.get(season, f'Q{season}')}"

            # Also grab other useful income fields
            revenue_q = parse_num(row.get("營業收入"))
            operating_income = parse_num(row.get("營業利益（損失）"))
            net_income = parse_num(row.get("本期淨利（淨損）"))
            net_income_parent = parse_num(row.get("淨利（淨損）歸屬於母公司業主"))

            eps_found[sid] = True

            doc = {
                "stock_id": sid,
                "company_name": row.get("公司名稱", ""),
                "period": period,
                "year": ad_year,
                "season": season,
                "eps": eps_val,
                "revenue": revenue_q * 1000,
                "operating_income": operating_income * 1000,
                "net_income": net_income * 1000,
                "net_income_parent": net_income_parent * 1000,
                "updated_at": datetime.now(),
            }

            ops.append(UpdateOne(
                {"stock_id": sid, "period": period},
                {"$set": doc},
                upsert=True,
            ))
            log.info(f"  [{sid}] {row.get('公司名稱', '')} EPS {period}: {eps_val}")

        time.sleep(1)

    # --- 每股參考淨值 from 資產負債表 ---
    log.info("--- Fetching balance sheets (NAV per share) ---")
    nav_found = {}

    for suffix in INDUSTRY_SUFFIXES:
        endpoint = f"/opendata/t187ap07_L_{suffix}"
        data = fetch_json(endpoint)
        if not data:
            continue

        for row in data:
            sid = str(row.get("公司代號", "")).strip()
            if sid not in stock_set or sid in nav_found:
                continue

            year_str = str(row.get("年度", "")).strip()
            season_str = str(row.get("季別", "")).strip()
            nav_val = parse_num(row.get("每股參考淨值"))

            if not year_str or not season_str:
                continue

            tw_year = int(year_str)
            season = int(season_str)
            ad_year = tw_year + 1911
            q_map = {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}
            period = f"{ad_year}{q_map.get(season, f'Q{season}')}"

            nav_found[sid] = True

            # Also grab balance sheet highlights
            total_assets = parse_num(row.get("資產總額"))
            total_liabilities = parse_num(row.get("負債總額"))
            total_equity = parse_num(row.get("權益總額"))

            ops.append(UpdateOne(
                {"stock_id": sid, "period": period},
                {"$set": {
                    "nav_per_share": nav_val,
                    "total_assets": total_assets * 1000,
                    "total_liabilities": total_liabilities * 1000,
                    "total_equity": total_equity * 1000,
                    "updated_at": datetime.now(),
                }},
                upsert=True,
            ))
            log.info(f"  [{sid}] {row.get('公司名稱', '')} NAV {period}: {nav_val}")

        time.sleep(1)

    if ops:
        result = db["financial"].bulk_write(ops)
        log.info(f"Financial: upserted={result.upserted_count}, modified={result.modified_count}")
    else:
        log.warning("No matching financial records for target stocks")


# --------------- Orchestration ---------------
def run_all(db):
    log.info(f"Starting scrape for stocks: {STOCK_IDS}")

    scrape_monthly_revenue(db, STOCK_IDS)
    scrape_financial(db, STOCK_IDS)

    # Log completion
    db["scrape_log"].insert_one({
        "timestamp": datetime.now(),
        "stocks": STOCK_IDS,
        "status": "completed",
    })
    log.info("Scrape complete!")


def main():
    db = connect_db()

    if "--watch" in sys.argv:
        import schedule as sched

        log.info(f"Watch mode: running now, then daily at {SCHEDULE_HOUR}:00")
        run_all(db)

        sched.every().day.at(f"{SCHEDULE_HOUR:02d}:00").do(run_all, db)

        while True:
            sched.run_pending()
            time.sleep(60)
    else:
        run_all(db)


if __name__ == "__main__":
    main()
