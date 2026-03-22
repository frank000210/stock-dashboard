"""
MOPS Scraper — Fetches monthly revenue & quarterly financial statements
from mops.twse.com.tw and stores them in MongoDB.

Run locally (residential IP) since MOPS blocks cloud/datacenter IPs.

Usage:
  pip install -r requirements.txt
  cp .env.example .env   # fill in MONGODB_URI and STOCK_IDS
  python mops_scraper.py          # one-time run
  python mops_scraper.py --watch  # run every 6 hours
"""

import os
import re
import sys
import time
import json
import logging
from datetime import datetime, timedelta

import requests
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────
MONGODB_URI = os.getenv("MONGODB_URI", "")
STOCK_IDS = [s.strip() for s in os.getenv("STOCK_IDS", "2330").split(",") if s.strip()]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("mops_scraper")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8",
    "Origin": "https://mops.twse.com.tw",
    "Referer": "https://mops.twse.com.tw/mops/",
    "Content-Type": "application/json",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def connect_db():
    """Connect to MongoDB and return the database handle."""
    if not MONGODB_URI:
        log.error("MONGODB_URI not set. Copy .env.example to .env and fill it in.")
        sys.exit(1)
    client = MongoClient(MONGODB_URI)
    db = client["stock_dashboard"]
    # Create indexes
    db["revenue"].create_index([("stock_id", 1), ("period", 1)], unique=True)
    db["financial"].create_index([("stock_id", 1), ("period", 1)], unique=True)
    db["scrape_log"].create_index([("stock_id", 1), ("type", 1)])
    log.info("Connected to MongoDB")
    return db


# ── Revenue Scraper ─────────────────────────────────────────────────
def to_num(s):
    """Parse a number string like '317,656,613' → int."""
    if not s:
        return 0
    return int(re.sub(r"[,\s]", "", str(s))) if re.sub(r"[,\s]", "", str(s)).lstrip("-").isdigit() else 0


def scrape_revenue(db, stock_id, months=15):
    """Scrape monthly revenue for a stock from MOPS and upsert into MongoDB."""
    log.info(f"[{stock_id}] Scraping monthly revenue (last {months} months)...")
    now = datetime.now()
    ops = []

    for i in range(months):
        d = datetime(now.year, now.month, 1) - timedelta(days=i * 30)
        d = datetime(d.year, d.month, 1)  # normalize to 1st of month
        tw_year = d.year - 1911
        month = f"{d.month:02d}"

        try:
            resp = SESSION.post(
                "https://mops.twse.com.tw/mops/api/t05st10_ifrs",
                json={
                    "companyId": stock_id,
                    "dataType": "2",
                    "month": month,
                    "year": str(tw_year),
                    "subsidiaryCompanyId": "",
                },
                timeout=15,
            )
            data = resp.json()

            if data.get("code") != 200:
                log.debug(f"[{stock_id}] {tw_year}/{month}: code={data.get('code')}")
                continue

            result = data.get("result", {})
            rows = result.get("data", [])
            yymm = result.get("yymm", "")

            month_row = next((r for r in rows if r[0] == "本月"), None)
            cum_row = next((r for r in rows if r[0] == "累計"), None)
            yoy_row = next((r for r in rows if r[0] and "去年同期" in r[0]), None)

            if month_row and to_num(month_row[1]) > 0:
                tw_y = yymm[: len(yymm) - 2]
                tw_m = yymm[-2:]
                period = f"{tw_y}/{tw_m}"

                doc = {
                    "stock_id": stock_id,
                    "period": period,
                    "revenue": to_num(month_row[1]) * 1000,
                    "cum_revenue": to_num(cum_row[1]) * 1000 if cum_row else 0,
                    "yoy": float(yoy_row[1]) if yoy_row and yoy_row[1] else 0,
                    "tw_year": int(tw_y),
                    "tw_month": int(tw_m),
                    "updated_at": datetime.utcnow(),
                }
                ops.append(
                    UpdateOne(
                        {"stock_id": stock_id, "period": period},
                        {"$set": doc},
                        upsert=True,
                    )
                )
                log.info(f"[{stock_id}] Revenue {period}: {doc['revenue']:,}")

        except Exception as e:
            log.warning(f"[{stock_id}] Revenue {tw_year}/{month} error: {e}")

        time.sleep(0.3)  # polite delay

    if ops:
        result = db["revenue"].bulk_write(ops)
        log.info(f"[{stock_id}] Revenue: upserted={result.upserted_count}, modified={result.modified_count}")
    else:
        log.warning(f"[{stock_id}] No revenue data scraped")

    # Update scrape log
    db["scrape_log"].update_one(
        {"stock_id": stock_id, "type": "revenue"},
        {"$set": {"last_run": datetime.utcnow(), "records": len(ops)}},
        upsert=True,
    )


# ── Financial Statements Scraper ────────────────────────────────────
def scrape_financial(db, stock_id, quarters=8):
    """Scrape quarterly financial statements from MOPS and upsert into MongoDB."""
    log.info(f"[{stock_id}] Scraping financial statements (last {quarters} quarters)...")
    now = datetime.now()
    tw_year = now.year - 1911
    ops = []

    # Build list of (year, season) newest first
    periods = []
    for y in range(tw_year, tw_year - 4, -1):
        for s in range(4, 0, -1):
            periods.append((y, s))

    found = 0
    for year, season in periods:
        if found >= quarters:
            break

        try:
            resp = SESSION.post(
                "https://mops.twse.com.tw/mops/api/t164sb04",
                json={
                    "companyId": stock_id,
                    "dataType": "2",
                    "year": str(year),
                    "season": f"{season:02d}",
                    "subsidiaryCompanyId": "",
                },
                timeout=20,
            )
            data = resp.json()

            if data.get("code") != 200:
                continue

            result = data.get("result", {})
            report_list = result.get("reportList", [])
            if not report_list:
                continue

            revenue = 0
            op_income = 0
            net_income = 0
            eps = 0.0

            for row in report_list:
                label = (row[0] or "").strip()
                val_str = re.sub(r"[,\s]", "", str(row[1])) if row[1] else "0"

                if label == "營業收入合計":
                    revenue = to_num(val_str) * 1000
                elif label == "營業利益（損失）":
                    op_income = to_num(val_str) * 1000
                elif "本期淨利" in label and "歸" not in label:
                    net_income = to_num(val_str) * 1000
                elif label.replace(" ", "") == "基本每股盈餘":
                    try:
                        eps = float(val_str)
                    except ValueError:
                        pass

            # Fallback EPS search
            if eps == 0:
                for row in report_list:
                    lbl = (row[0] or "").replace(" ", "").strip()
                    if lbl == "基本每股盈餘" and row[1]:
                        try:
                            eps = float(re.sub(r"[,\s]", "", str(row[1])))
                            if eps != 0:
                                break
                        except ValueError:
                            pass

            q_map = {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}
            period = f"{year + 1911}{q_map[season]}"

            if revenue > 0 or eps != 0:
                doc = {
                    "stock_id": stock_id,
                    "period": period,
                    "revenue": revenue,
                    "op_income": op_income,
                    "net_income": net_income,
                    "eps": eps,
                    "tw_year": year,
                    "season": season,
                    "updated_at": datetime.utcnow(),
                }
                ops.append(
                    UpdateOne(
                        {"stock_id": stock_id, "period": period},
                        {"$set": doc},
                        upsert=True,
                    )
                )
                found += 1
                log.info(f"[{stock_id}] Financial {period}: rev={revenue:,} eps={eps}")

        except Exception as e:
            log.warning(f"[{stock_id}] Financial {year}Q{season} error: {e}")

        time.sleep(0.5)  # polite delay

    if ops:
        result = db["financial"].bulk_write(ops)
        log.info(f"[{stock_id}] Financial: upserted={result.upserted_count}, modified={result.modified_count}")
    else:
        log.warning(f"[{stock_id}] No financial data scraped")

    db["scrape_log"].update_one(
        {"stock_id": stock_id, "type": "financial"},
        {"$set": {"last_run": datetime.utcnow(), "records": len(ops)}},
        upsert=True,
    )


# ── Main ────────────────────────────────────────────────────────────
def run_all(db):
    """Run all scrapers for all configured stock IDs."""
    log.info(f"Starting scrape for stocks: {STOCK_IDS}")
    for stock_id in STOCK_IDS:
        scrape_revenue(db, stock_id)
        scrape_financial(db, stock_id)
        time.sleep(1)
    log.info("Scrape complete!")


def main():
    db = connect_db()

    if "--watch" in sys.argv:
        import schedule as sched

        log.info("Watch mode: running every 6 hours")
        run_all(db)
        sched.every(6).hours.do(run_all, db)
        while True:
            sched.run_pending()
            time.sleep(60)
    else:
        run_all(db)


if __name__ == "__main__":
    main()
