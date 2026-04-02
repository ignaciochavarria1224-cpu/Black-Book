"""
Budget Black Book — Cloud Edition
"""

from __future__ import annotations

import json
import math
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
import yfinance as yf

try:
    import psycopg2
    import psycopg2.extras
    _PSYCOPG2_AVAILABLE = True
except ImportError:
    _PSYCOPG2_AVAILABLE = False


APP_TITLE = "Budget Black Book"
DB_PATH = Path(os.getenv("BUDGET_BLACK_BOOK_DB_PATH", "budget_black_book.db"))

if "DATABASE_URL" not in os.environ:
    try:
        _url = st.secrets.get("DATABASE_URL", "")
        if _url:
            os.environ["DATABASE_URL"] = _url
    except Exception:
        pass

DATABASE_URL: str = os.environ.get("DATABASE_URL", "")
IS_POSTGRES: bool = bool(DATABASE_URL) and _PSYCOPG2_AVAILABLE
DATE_FMT = "%Y-%m-%d"

DEFAULT_SETTINGS = {
    "daily_food_budget": "30", "pay_period_days": "14", "statement_day": "2",
    "due_day": "27", "savings_pct": "0.30", "spending_pct": "0.40",
    "crypto_pct": "0.10", "taxable_investing_pct": "0.10", "roth_ira_pct": "0.10",
    "debt_allocation_mode": "proportional", "migration_completed": "0", "last_price_refresh_at": "",
}
DEFAULT_ACCOUNTS = [
    {"name": "Checking",    "account_type": "cash",       "is_debt": 0, "include_in_runway": 1, "sort_order": 1},
    {"name": "Savings",     "account_type": "savings",    "is_debt": 0, "include_in_runway": 1, "sort_order": 2},
    {"name": "Savor",       "account_type": "credit",     "is_debt": 1, "include_in_runway": 0, "sort_order": 3},
    {"name": "Venture",     "account_type": "credit",     "is_debt": 1, "include_in_runway": 0, "sort_order": 4},
    {"name": "Coinbase",    "account_type": "investment", "is_debt": 0, "include_in_runway": 0, "sort_order": 5},
    {"name": "Roth IRA",    "account_type": "investment", "is_debt": 0, "include_in_runway": 0, "sort_order": 6},
    {"name": "Investments", "account_type": "investment", "is_debt": 0, "include_in_runway": 0, "sort_order": 7},
]
COMMON_CATEGORIES = [
    "Food", "Bills", "Subscriptions", "Income", "Debt Payment",
    "Gas", "Health", "Shopping", "Entertainment", "Savings",
    "Transfer", "Investing", "Other",
]
CRYPTO_NAME_TO_ID = {
    "XRP": "ripple", "Bitcoin (BTC)": "bitcoin", "Bittensor (TAO)": "bittensor",
    "Worldcoin (WLD)": "worldcoin-wld", "Sui (SUI)": "sui",
    "Solana (SOL)": "solana", "Cash (USD)": "",
}
STOCK_NAME_TO_TICKER = {
    "NVIDIA (NVDA)": "NVDA", "Palantir (PLTR)": "PLTR", "Tesla (TSLA)": "TSLA",
    "Invesco QQQ (QQQ)": "QQQ", "SPDR S&P 500 (SPY)": "SPY",
}

# Colour palette
C_GREEN  = "#00c896"
C_GOLD   = "#f0a500"
C_RED    = "#ff4d4d"
C_BLUE   = "#4da6ff"
C_PURPLE = "#a78bfa"
C_PINK   = "#f472b6"
C_DIM    = "#6b7280"
CHART_PALETTE = [C_GREEN, C_BLUE, C_GOLD, C_PURPLE, C_PINK, C_RED, "#34d399"]

st.set_page_config(page_title=APP_TITLE, page_icon="📖", layout="wide", initial_sidebar_state="expanded")


@dataclass
class Signal:
    level: str
    title: str
    body: str


# ── CSS ───────────────────────────────────────────────────────────────────────

def inject_css() -> None:
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700;900&family=JetBrains+Mono:wght@400;600&display=swap');

    .main > div { padding-top: 0.5rem; }

    section[data-testid="stSidebar"] {
        background: #060810;
        border-right: 1px solid rgba(255,255,255,0.04);
    }

    /* Title */
    .bb-title {
        font-family: 'Playfair Display', serif;
        font-size: 2.4rem;
        font-weight: 900;
        letter-spacing: 0.05em;
        color: #f0f0f0;
        line-height: 1;
        margin-bottom: 0;
    }
    .bb-subtitle {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.62rem;
        letter-spacing: 0.25em;
        color: #374151;
        text-transform: uppercase;
        margin-bottom: 1.2rem;
        border-bottom: 1px solid rgba(255,255,255,0.04);
        padding-bottom: 0.8rem;
    }

    /* Section headers */
    h2, h3 {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.65rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.18em !important;
        text-transform: uppercase !important;
        color: #374151 !important;
        margin-top: 1.5rem !important;
        margin-bottom: 0.5rem !important;
    }

    /* Metrics */
    [data-testid="stMetric"] {
        background: #0d1117;
        border: 1px solid rgba(255,255,255,0.05);
        border-radius: 2px;
        padding: 0.9rem 1rem 0.7rem 1rem;
        position: relative;
    }
    [data-testid="stMetric"]::before {
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(0,200,150,0.3), transparent);
    }
    [data-testid="stMetricLabel"] p {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.6rem !important;
        text-transform: uppercase;
        letter-spacing: 0.15em;
        color: #374151 !important;
    }
    [data-testid="stMetricValue"] {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 1.2rem !important;
        color: #e2e8f0 !important;
    }
    [data-testid="stMetricDelta"] {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.72rem !important;
    }

    /* Dataframes */
    [data-testid="stDataFrame"] {
        border: 1px solid rgba(255,255,255,0.05) !important;
        border-radius: 2px !important;
    }

    /* Buttons */
    [data-testid="baseButton-primary"] {
        border-radius: 2px !important;
        letter-spacing: 0.1em;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.72rem !important;
        text-transform: uppercase;
    }

    /* Sidebar */
    [data-testid="stRadio"] label {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.72rem !important;
        letter-spacing: 0.08em;
    }

    /* Sidebar caption */
    .bb-sidebar-brand {
        font-family: 'Playfair Display', serif;
        font-size: 1rem;
        font-weight: 700;
        color: #374151;
        letter-spacing: 0.05em;
    }
    </style>
    """, unsafe_allow_html=True)


# ── DB core ───────────────────────────────────────────────────────────────────

def get_connection():
    url = st.secrets.get("DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        st.error("DATABASE_URL is not configured.")
        st.stop()
    if "sslmode" not in url:
        url += "?sslmode=require"
    return psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)


def db_execute(conn, sql: str, params: tuple = ()) -> Any:
    if IS_POSTGRES:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur
    return conn.execute(re.sub(r"%s", "?", sql), params)


def _to_float_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0.0).astype(float)


def _cursor_to_df(cur) -> pd.DataFrame:
    """
    THE definitive fix for RealDictCursor + pandas.

    pd.read_sql_query with psycopg2 RealDictCursor iterates over dict KEYS
    instead of values, producing DataFrames where every cell contains the
    column name as a string. We bypass this by fetching rows ourselves and
    converting each RealDictRow to a plain Python dict before building the DF.
    """
    rows = cur.fetchall()
    if not rows:
        cols = [desc[0] for desc in cur.description] if cur.description else []
        return pd.DataFrame(columns=cols)
    return pd.DataFrame([dict(row) for row in rows])


# ── Schema ────────────────────────────────────────────────────────────────────

def init_db() -> None:
    serial = "SERIAL" if IS_POSTGRES else "INTEGER"
    ai = "" if IS_POSTGRES else "AUTOINCREMENT"
    ddl = [
        f"CREATE TABLE IF NOT EXISTS accounts (id {serial} PRIMARY KEY {ai}, name TEXT NOT NULL UNIQUE, account_type TEXT NOT NULL, is_debt INTEGER NOT NULL DEFAULT 0, include_in_runway INTEGER NOT NULL DEFAULT 1, starting_balance REAL NOT NULL DEFAULT 0, sort_order INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        f"CREATE TABLE IF NOT EXISTS transactions (id {serial} PRIMARY KEY {ai}, date TEXT NOT NULL, description TEXT NOT NULL, category TEXT NOT NULL, amount REAL NOT NULL, account_id INTEGER NOT NULL, type TEXT NOT NULL, to_account_id INTEGER, notes TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(account_id) REFERENCES accounts(id), FOREIGN KEY(to_account_id) REFERENCES accounts(id))",
        f"CREATE TABLE IF NOT EXISTS holdings (id {serial} PRIMARY KEY {ai}, symbol TEXT NOT NULL, display_name TEXT NOT NULL, asset_type TEXT NOT NULL, account_id INTEGER NOT NULL, amount_invested REAL NOT NULL DEFAULT 0, quantity REAL NOT NULL DEFAULT 0, avg_price REAL NOT NULL DEFAULT 0, coingecko_id TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(account_id) REFERENCES accounts(id))",
        f"CREATE TABLE IF NOT EXISTS allocation_snapshots (id {serial} PRIMARY KEY {ai}, paycheck_amount REAL NOT NULL, run_date TEXT NOT NULL, debt_total REAL NOT NULL, food_reserved REAL NOT NULL, debt_reserved REAL NOT NULL, savings_reserved REAL NOT NULL, spending_reserved REAL NOT NULL, crypto_reserved REAL NOT NULL, taxable_reserved REAL NOT NULL, roth_reserved REAL NOT NULL, debt_breakdown_json TEXT NOT NULL, meta_json TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS price_cache (symbol TEXT NOT NULL, asset_type TEXT NOT NULL, price REAL NOT NULL, previous_close REAL, currency TEXT NOT NULL DEFAULT 'USD', source TEXT NOT NULL, as_of_date TEXT NOT NULL, fetched_at TEXT NOT NULL, PRIMARY KEY(symbol, asset_type))",
        f"CREATE TABLE IF NOT EXISTS price_history (id {serial} PRIMARY KEY {ai}, symbol TEXT NOT NULL, asset_type TEXT NOT NULL, price REAL NOT NULL, previous_close REAL, as_of_date TEXT NOT NULL, source TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    ]
    conn = get_connection()
    try:
        for stmt in ddl:
            db_execute(conn, stmt)
        for k, v in DEFAULT_SETTINGS.items():
            db_execute(conn, "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT(key) DO NOTHING", (k, v))
        for a in DEFAULT_ACCOUNTS:
            db_execute(conn, "INSERT INTO accounts (name, account_type, is_debt, include_in_runway, starting_balance, sort_order) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT(name) DO NOTHING",
                       (a["name"], a["account_type"], a["is_debt"], a["include_in_runway"], 0.0, a["sort_order"]))
        conn.commit()
    finally:
        conn.close()


# ── Data access (all use _cursor_to_df — no pd.read_sql_query) ───────────────

def table_exists_with_rows(table_name: str) -> bool:
    conn = get_connection()
    try:
        row = db_execute(conn, f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    finally:
        conn.close()
    return bool(int(row["count"]))


def get_settings() -> dict[str, str]:
    conn = get_connection()
    try:
        rows = db_execute(conn, "SELECT key, value FROM settings").fetchall()
    finally:
        conn.close()
    return {str(r["key"]): str(r["value"]) for r in rows}


def set_settings(settings: dict[str, Any]) -> None:
    conn = get_connection()
    try:
        for k, v in settings.items():
            db_execute(conn, "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT(key) DO UPDATE SET value = excluded.value", (k, str(v)))
        conn.commit()
    finally:
        conn.close()


def load_accounts() -> pd.DataFrame:
    conn = get_connection()
    try:
        cur = db_execute(conn, "SELECT id, name, account_type, is_debt, include_in_runway, starting_balance, sort_order FROM accounts ORDER BY sort_order, name")
        df = _cursor_to_df(cur)
    finally:
        conn.close()
    if df.empty:
        return df
    for col in ("id", "is_debt", "include_in_runway", "sort_order"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    df["starting_balance"] = _to_float_series(df["starting_balance"])
    df["name"] = df["name"].astype(str)
    df["account_type"] = df["account_type"].astype(str)
    return df


def add_account(name: str, account_type: str, is_debt: int, include_in_runway: int) -> None:
    conn = get_connection()
    try:
        row = db_execute(conn, "SELECT COUNT(*) AS count FROM accounts").fetchone()
        sort_order = int(row["count"]) + 1
        db_execute(conn, "INSERT INTO accounts (name, account_type, is_debt, include_in_runway, starting_balance, sort_order) VALUES (%s, %s, %s, %s, 0.0, %s) ON CONFLICT(name) DO NOTHING",
                   (name, account_type, is_debt, include_in_runway, sort_order))
        conn.commit()
    finally:
        conn.close()


def add_transaction(tx_date: date, description: str, category: str, amount: float,
                    account_id: int, tx_type: str, to_account_id: int | None, notes: str) -> None:
    conn = get_connection()
    try:
        db_execute(conn, "INSERT INTO transactions (date, description, category, amount, account_id, type, to_account_id, notes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                   (tx_date.strftime(DATE_FMT), description.strip(), category, float(amount), int(account_id), tx_type,
                    int(to_account_id) if to_account_id else None, notes.strip() or None))
        conn.commit()
    finally:
        conn.close()


def load_transactions() -> pd.DataFrame:
    conn = get_connection()
    try:
        cur = db_execute(conn, """
            SELECT t.id, t.date, t.description, t.category, t.amount, t.type, t.notes,
                   a.name AS account, a.id AS account_id,
                   ta.name AS to_account, ta.id AS to_account_id
            FROM transactions t
            JOIN accounts a ON a.id = t.account_id
            LEFT JOIN accounts ta ON ta.id = t.to_account_id
            ORDER BY t.date DESC, t.id DESC""")
        df = _cursor_to_df(cur)
    finally:
        conn.close()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["amount"] = _to_float_series(df["amount"])
    df["account_id"] = pd.to_numeric(df["account_id"], errors="coerce").fillna(0).astype(int)
    df["to_account_id"] = pd.to_numeric(df["to_account_id"], errors="coerce")
    for col in ("description", "category", "type", "account"):
        df[col] = df[col].astype(str)
    return df


def load_holdings() -> pd.DataFrame:
    conn = get_connection()
    try:
        cur = db_execute(conn, """
            SELECT h.id, h.symbol, h.display_name, h.asset_type,
                   h.amount_invested, h.quantity, h.avg_price, h.coingecko_id,
                   a.name AS account, a.id AS account_id
            FROM holdings h JOIN accounts a ON a.id = h.account_id
            ORDER BY a.sort_order, h.display_name""")
        df = _cursor_to_df(cur)
    finally:
        conn.close()
    if df.empty:
        return df
    for col in ("amount_invested", "quantity", "avg_price"):
        df[col] = _to_float_series(df[col])
    df["account_id"] = pd.to_numeric(df["account_id"], errors="coerce").fillna(0).astype(int)
    for col in ("symbol", "display_name", "asset_type", "account"):
        df[col] = df[col].astype(str)
    return df


def save_allocation_snapshot(payload: dict[str, Any]) -> None:
    conn = get_connection()
    try:
        db_execute(conn, """INSERT INTO allocation_snapshots
            (paycheck_amount, run_date, debt_total, food_reserved, debt_reserved,
             savings_reserved, spending_reserved, crypto_reserved, taxable_reserved,
             roth_reserved, debt_breakdown_json, meta_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                   (payload["paycheck_amount"], payload["run_date"], payload["debt_total"],
                    payload["food_reserved"], payload["debt_reserved"], payload["savings_reserved"],
                    payload["spending_reserved"], payload["crypto_reserved"], payload["taxable_reserved"],
                    payload["roth_reserved"], json.dumps(payload["debt_breakdown"]), json.dumps(payload["meta"])))
        conn.commit()
    finally:
        conn.close()


def load_allocation_snapshots(limit: int = 10) -> pd.DataFrame:
    conn = get_connection()
    try:
        sql = "SELECT * FROM allocation_snapshots ORDER BY run_date DESC, id DESC LIMIT %s" if IS_POSTGRES \
              else "SELECT * FROM allocation_snapshots ORDER BY date(run_date) DESC, id DESC LIMIT ?"
        cur = db_execute(conn, sql, (limit,))
        df = _cursor_to_df(cur)
    finally:
        conn.close()
    if df.empty:
        return df
    for col in ("paycheck_amount", "debt_total", "food_reserved", "debt_reserved",
                "savings_reserved", "spending_reserved", "crypto_reserved", "taxable_reserved", "roth_reserved"):
        if col in df.columns:
            df[col] = _to_float_series(df[col])
    return df


def upsert_price(symbol: str, asset_type: str, price: float, previous_close: float | None, source: str, as_of_date: str) -> None:
    fetched_at = datetime.now().isoformat(timespec="seconds")
    conn = get_connection()
    try:
        db_execute(conn, """INSERT INTO price_cache (symbol, asset_type, price, previous_close, currency, source, as_of_date, fetched_at)
            VALUES (%s,%s,%s,%s,'USD',%s,%s,%s)
            ON CONFLICT(symbol, asset_type) DO UPDATE SET price=excluded.price,
            previous_close=excluded.previous_close, source=excluded.source,
            as_of_date=excluded.as_of_date, fetched_at=excluded.fetched_at""",
                   (symbol, asset_type, price, previous_close, source, as_of_date, fetched_at))
        db_execute(conn, """INSERT INTO price_history (symbol, asset_type, price, previous_close, as_of_date, source)
            SELECT %s,%s,%s,%s,%s,%s WHERE NOT EXISTS
            (SELECT 1 FROM price_history WHERE symbol=%s AND asset_type=%s AND as_of_date=%s)""",
                   (symbol, asset_type, price, previous_close, as_of_date, source, symbol, asset_type, as_of_date))
        conn.commit()
    finally:
        conn.close()


def load_price_cache() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = _cursor_to_df(db_execute(conn, "SELECT * FROM price_cache"))
    finally:
        conn.close()
    if df.empty:
        return df
    df["price"] = _to_float_series(df["price"])
    df["previous_close"] = _to_float_series(df["previous_close"])
    return df


def load_price_history() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = _cursor_to_df(db_execute(conn, "SELECT symbol, asset_type, price, previous_close, as_of_date FROM price_history ORDER BY as_of_date"))
    finally:
        conn.close()
    if df.empty:
        return df
    df["price"] = _to_float_series(df["price"])
    df["previous_close"] = _to_float_series(df["previous_close"])
    return df


# ── Excel migration ───────────────────────────────────────────────────────────

def excel_serial_to_date(value: Any) -> date | None:
    if pd.isna(value) or value in ("", None): return None
    if isinstance(value, datetime): return value.date()
    if isinstance(value, date): return value
    if isinstance(value, (int, float)): return (datetime(1899, 12, 30) + timedelta(days=float(value))).date()
    try: return pd.to_datetime(value).date()
    except Exception: return None


def detect_workbook() -> Path | None:
    for c in [Path.cwd() / "Budget Black Book copy.xlsx", Path.cwd() / "Budget Black Book.xlsx",
               Path.home() / "Downloads" / "Budget Black Book copy.xlsx", Path.home() / "Downloads" / "Budget Black Book.xlsx"]:
        if c.exists(): return c
    return None


def normalize_account_name(name: Any) -> str:
    value = str(name or "").strip()
    return {"Savor (CC)": "Savor", "Venture (CC)": "Venture", "Roth IRA (Fidelity)": "Roth IRA", "Investments (Fidelity)": "Investments"}.get(value, value)


def parse_home_settings(home_df: pd.DataFrame) -> tuple[dict[str, float], dict[str, str]]:
    su: dict[str, str] = {}; ab: dict[str, float] = {}
    lu = {"Daily Budget": ("daily_food_budget", "numeric"), "Checking — Starting Balance": ("Checking", "account"),
          "Savings — Starting Balance": ("Savings", "account"), "Savor (CC) — Starting Balance": ("Savor", "account"),
          "Venture (CC) — Starting Balance": ("Venture", "account"), "Coinbase — Starting Balance": ("Coinbase", "account"),
          "Roth IRA (Fidelity) — Starting Balance": ("Roth IRA", "account"), "Investments (Fidelity) — Starting Balance": ("Investments", "account")}
    for _, row in home_df.iterrows():
        label = str(row.iloc[2]).strip() if len(row) > 2 and pd.notna(row.iloc[2]) else ""
        value = row.iloc[4] if len(row) > 4 else None
        if label in lu and pd.notna(value):
            target, kind = lu[label]
            if kind == "numeric": su[target] = str(float(value))
            else: ab[target] = float(value)
    return ab, su


def ensure_account(conn, name: str) -> int:
    existing = db_execute(conn, "SELECT id FROM accounts WHERE name = %s", (name,)).fetchone()
    if existing: return int(existing["id"])
    fb = next((a for a in DEFAULT_ACCOUNTS if a["name"] == name), None)
    at = fb["account_type"] if fb else "cash"; isd = fb["is_debt"] if fb else 0
    inc = fb["include_in_runway"] if fb else 1; so = fb["sort_order"] if fb else 99
    if IS_POSTGRES:
        return int(db_execute(conn, "INSERT INTO accounts (name, account_type, is_debt, include_in_runway, starting_balance, sort_order) VALUES (%s,%s,%s,%s,0,%s) RETURNING id",
                              (name, at, isd, inc, so)).fetchone()["id"])
    return int(db_execute(conn, "INSERT INTO accounts (name, account_type, is_debt, include_in_runway, starting_balance, sort_order) VALUES (%s,%s,%s,%s,0,%s)",
                          (name, at, isd, inc, so)).lastrowid)


def migrate_from_excel_if_needed() -> str | None:
    if get_settings().get("migration_completed") == "1": return None
    wp = detect_workbook()
    if not wp: return None
    try:
        home_df = pd.read_excel(wp, sheet_name="Home", header=None, engine="openpyxl")
        spending_df = pd.read_excel(wp, sheet_name="Spending Log", header=4, engine="openpyxl")
        investments_df = pd.read_excel(wp, sheet_name="Investments", header=12, engine="openpyxl")
    except Exception as exc:
        return f"Import failed: {exc}"
    conn = get_connection()
    try:
        ab, su = parse_home_settings(home_df)
        for name, balance in ab.items():
            db_execute(conn, "UPDATE accounts SET starting_balance = %s WHERE id = %s", (float(balance), ensure_account(conn, name)))
        for k, v in su.items():
            db_execute(conn, "INSERT INTO settings (key, value) VALUES (%s,%s) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (k, v))
        if not table_exists_with_rows("transactions"):
            spending_df.columns = [str(c).strip() for c in spending_df.columns]
            spending_df = spending_df.dropna(how="all")
            for _, row in spending_df.iterrows():
                tx_date = excel_serial_to_date(row.get("Date")); desc = str(row.get("Description") or "").strip()
                cat = str(row.get("Category") or "Other").strip(); amount = row.get("Amount")
                acct = normalize_account_name(row.get("Account")); ttype = str(row.get("Type") or "Expense").strip()
                to_acct = normalize_account_name(row.get("To Account"))
                if not tx_date or not desc or pd.isna(amount) or not acct: continue
                aid = ensure_account(conn, acct); taid = ensure_account(conn, to_acct) if to_acct else None
                db_execute(conn, "INSERT INTO transactions (date, description, category, amount, account_id, type, to_account_id, notes) VALUES (%s,%s,%s,%s,%s,%s,%s,NULL)",
                           (tx_date.strftime(DATE_FMT), desc, cat, float(amount), aid, ttype, taid))
        if not table_exists_with_rows("holdings"):
            investments_df.columns = [str(c).strip() for c in investments_df.columns]
            investments_df = investments_df.dropna(how="all").rename(columns={"TICKER / NAME":"Ticker / Name","ACCOUNT":"Account","AMOUNT ($)":"Amount ($)","HOLDING AMT":"Holding Amt","AVG PRICE":"Avg Price"})
            if {"Ticker / Name","Account","Amount ($)","Holding Amt","Avg Price"}.issubset(set(investments_df.columns)):
                for _, row in investments_df.iterrows():
                    name = str(row.get("Ticker / Name") or "").strip(); an = normalize_account_name(row.get("Account"))
                    if not name or not an: continue
                    ai_val = coerce_float(row.get("Amount ($)")); qty = coerce_float(row.get("Holding Amt")); ap = coerce_float(row.get("Avg Price"))
                    aid = ensure_account(conn, an)
                    if an == "Coinbase": at = "crypto" if "Cash" not in name else "cash"; sym = name; cg = CRYPTO_NAME_TO_ID.get(name, "")
                    else: at = "etf" if any(t in name for t in ("QQQ","SPY")) else "stock"; sym = STOCK_NAME_TO_TICKER.get(name, name); cg = None
                    db_execute(conn, "INSERT INTO holdings (symbol, display_name, asset_type, account_id, amount_invested, quantity, avg_price, coingecko_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                               (sym, name, at, aid, ai_val, qty, ap, cg))
        db_execute(conn, "INSERT INTO settings (key, value) VALUES ('migration_completed','1') ON CONFLICT(key) DO UPDATE SET value='1'")
        conn.commit()
    finally:
        conn.close()
    return f"Imported workbook from `{wp}`."


# ── Utility ───────────────────────────────────────────────────────────────────

def coerce_float(value: Any, fallback: float = 0.0) -> float:
    try:
        r = float(value)
        return fallback if math.isnan(r) else r
    except Exception:
        return fallback


def format_currency(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}${abs(value):,.2f}"


def format_percent(value: float) -> str:
    return f"{value * 100:.1f}%"


def safe_div(num: float, denom: float) -> float:
    return num / denom if denom else 0.0


def get_setting_float(settings: dict[str, str], key: str) -> float:
    return coerce_float(settings.get(key), 0.0)


def _chart_theme(fig, title: str = "") -> object:
    """Apply Black Book dark chart theme."""
    fig.update_layout(
        title=dict(text=title, font=dict(family="JetBrains Mono, monospace", size=10, color="#374151"),
                   x=0, xanchor="left", pad=dict(l=0, b=8)) if title else dict(text=""),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#6b7280", size=10, family="JetBrains Mono, monospace"),
        legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="rgba(255,255,255,0.04)",
                    borderwidth=1, font=dict(size=9, family="JetBrains Mono, monospace")),
        margin=dict(l=0, r=0, t=28 if title else 4, b=0),
        hoverlabel=dict(bgcolor="#0d1117", bordercolor="rgba(255,255,255,0.1)",
                        font=dict(family="JetBrains Mono, monospace", size=10)),
    )
    fig.update_xaxes(
        gridcolor="rgba(255,255,255,0.03)", zerolinecolor="rgba(255,255,255,0.05)",
        tickfont=dict(size=9, family="JetBrains Mono, monospace"), showline=False,
    )
    fig.update_yaxes(
        gridcolor="rgba(255,255,255,0.03)", zerolinecolor="rgba(255,255,255,0.05)",
        tickfont=dict(size=9, family="JetBrains Mono, monospace"), showline=False,
    )
    return fig


def _pie_chart(labels, values, title="") -> go.Figure:
    fig = go.Figure(go.Pie(
        labels=labels, values=values,
        hole=0.6,
        marker=dict(colors=CHART_PALETTE, line=dict(color="#060810", width=2)),
        textfont=dict(family="JetBrains Mono, monospace", size=9),
        textposition="outside",
        hovertemplate="<b>%{label}</b><br>%{value:,.2f}<br>%{percent}<extra></extra>",
    ))
    _chart_theme(fig, title)
    fig.update_layout(showlegend=True, legend=dict(orientation="v", x=1.02, y=0.5))
    return fig


def _bar_chart(x, y, color=C_GREEN, title="", hline=None) -> go.Figure:
    fig = go.Figure(go.Bar(
        x=x, y=y,
        marker=dict(
            color=color,
            opacity=0.85,
            line=dict(width=0),
        ),
        hovertemplate="%{x}<br><b>$%{y:,.2f}</b><extra></extra>",
    ))
    if hline is not None:
        fig.add_hline(y=hline, line_dash="dot", line_color=C_GOLD, line_width=1,
                      annotation_text="cap", annotation_font=dict(color=C_GOLD, size=8, family="JetBrains Mono, monospace"),
                      annotation_position="top right")
    _chart_theme(fig, title)
    return fig


def _line_chart(x, y, color=C_GREEN, title="") -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=x, y=y, mode="lines",
        line=dict(color=color, width=2),
        fill="tozeroy",
        fillcolor=color.replace(")", ", 0.08)").replace("rgb(", "rgba(") if "rgb" in color else f"rgba(0,200,150,0.06)",
        hovertemplate="<b>$%{y:,.2f}</b><extra></extra>",
    ))
    _chart_theme(fig, title)
    return fig


# ── Business logic ────────────────────────────────────────────────────────────

def build_enriched_holdings(holdings_df: pd.DataFrame, price_cache_df: pd.DataFrame) -> pd.DataFrame:
    if holdings_df.empty: return holdings_df
    enriched = holdings_df.copy()
    for col in ("amount_invested", "quantity", "avg_price"):
        enriched[col] = _to_float_series(enriched[col])
    price_map = {} if price_cache_df.empty else {(r["symbol"], r["asset_type"]): r for _, r in price_cache_df.iterrows()}
    lp, pc, ps, fa = [], [], [], []
    for _, row in enriched.iterrows():
        cr = price_map.get((row["symbol"], row["asset_type"]))
        if cr is not None:
            lp.append(float(cr["price"])); pc.append(coerce_float(cr["previous_close"], float(cr["price"])))
            ps.append(cr["source"]); fa.append(cr["fetched_at"])
        else:
            fb = 1.0 if row["asset_type"] == "cash" and row["display_name"] == "Cash (USD)" else float(row["avg_price"] or 0)
            lp.append(fb); pc.append(fb); ps.append("fallback"); fa.append("")
    enriched["latest_price"] = lp; enriched["previous_close"] = pc
    enriched["price_source"] = ps; enriched["fetched_at"] = fa
    enriched["current_value"] = enriched["quantity"] * enriched["latest_price"]
    enriched["current_value"] = enriched["current_value"].where(enriched["quantity"] > 0, enriched["amount_invested"])
    enriched["total_pnl"] = enriched["current_value"] - enriched["amount_invested"]
    enriched["total_pnl_pct"] = enriched.apply(lambda r: safe_div(r["total_pnl"], r["amount_invested"]) if r["amount_invested"] else 0.0, axis=1)
    enriched["tdy_pnl"] = (enriched["latest_price"] - enriched["previous_close"]) * enriched["quantity"]
    return enriched


def build_account_balances(accounts_df: pd.DataFrame, transactions_df: pd.DataFrame, holdings_df: pd.DataFrame, price_cache_df: pd.DataFrame) -> pd.DataFrame:
    balances = accounts_df.copy()
    balances["current_balance"] = _to_float_series(balances["starting_balance"])
    if transactions_df.empty:
        tx_df = pd.DataFrame(columns=["id", "date", "account_id", "to_account_id", "type", "amount"])
    else:
        tx_df = transactions_df.copy(); tx_df["amount"] = _to_float_series(tx_df["amount"])
    debt_ids = set(balances.loc[balances["is_debt"] == 1, "id"].astype(int))
    cb = {int(r["id"]): coerce_float(r["starting_balance"]) for _, r in balances.iterrows()}
    for _, tx in tx_df.sort_values(by=["date", "id"]).iterrows():
        aid = int(tx["account_id"]); taid = int(tx["to_account_id"]) if pd.notna(tx["to_account_id"]) else None
        amt = float(tx["amount"]); tt = str(tx["type"])
        if aid in debt_ids:
            if tt == "Expense": cb[aid] += amt
            elif tt == "Income": cb[aid] -= amt
            elif tt == "Transfer": cb[aid] += amt
        else:
            if tt == "Expense": cb[aid] -= amt
            elif tt == "Income": cb[aid] += amt
            elif tt == "Transfer": cb[aid] -= amt
        if taid: cb[taid] = cb.get(taid, 0.0) + (-amt if taid in debt_ids else amt)
    balances["current_balance"] = balances["id"].map(cb).astype(float)
    hv = {}
    if not holdings_df.empty:
        enriched = build_enriched_holdings(holdings_df, price_cache_df)
        hv = enriched.groupby("account_id", dropna=False)["current_value"].sum().to_dict()
    for idx, row in balances.iterrows():
        balances.at[idx, "display_balance"] = hv[row["id"]] if row["account_type"] == "investment" and row["id"] in hv else row["current_balance"]
    return balances


def build_food_metrics(transactions_df: pd.DataFrame, settings: dict[str, str]) -> dict[str, Any]:
    db = get_setting_float(settings, "daily_food_budget")
    today = date.today(); ws = today - timedelta(days=today.weekday())
    food_df = transactions_df.loc[transactions_df["category"].eq("Food")].copy() if not transactions_df.empty else pd.DataFrame()
    ts = ws_spent = tfs = cc = ls = adf = 0.0; ad = 0
    if not food_df.empty:
        food_df["date"] = pd.to_datetime(food_df["date"]).dt.date
        ts = float(food_df.loc[food_df["date"] == today, "amount"].sum())
        ws_spent = float(food_df.loc[food_df["date"] >= ws, "amount"].sum())
        tfs = float(food_df["amount"].sum())
        all_days = pd.date_range(start=min(food_df["date"].min(), today), end=today, freq="D")
        ds = food_df.groupby("date")["amount"].sum().reindex(all_days.date, fill_value=0.0)
        carry = life = 0.0
        for x in ds: carry += db - float(x); life += max(db - float(x), 0.0)
        cc = carry; ls = life; ad = len(all_days); adf = safe_div(tfs, ad)
    return {
        "daily_budget": db, "weekly_budget": db * 7,
        "food_spent_today": ts, "food_spent_week": ws_spent,
        "remaining_today": db - ts, "remaining_week": (db * 7) - ws_spent,
        "current_carry_surplus": cc, "lifetime_surplus": ls,
        "avg_daily_food_spend": adf, "food_days_tracked": ad,
        "transactions_today": int(0 if transactions_df.empty else (pd.to_datetime(transactions_df["date"]).dt.date == today).sum()),
    }


def build_runway(transactions_df: pd.DataFrame, balances_df: pd.DataFrame, food_metrics: dict[str, Any]) -> dict[str, float]:
    lc = float(balances_df.loc[balances_df["include_in_runway"] == 1, "display_balance"].sum())
    if transactions_df.empty:
        avg = food_metrics["avg_daily_food_spend"]
    else:
        sd = transactions_df.loc[transactions_df["type"].eq("Expense")].copy()
        sd["date"] = pd.to_datetime(sd["date"]).dt.date
        td = sd.loc[sd["date"] >= date.today() - timedelta(days=29)]
        avg = float(td["amount"].sum()) / 30.0 if not td.empty else food_metrics["avg_daily_food_spend"]
    return {"liquid_cash": lc, "avg_daily_spending": avg, "runway_days": safe_div(lc, avg)}


def build_debt_summary(balances_df: pd.DataFrame) -> dict[str, Any]:
    ddf = balances_df.loc[balances_df["is_debt"] == 1, ["id", "name", "display_balance"]].copy()
    ddf["display_balance"] = _to_float_series(ddf["display_balance"]).clip(lower=0)
    return {"total_debt": float(ddf["display_balance"].sum()), "by_account": ddf.sort_values("display_balance", ascending=False)}


def build_signals(balances_df, debt_summary, food_metrics, runway, settings) -> list[Signal]:
    signals: list[Signal] = []
    checking = float(balances_df.loc[balances_df["name"].eq("Checking"), "display_balance"].sum())
    due_day = int(get_setting_float(settings, "due_day") or 27)
    stmt_day = int(get_setting_float(settings, "statement_day") or 2)
    today = date.today()
    dd = date(today.year, today.month, min(due_day, 28))
    if today.day > due_day:
        nm = today.replace(day=28) + timedelta(days=4); dd = date(nm.year, nm.month, min(due_day, 28))
    dtd = (dd - today).days
    if food_metrics["remaining_today"] < 0: signals.append(Signal("danger", "⚠ Food overspent today", "Over the daily cap."))
    elif food_metrics["remaining_week"] < 0: signals.append(Signal("warning", "⚠ Food budget behind", "Weekly spend over budget."))
    if debt_summary["total_debt"] > runway["liquid_cash"] * 0.75 and debt_summary["total_debt"] > 0:
        signals.append(Signal("danger", "⚠ Debt pressure high", "Debt large relative to cash. Keep next paycheck debt-heavy."))
    elif debt_summary["total_debt"] > 0:
        signals.append(Signal("warning", "⚠ Debt needs room", "Paycheck engine will reserve a payment first."))
    if dtd <= 5 and debt_summary["total_debt"] > 0: signals.append(Signal("warning", "📅 Payment due soon", f"Due in {dtd} day(s)."))
    if today.day <= stmt_day + 2 and debt_summary["total_debt"] > 0: signals.append(Signal("warning", "🧾 Statement window open", "Review balances before next allocation."))
    if checking < 50: signals.append(Signal("danger", "💸 Checking running low", "Below $50. Protect essentials."))
    if runway["runway_days"] < 14: signals.append(Signal("danger", "🛣 Runway short", "Under two weeks of runway."))
    elif runway["runway_days"] < 30: signals.append(Signal("warning", "🛣 Runway needs work", "Under a month of runway."))
    if food_metrics["current_carry_surplus"] > food_metrics["daily_budget"] * 3: signals.append(Signal("success", "🍽 Food discipline paying off", "Healthy carry surplus built."))
    if not signals: signals.append(Signal("success", "💰 Budget steady", "No pressure signals. Keep logging."))
    return sorted(signals, key=lambda s: {"danger": 0, "warning": 1, "success": 2}[s.level])


def compute_paycheck_allocation(paycheck_amount: float, settings: dict[str, str], debt_df: pd.DataFrame) -> dict[str, Any]:
    ppd = int(get_setting_float(settings, "pay_period_days") or 14)
    fr = max(get_setting_float(settings, "daily_food_budget") * ppd, 0.0)
    raf = max(paycheck_amount - fr, 0.0)
    td = float(debt_df["display_balance"].sum()) if not debt_df.empty else 0.0
    dr = min(td, raf); rad = max(raf - dr, 0.0)
    bd: list[dict[str, Any]] = []
    if td > 0 and dr > 0 and not debt_df.empty:
        tmp = debt_df.copy(); tmp["share"] = tmp["display_balance"] / td
        for _, r in tmp.iterrows():
            bd.append({"account_id": int(r["id"]), "account": r["name"], "debt_balance": float(r["display_balance"]), "allocation": float(dr * r["share"])})
    return {
        "paycheck_amount": paycheck_amount, "run_date": date.today().strftime(DATE_FMT),
        "debt_total": td, "food_reserved": fr, "debt_reserved": dr,
        "remaining_after_food": raf, "remaining_after_debt": rad,
        "savings_reserved": rad * get_setting_float(settings, "savings_pct"),
        "spending_reserved": rad * get_setting_float(settings, "spending_pct"),
        "crypto_reserved": rad * get_setting_float(settings, "crypto_pct"),
        "taxable_reserved": rad * get_setting_float(settings, "taxable_investing_pct"),
        "roth_reserved": rad * get_setting_float(settings, "roth_ira_pct"),
        "debt_breakdown": bd,
        "meta": {"pay_period_days": ppd, "allocation_mode": settings.get("debt_allocation_mode", "proportional")},
    }


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_yfinance_prices(symbols: tuple[str, ...]) -> dict[str, dict[str, float | None]]:
    prices: dict[str, dict[str, float | None]] = {}
    if not symbols: return prices
    tickers = yf.Tickers(" ".join(symbols))
    for symbol in symbols:
        info: dict[str, float | None] = {"price": None, "previous_close": None}
        try:
            hist = tickers.tickers[symbol].history(period="2d", interval="1d", auto_adjust=False)
            if not hist.empty:
                info["price"] = float(hist["Close"].iloc[-1])
                info["previous_close"] = float(hist["Close"].iloc[-2]) if len(hist) > 1 else float(hist["Close"].iloc[-1])
        except Exception: pass
        prices[symbol] = info
    return prices


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_coingecko_prices(ids: tuple[str, ...]) -> dict[str, dict[str, float | None]]:
    prices: dict[str, dict[str, float | None]] = {cid: {"price": None, "previous_close": None} for cid in ids}
    ids = tuple(cid for cid in ids if cid)
    if not ids: return prices
    r = requests.get("https://api.coingecko.com/api/v3/simple/price",
                     params={"ids": ",".join(ids), "vs_currencies": "usd", "include_24hr_change": "true"}, timeout=15)
    r.raise_for_status()
    for cid in ids:
        data = r.json().get(cid, {}); price = data.get("usd"); change = data.get("usd_24h_change")
        prev = None
        if price is not None and change is not None:
            try: prev = float(price) / (1 + float(change) / 100)
            except ZeroDivisionError: prev = float(price)
        prices[cid] = {"price": float(price) if price is not None else None, "previous_close": prev}
    return prices


def maybe_refresh_prices(holdings_df: pd.DataFrame, force: bool = False) -> tuple[bool, str]:
    if holdings_df.empty: return False, "No holdings to refresh yet."
    settings = get_settings(); now = datetime.now()
    mcp = now.time() >= time(hour=16, minute=15)
    ard = settings.get("last_price_refresh_at", "").startswith(date.today().strftime(DATE_FMT))
    if not (force or (mcp and not ard)): return False, "Using cached prices."
    stock_syms = tuple(sorted(set(holdings_df.loc[holdings_df["asset_type"].isin(["stock","etf"]),"symbol"].astype(str))))
    cr = holdings_df.loc[holdings_df["asset_type"].eq("crypto") & holdings_df["coingecko_id"].fillna("").ne("")]
    coin_ids = tuple(sorted(set(cr["coingecko_id"].astype(str))))
    sp = fetch_yfinance_prices(stock_syms); cp = {}
    if coin_ids:
        try: cp = fetch_coingecko_prices(coin_ids)
        except Exception: pass
    aod = date.today().strftime(DATE_FMT); refreshed = 0
    for _, row in holdings_df.iterrows():
        sym = str(row["symbol"]); at = str(row["asset_type"])
        if at in {"stock","etf"}: pi=sp.get(sym,{}); price=pi.get("price"); prev=pi.get("previous_close"); src="yfinance"
        elif at == "crypto": cid=str(row.get("coingecko_id") or ""); pi=cp.get(cid,{}); price=pi.get("price"); prev=pi.get("previous_close"); src="coingecko"
        else: price=row["avg_price"] or 1.0; prev=row["avg_price"] or 1.0; src="internal"
        if price is None: continue
        upsert_price(sym, at, float(price), float(prev) if prev else None, src, aod); refreshed += 1
    set_settings({"last_price_refresh_at": now.isoformat(timespec="seconds")})
    return True, f"Refreshed {refreshed} holding price(s)."


def build_net_worth(balances_df: pd.DataFrame) -> dict[str, float]:
    assets = float(balances_df.loc[balances_df["is_debt"] == 0, "display_balance"].sum())
    debt = float(balances_df.loc[balances_df["is_debt"] == 1, "display_balance"].clip(lower=0).sum())
    return {"assets": assets, "debt": debt, "net_worth": assets - debt}


def prepare_report_frames(transactions_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if transactions_df.empty: return {"spending": pd.DataFrame(), "food": pd.DataFrame()}
    tx = transactions_df.copy(); tx["date"] = pd.to_datetime(tx["date"])
    return {"spending": tx.loc[tx["type"] == "Expense"].copy(), "food": tx.loc[tx["category"] == "Food"].copy()}


# ── Renderers ─────────────────────────────────────────────────────────────────

def render_signal(signal: Signal) -> None:
    colors = {"danger": C_RED, "warning": C_GOLD, "success": C_GREEN}
    c = colors[signal.level]
    title_text = signal.title.split(" ", 1)[-1] if signal.title and signal.title[0] in "⚠📅🧾💸🛣🍽💰💳" else signal.title
    st.markdown(
        f'<div style="border-left:2px solid {c};padding:0.55rem 1rem;background:rgba(255,255,255,0.015);'
        f'margin-bottom:1rem;display:flex;gap:1rem;align-items:baseline">'
        f'<span style="font-family:JetBrains Mono,monospace;font-size:0.65rem;font-weight:700;'
        f'text-transform:uppercase;letter-spacing:0.12em;color:{c};white-space:nowrap">{title_text}</span>'
        f'<span style="font-size:0.78rem;color:#6b7280">{signal.body}</span></div>',
        unsafe_allow_html=True)


def render_dashboard(settings, transactions_df, holdings_df, balances_df, price_cache_df) -> None:
    food = build_food_metrics(transactions_df, settings)
    runway = build_runway(transactions_df, balances_df, food)
    debt = build_debt_summary(balances_df)
    net_worth = build_net_worth(balances_df)
    signals = build_signals(balances_df, debt, food, runway, settings)
    latest_allocation = load_allocation_snapshots(limit=1)

    # ── Title
    st.markdown('<div class="bb-title">Black Book</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="bb-subtitle">Personal Finance · {date.today().strftime("%B %d, %Y")}</div>', unsafe_allow_html=True)
    render_signal(signals[0])

    # ── Top metrics
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Daily Food Left", format_currency(food["remaining_today"]), format_currency(-food["food_spent_today"]))
    m2.metric("Weekly Food Left", format_currency(food["remaining_week"]), format_currency(-food["food_spent_week"]))
    m3.metric("Net Worth", format_currency(net_worth["net_worth"]))
    m4.metric("Runway", f"{runway['runway_days']:.0f} days", format_currency(runway["avg_daily_spending"]) + "/day")

    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Total Debt", format_currency(debt["total_debt"]))
    s2.metric("Food Surplus", format_currency(food["current_carry_surplus"]))
    s3.metric("Lifetime Surplus", format_currency(food["lifetime_surplus"]))
    s4.metric("Txns Today", str(food["transactions_today"]))

    left, right = st.columns([1.1, 0.9])

    with left:
        st.subheader("Accounts")
        sorted_bal = balances_df.sort_values("sort_order")
        acct_display = pd.DataFrame({
            "Account": [str(x) for x in sorted_bal["name"].tolist()],
            "Balance": [format_currency(float(x)) for x in sorted_bal["display_balance"].tolist()],
            "Type": ["Debt" if int(r["is_debt"]) else str(r["account_type"]).title() for _, r in sorted_bal.iterrows()],
        })
        st.dataframe(acct_display, use_container_width=True, hide_index=True)

        st.subheader("Recent Money Moves")
        recent = transactions_df.head(8).copy() if not transactions_df.empty else pd.DataFrame()
        if recent.empty:
            st.info("No transactions logged yet.")
        else:
            recent["date"] = pd.to_datetime(recent["date"]).dt.strftime("%Y-%m-%d")
            recent["amount"] = recent["amount"].map(format_currency)
            st.dataframe(recent[["date", "description", "category", "amount", "account", "type", "to_account"]],
                         use_container_width=True, hide_index=True)

    with right:
        reports = prepare_report_frames(transactions_df)

        if not reports["spending"].empty:
            st.subheader("Spending Mix")
            sc = reports["spending"].groupby("category", as_index=False)["amount"].sum()
            fig = _pie_chart(sc["category"].tolist(), sc["amount"].tolist())
            st.plotly_chart(fig, use_container_width=True)

        if not reports["food"].empty:
            st.subheader("Food Trend")
            daily_food = reports["food"].groupby(reports["food"]["date"].dt.date, as_index=False)["amount"].sum()
            fig = _bar_chart(daily_food["date"].tolist(), daily_food["amount"].tolist(),
                             color=C_GREEN, hline=get_setting_float(settings, "daily_food_budget"))
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Last Paycheck")
        if latest_allocation.empty:
            preview = compute_paycheck_allocation(580.0, settings, debt["by_account"])
            st.caption("Preview — no snapshot saved yet.")
        else:
            preview = latest_allocation.iloc[0].to_dict()
        alloc_df = pd.DataFrame([
            ("Food", preview["food_reserved"]), ("Debt", preview["debt_reserved"]),
            ("Savings", preview["savings_reserved"]), ("Spending", preview["spending_reserved"]),
            ("Crypto", preview["crypto_reserved"]), ("Taxable", preview.get("taxable_reserved", 0.0)),
            ("Roth IRA", preview.get("roth_reserved", 0.0)),
        ], columns=["Bucket", "Amount"])
        st.dataframe(alloc_df.assign(Amount=alloc_df["Amount"].map(format_currency)), use_container_width=True, hide_index=True)


def render_log_transaction(accounts_df: pd.DataFrame) -> None:
    st.markdown('<div class="bb-title">Log Transaction</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="bb-subtitle">Record a money move</div>', unsafe_allow_html=True)
    sorted_accts = accounts_df.sort_values("sort_order")
    names = [str(x) for x in sorted_accts["name"].tolist()]
    ids = [int(x) for x in sorted_accts["id"].tolist()]
    account_name_to_id = dict(zip(names, ids))
    with st.form("transaction_form", clear_on_submit=True):
        c1, c2, c3 = st.columns([1, 2, 1])
        tx_date = c1.date_input("Date", value=date.today())
        description = c2.text_input("Description", placeholder="Chick-fil-A, paycheck, rent...")
        amount = c3.number_input("Amount", min_value=0.0, step=0.01, format="%.2f")
        c4, c5, c6 = st.columns(3)
        category = c4.selectbox("Category", COMMON_CATEGORIES, index=0)
        account = c5.selectbox("Account", list(account_name_to_id))
        tx_type = c6.selectbox("Type", ["Expense", "Income", "Transfer"])
        to_account = None
        notes = st.text_area("Notes", placeholder="Optional note...")
        if tx_type == "Transfer":
            to_account = st.selectbox("To Account", [""] + [n for n in account_name_to_id if n != account])
        submitted = st.form_submit_button("Save Transaction", type="primary")
        if submitted:
            if not description.strip(): st.error("Description is required.")
            elif amount <= 0: st.error("Amount must be greater than zero.")
            elif tx_type == "Transfer" and not to_account: st.error("Transfers need a destination.")
            else:
                add_transaction(tx_date, description, category, float(amount),
                                int(account_name_to_id[account]), tx_type,
                                int(account_name_to_id[to_account]) if to_account else None, notes)
                st.success("Transaction saved."); st.rerun()


def render_paycheck_allocation(settings: dict[str, str], balances_df: pd.DataFrame) -> None:
    st.markdown('<div class="bb-title">Paycheck Allocation</div>', unsafe_allow_html=True)
    st.markdown('<div class="bb-subtitle">Break down your next deposit</div>', unsafe_allow_html=True)
    debt_df = build_debt_summary(balances_df)["by_account"]
    c1, c2 = st.columns([0.9, 1.1])
    with c1:
        paycheck_amount = st.number_input("Paycheck Amount", min_value=0.0, value=580.0, step=10.0, format="%.2f")
        allocation = compute_paycheck_allocation(float(paycheck_amount), settings, debt_df)
        st.metric("Debt Total", format_currency(allocation["debt_total"]))
        st.metric("Food Reserve", format_currency(allocation["food_reserved"]))
        st.metric("After Food", format_currency(allocation["remaining_after_food"]))
        st.metric("After Debt", format_currency(allocation["remaining_after_debt"]))
        if st.button("Save Snapshot", type="primary"):
            save_allocation_snapshot(allocation); st.success("Saved."); st.rerun()
    with c2:
        alloc_rows = [("Food", allocation["food_reserved"]), ("Debt", allocation["debt_reserved"]),
                      ("Savings", allocation["savings_reserved"]), ("Spending", allocation["spending_reserved"]),
                      ("Crypto", allocation["crypto_reserved"]), ("Taxable", allocation["taxable_reserved"]),
                      ("Roth IRA", allocation["roth_reserved"])]
        alloc_df = pd.DataFrame(alloc_rows, columns=["Bucket", "Amount"])
        alloc_df["Share"] = alloc_df["Amount"].apply(lambda x: safe_div(x, allocation["paycheck_amount"]))
        st.dataframe(alloc_df.assign(Amount=alloc_df["Amount"].map(format_currency), Share=alloc_df["Share"].map(format_percent)),
                     use_container_width=True, hide_index=True)
        if allocation["debt_breakdown"]:
            st.subheader("Debt Split")
            dbd = pd.DataFrame(allocation["debt_breakdown"])
            st.dataframe(dbd.assign(debt_balance=dbd["debt_balance"].map(format_currency), allocation=dbd["allocation"].map(format_currency))[["account","debt_balance","allocation"]],
                         use_container_width=True, hide_index=True)
    history = load_allocation_snapshots(limit=8)
    st.subheader("Recent Snapshots")
    if history.empty:
        st.info("Save a paycheck allocation to build your history.")
    else:
        display = history[["run_date","paycheck_amount","food_reserved","debt_reserved","savings_reserved","spending_reserved","crypto_reserved","taxable_reserved","roth_reserved"]].copy()
        for col in display.columns:
            if col != "run_date": display[col] = display[col].map(format_currency)
        st.dataframe(display, use_container_width=True, hide_index=True)


def render_investments(holdings_df: pd.DataFrame, price_cache_df: pd.DataFrame) -> None:
    st.markdown('<div class="bb-title">Investments</div>', unsafe_allow_html=True)
    st.markdown('<div class="bb-subtitle">Portfolio overview</div>', unsafe_allow_html=True)
    if holdings_df.empty:
        st.info("No holdings yet."); return
    refreshed, message = maybe_refresh_prices(holdings_df, force=False)
    if refreshed: price_cache_df = load_price_cache()
    if message: st.caption(message)
    _, c2 = st.columns([0.7, 0.3])
    with c2:
        if st.button("Refresh Prices", type="primary"):
            _, msg = maybe_refresh_prices(holdings_df, force=True); st.success(msg); st.rerun()
    enriched = build_enriched_holdings(holdings_df, price_cache_df)
    tv = float(enriched["current_value"].sum()); ti = float(enriched["amount_invested"].sum())
    tp = float(enriched["total_pnl"].sum()); tdp = float(enriched["tdy_pnl"].sum())
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Portfolio Value", format_currency(tv))
    m2.metric("Cost Basis", format_currency(ti))
    m3.metric("Total PnL", format_currency(tp), format_percent(safe_div(tp, ti)))
    m4.metric("Today PnL", format_currency(tdp))
    t1, t2, t3 = st.tabs(["Today", "Total PnL", "Prices"])
    with t1:
        v = enriched[["display_name","account","quantity","latest_price","previous_close","tdy_pnl"]].copy()
        v["latest_price"]=v["latest_price"].map(format_currency); v["previous_close"]=v["previous_close"].map(format_currency); v["tdy_pnl"]=v["tdy_pnl"].map(format_currency)
        st.dataframe(v, use_container_width=True, hide_index=True)
    with t2:
        v = enriched[["display_name","account","amount_invested","current_value","total_pnl","total_pnl_pct"]].copy()
        v["amount_invested"]=v["amount_invested"].map(format_currency); v["current_value"]=v["current_value"].map(format_currency)
        v["total_pnl"]=v["total_pnl"].map(format_currency); v["total_pnl_pct"]=v["total_pnl_pct"].map(format_percent)
        st.dataframe(v, use_container_width=True, hide_index=True)
    with t3:
        v = enriched[["display_name","symbol","account","latest_price","price_source","fetched_at"]].copy()
        v["latest_price"]=v["latest_price"].map(format_currency)
        st.dataframe(v, use_container_width=True, hide_index=True)
    st.subheader("Allocation")
    ac1, ac2 = st.columns(2)
    with ac1:
        ba = enriched.groupby("account", as_index=False)["current_value"].sum()
        fig = _pie_chart(ba["account"].tolist(), ba["current_value"].tolist(), "By Account")
        st.plotly_chart(fig, use_container_width=True)
    with ac2:
        bv = enriched.groupby("asset_type", as_index=False)["current_value"].sum()
        fig = _pie_chart(bv["asset_type"].tolist(), bv["current_value"].tolist(), "By Asset Type")
        st.plotly_chart(fig, use_container_width=True)
    history = load_price_history()
    if not history.empty:
        vh = history.merge(holdings_df[["symbol","asset_type","quantity"]], on=["symbol","asset_type"], how="left")
        vh["quantity"] = vh["quantity"].fillna(0.0); vh["portfolio_value"] = vh["price"] * vh["quantity"]
        merged = vh.groupby("as_of_date", as_index=False)["portfolio_value"].sum()
        fig = _line_chart(merged["as_of_date"].tolist(), merged["portfolio_value"].tolist(), title="Portfolio Value")
        st.plotly_chart(fig, use_container_width=True)
    st.download_button("Export Holdings CSV", data=enriched.to_csv(index=False).encode("utf-8"), file_name="holdings.csv", mime="text/csv")


def render_reports(settings, transactions_df, holdings_df, price_cache_df) -> None:
    st.markdown('<div class="bb-title">Reports</div>', unsafe_allow_html=True)
    st.markdown('<div class="bb-subtitle">Spending analysis</div>', unsafe_allow_html=True)
    if transactions_df.empty:
        st.info("Log transactions to see reports."); return
    tx = transactions_df.copy(); tx["date"] = pd.to_datetime(tx["date"])
    min_d, max_d = tx["date"].min().date(), tx["date"].max().date()
    c1, c2 = st.columns(2)
    start_date = c1.date_input("From", value=min_d, min_value=min_d, max_value=max_d)
    end_date = c2.date_input("To", value=max_d, min_value=min_d, max_value=max_d)
    filtered = tx.loc[(tx["date"].dt.date >= start_date) & (tx["date"].dt.date <= end_date)].copy()
    expense_df = filtered.loc[filtered["type"] == "Expense"].copy()
    food_df = filtered.loc[filtered["category"] == "Food"].copy()
    ch1, ch2 = st.columns(2)
    with ch1:
        if not expense_df.empty:
            bc = expense_df.groupby("category", as_index=False)["amount"].sum()
            fig = _pie_chart(bc["category"].tolist(), bc["amount"].tolist(), "Spending by Category")
            st.plotly_chart(fig, use_container_width=True)
    with ch2:
        if not food_df.empty:
            ft = food_df.groupby(food_df["date"].dt.date, as_index=False)["amount"].sum()
            fig = _bar_chart(ft["date"].tolist(), ft["amount"].tolist(), color=C_GREEN,
                             title="Food Trend", hline=get_setting_float(settings, "daily_food_budget"))
            st.plotly_chart(fig, use_container_width=True)
    ch3, ch4 = st.columns(2)
    with ch3:
        if not expense_df.empty:
            wb = get_setting_float(settings, "daily_food_budget") * 7
            wg = expense_df.loc[expense_df["category"] == "Food"].copy()
            wg["week"] = wg["date"].dt.to_period("W").astype(str)
            fw = wg.groupby("week", as_index=False)["amount"].sum()
            if not fw.empty:
                fig = go.Figure()
                fig.add_bar(x=fw["week"].tolist(), y=fw["amount"].tolist(), name="Food", marker_color=C_GREEN, marker_opacity=0.85)
                fig.add_scatter(x=fw["week"].tolist(), y=[wb]*len(fw), name="Budget", mode="lines",
                                line=dict(color=C_GOLD, dash="dot", width=1))
                _chart_theme(fig, "Food vs Weekly Budget"); st.plotly_chart(fig, use_container_width=True)
    with ch4:
        if not holdings_df.empty:
            enriched = build_enriched_holdings(holdings_df, price_cache_df)
            ba = enriched.groupby("asset_type", as_index=False)["current_value"].sum()
            fig = _bar_chart(ba["asset_type"].tolist(), ba["current_value"].tolist(), color=C_BLUE, title="Portfolio by Asset Type")
            st.plotly_chart(fig, use_container_width=True)
    csv = filtered.copy(); csv["date"] = csv["date"].dt.strftime("%Y-%m-%d")
    st.download_button("Export CSV", data=csv.to_csv(index=False).encode("utf-8"), file_name="transactions.csv", mime="text/csv")


def render_settings(settings: dict[str, str], accounts_df: pd.DataFrame) -> None:
    st.markdown('<div class="bb-title">Settings</div>', unsafe_allow_html=True)
    st.markdown('<div class="bb-subtitle">Configure your book</div>', unsafe_allow_html=True)

    with st.form("settings_form"):
        c1, c2, c3 = st.columns(3)
        daily_food_budget = c1.number_input("Daily Food Budget", min_value=0.0, value=get_setting_float(settings, "daily_food_budget"), step=1.0)
        pay_period_days = c2.number_input("Pay Period Days", min_value=1, value=int(get_setting_float(settings, "pay_period_days") or 14), step=1)
        statement_day = c3.number_input("Statement Day", min_value=1, max_value=31, value=int(get_setting_float(settings, "statement_day") or 2), step=1)
        c4, c5, c6, c7, c8 = st.columns(5)
        due_day = c4.number_input("Due Day", min_value=1, max_value=31, value=int(get_setting_float(settings, "due_day") or 27), step=1)
        savings_pct = c5.number_input("Savings %", min_value=0.0, max_value=1.0, value=get_setting_float(settings, "savings_pct"), step=0.01, format="%.2f")
        spending_pct = c6.number_input("Spending %", min_value=0.0, max_value=1.0, value=get_setting_float(settings, "spending_pct"), step=0.01, format="%.2f")
        crypto_pct = c7.number_input("Crypto %", min_value=0.0, max_value=1.0, value=get_setting_float(settings, "crypto_pct"), step=0.01, format="%.2f")
        taxable_pct = c8.number_input("Taxable %", min_value=0.0, max_value=1.0, value=get_setting_float(settings, "taxable_investing_pct"), step=0.01, format="%.2f")
        roth_pct = st.number_input("Roth IRA %", min_value=0.0, max_value=1.0, value=get_setting_float(settings, "roth_ira_pct"), step=0.01, format="%.2f")
        if not math.isclose(savings_pct + spending_pct + crypto_pct + taxable_pct + roth_pct, 1.0, abs_tol=0.0001):
            st.warning("Post-debt percentages should add to 1.00.")

        st.subheader("Account Starting Balances")
        updated_balances: dict[int, float] = {}
        cols = st.columns(2)
        # Extract as plain Python primitives — no Arrow types whatsoever
        acct_ids   = [int(x)   for x in accounts_df["id"].tolist()]
        acct_names = [str(x)   for x in accounts_df["name"].tolist()]
        acct_bals  = [float(x) for x in accounts_df["starting_balance"].tolist()]
        acct_sorts = [int(x)   for x in accounts_df["sort_order"].tolist()]
        combined = sorted(zip(acct_sorts, acct_ids, acct_names, acct_bals), key=lambda x: x[0])
        for idx, (_, acct_id, acct_name, acct_bal) in enumerate(combined):
            updated_balances[acct_id] = cols[idx % 2].number_input(acct_name, value=acct_bal, step=10.0, format="%.2f", key=f"bal_{idx}")

        submitted = st.form_submit_button("Save Settings", type="primary")
        if submitted:
            set_settings({
                "daily_food_budget": daily_food_budget, "pay_period_days": pay_period_days,
                "statement_day": statement_day, "due_day": due_day,
                "savings_pct": savings_pct, "spending_pct": spending_pct,
                "crypto_pct": crypto_pct, "taxable_investing_pct": taxable_pct, "roth_ira_pct": roth_pct,
            })
            conn = get_connection()
            try:
                for acct_id, balance in updated_balances.items():
                    db_execute(conn, "UPDATE accounts SET starting_balance = %s WHERE id = %s", (float(balance), int(acct_id)))
                conn.commit()
            finally:
                conn.close()
            st.success("Settings saved."); st.rerun()

    st.subheader("Add New Account")
    with st.form("add_account_form"):
        a1, a2 = st.columns(2)
        new_name = a1.text_input("Account Name", placeholder="e.g. Chase Checking")
        new_type = a2.selectbox("Account Type", ["cash", "savings", "credit", "investment"])
        a3, a4 = st.columns(2)
        new_is_debt = 1 if a3.selectbox("Is this a debt?", ["No", "Yes"]) == "Yes" else 0
        new_runway = 1 if a4.selectbox("Include in runway?", ["Yes", "No"]) == "Yes" else 0
        if st.form_submit_button("Add Account", type="primary"):
            if not new_name.strip(): st.error("Account name required.")
            else:
                add_account(new_name.strip(), new_type, new_is_debt, new_runway)
                st.success(f"'{new_name}' added."); st.rerun()

    st.subheader("Export Data")
    tx_df = load_transactions(); hld_df = load_holdings()
    c1, c2 = st.columns(2)
    c1.download_button("Transactions CSV", data=tx_df.to_csv(index=False).encode("utf-8"), file_name="transactions.csv", mime="text/csv")
    c2.download_button("Holdings CSV", data=hld_df.to_csv(index=False).encode("utf-8"), file_name="holdings.csv", mime="text/csv")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    inject_css()
    init_db()
    migration_message = migrate_from_excel_if_needed()
    if migration_message:
        st.toast(migration_message, icon="📥")

    settings = get_settings()
    accounts_df = load_accounts()
    transactions_df = load_transactions()
    holdings_df = load_holdings()
    price_cache_df = load_price_cache()
    balances_df = build_account_balances(accounts_df, transactions_df, holdings_df, price_cache_df)

    with st.sidebar:
        st.markdown('<div class="bb-sidebar-brand">Black Book</div>', unsafe_allow_html=True)
        st.markdown("---")
        page = st.radio("", ["Dashboard", "Log Transaction", "Paycheck Allocation", "Investments", "Reports", "Settings"], label_visibility="collapsed")
        st.markdown("---")
        st.caption("PostgreSQL · Cloud" if IS_POSTGRES else f"SQLite · {DB_PATH}")

    if page == "Dashboard":
        render_dashboard(settings, transactions_df, holdings_df, balances_df, price_cache_df)
    elif page == "Log Transaction":
        render_log_transaction(accounts_df)
    elif page == "Paycheck Allocation":
        render_paycheck_allocation(settings, balances_df)
    elif page == "Investments":
        render_investments(holdings_df, price_cache_df)
    elif page == "Reports":
        render_reports(settings, transactions_df, holdings_df, price_cache_df)
    elif page == "Settings":
        render_settings(settings, accounts_df)


if __name__ == "__main__":
    main()
