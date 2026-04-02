"""
Budget Black Book
Run locally:
    1. pip install -r requirements.txt
    2. streamlit run app.py

Migration:
    - On first launch, the app looks for `Budget Black Book copy.xlsx` or
      `Budget Black Book.xlsx` in the app folder and your Downloads folder.
    - If a workbook is found, the app imports:
        * Spending Log -> transactions
        * Investments -> holdings
        * Home settings/balances -> accounts + settings
    - Migration runs once and sets a completion flag in the database.

Storage:
    - By default the app uses a local SQLite database named
      `budget_black_book.db`.
    - Set `BUDGET_BLACK_BOOK_DB_PATH` to change the file location.
    - For hosted deployment, point that env var to persistent storage.
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

# Load DATABASE_URL from Streamlit secrets if not already in environment.
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
    "daily_food_budget": "30",
    "pay_period_days": "14",
    "statement_day": "2",
    "due_day": "27",
    "savings_pct": "0.30",
    "spending_pct": "0.40",
    "crypto_pct": "0.10",
    "taxable_investing_pct": "0.10",
    "roth_ira_pct": "0.10",
    "debt_allocation_mode": "proportional",
    "migration_completed": "0",
    "last_price_refresh_at": "",
}
DEFAULT_ACCOUNTS = [
    {"name": "Checking", "account_type": "cash", "is_debt": 0, "include_in_runway": 1, "sort_order": 1},
    {"name": "Savings", "account_type": "savings", "is_debt": 0, "include_in_runway": 1, "sort_order": 2},
    {"name": "Savor", "account_type": "credit", "is_debt": 1, "include_in_runway": 0, "sort_order": 3},
    {"name": "Venture", "account_type": "credit", "is_debt": 1, "include_in_runway": 0, "sort_order": 4},
    {"name": "Coinbase", "account_type": "investment", "is_debt": 0, "include_in_runway": 0, "sort_order": 5},
    {"name": "Roth IRA", "account_type": "investment", "is_debt": 0, "include_in_runway": 0, "sort_order": 6},
    {"name": "Investments", "account_type": "investment", "is_debt": 0, "include_in_runway": 0, "sort_order": 7},
]
COMMON_CATEGORIES = [
    "Food",
    "Bills",
    "Subscriptions",
    "Income",
    "Debt Payment",
    "Gas",
    "Health",
    "Shopping",
    "Entertainment",
    "Savings",
    "Transfer",
    "Investing",
    "Other",
]
CRYPTO_NAME_TO_ID = {
    "XRP": "ripple",
    "Bitcoin (BTC)": "bitcoin",
    "Bittensor (TAO)": "bittensor",
    "Worldcoin (WLD)": "worldcoin-wld",
    "Sui (SUI)": "sui",
    "Solana (SOL)": "solana",
    "Cash (USD)": "",
}
STOCK_NAME_TO_TICKER = {
    "NVIDIA (NVDA)": "NVDA",
    "Palantir (PLTR)": "PLTR",
    "Tesla (TSLA)": "TSLA",
    "Invesco QQQ (QQQ)": "QQQ",
    "SPDR S&P 500 (SPY)": "SPY",
}
ASSET_EMOJI = {
    "cash": "💵",
    "savings": "🏦",
    "credit": "💳",
    "investment": "📈",
}


st.set_page_config(
    page_title=APP_TITLE,
    page_icon="📖",
    layout="wide",
    initial_sidebar_state="expanded",
)


@dataclass
class Signal:
    level: str
    title: str
    body: str


def inject_css() -> None:
    st.markdown(
        """
        <style>
        /* ── Layout ───────────────────────────────── */
        .main > div { padding-top: 1rem; }
        section[data-testid="stSidebar"] {
            background: #080b10;
            border-right: 1px solid rgba(255,255,255,0.05);
        }

        /* ── Typography ───────────────────────────── */
        h1 { font-size: 1.1rem !important; font-weight: 700 !important;
             letter-spacing: 0.12em !important; text-transform: uppercase;
             color: #e2e2e2 !important; margin-bottom: 0.25rem !important; }
        h2, h3 { font-size: 0.75rem !important; font-weight: 600 !important;
                 letter-spacing: 0.1em !important; text-transform: uppercase;
                 color: #6b7280 !important; margin-top: 1.25rem !important; }

        /* ── Metrics ──────────────────────────────── */
        [data-testid="stMetric"] {
            background: rgba(255,255,255,0.02);
            border: 1px solid rgba(255,255,255,0.06);
            border-radius: 3px;
            padding: 0.75rem 1rem 0.55rem 1rem;
        }
        [data-testid="stMetricLabel"] p {
            font-size: 0.65rem !important;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            color: #6b7280 !important;
        }
        [data-testid="stMetricValue"] {
            font-family: 'SF Mono', 'Consolas', 'Courier New', monospace !important;
            font-size: 1.15rem !important;
            color: #f0f0f0 !important;
        }
        [data-testid="stMetricDelta"] {
            font-family: 'SF Mono', 'Consolas', 'Courier New', monospace !important;
            font-size: 0.75rem !important;
        }

        /* ── Signal banner ────────────────────────── */
        .term-signal {
            display: flex;
            align-items: baseline;
            gap: 0.8rem;
            padding: 0.6rem 1rem;
            border-left: 3px solid;
            background: rgba(255,255,255,0.02);
            margin-bottom: 1rem;
        }
        .term-signal-danger  { border-color: #ff4d4d; }
        .term-signal-warning { border-color: #f0a500; }
        .term-signal-success { border-color: #00c896; }
        .term-signal-title {
            font-size: 0.72rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.09em;
            white-space: nowrap;
        }
        .term-signal-danger  .term-signal-title { color: #ff4d4d; }
        .term-signal-warning .term-signal-title { color: #f0a500; }
        .term-signal-success .term-signal-title { color: #00c896; }
        .term-signal-body {
            font-size: 0.78rem;
            color: #9ca3af;
            line-height: 1.4;
        }

        /* ── Account rows ─────────────────────────── */
        .term-accounts { width: 100%; }
        .term-acct-header {
            display: grid;
            grid-template-columns: 1fr 120px 70px;
            padding: 0 0 0.35rem 0;
            border-bottom: 1px solid rgba(255,255,255,0.08);
            margin-bottom: 0.1rem;
        }
        .term-acct-header span {
            font-size: 0.6rem;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            color: #4b5563;
        }
        .term-acct-header span:not(:first-child) { text-align: right; }
        .term-acct-row {
            display: grid;
            grid-template-columns: 1fr 120px 70px;
            padding: 0.45rem 0;
            border-bottom: 1px solid rgba(255,255,255,0.03);
        }
        .term-acct-row:hover { background: rgba(255,255,255,0.02); }
        .term-acct-name {
            font-size: 0.78rem;
            letter-spacing: 0.04em;
            color: #c9d1d9;
        }
        .term-acct-bal {
            font-family: 'SF Mono', 'Consolas', 'Courier New', monospace;
            font-size: 0.82rem;
            font-weight: 600;
            text-align: right;
            color: #f0f0f0;
        }
        .term-acct-bal.neg { color: #ff4d4d; }
        .term-acct-type {
            font-size: 0.62rem;
            text-transform: uppercase;
            letter-spacing: 0.07em;
            color: #4b5563;
            text-align: right;
        }
        .term-acct-debt .term-acct-bal { color: #ff4d4d; }
        .term-acct-debt .term-acct-name { color: #9ca3af; }

        /* ── Dataframes ───────────────────────────── */
        [data-testid="stDataFrame"] {
            border: 1px solid rgba(255,255,255,0.06) !important;
            border-radius: 3px !important;
        }

        /* ── Buttons ──────────────────────────────── */
        [data-testid="baseButton-primary"] {
            border-radius: 3px !important;
            letter-spacing: 0.06em;
            font-size: 0.78rem !important;
        }

        /* ── Sidebar nav ──────────────────────────── */
        [data-testid="stRadio"] label {
            font-size: 0.78rem !important;
            letter-spacing: 0.06em;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def get_connection():
    url = st.secrets.get("DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        st.error("DATABASE_URL is not configured.")
        st.stop()
    # Embed sslmode in the URL to avoid kwarg conflict with psycopg2
    if "sslmode" not in url:
        url += "?sslmode=require"
    conn = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
    return conn


def db_execute(conn, sql: str, params: tuple = ()) -> Any:
    """Execute a single parameterized statement on either SQLite or PostgreSQL.

    Uses %s placeholders in SQL (PostgreSQL style). Converts to ? for SQLite.
    Returns the cursor so callers can use .fetchone(), .fetchall(), .lastrowid, etc.
    """
    if IS_POSTGRES:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur
    # SQLite expects ? placeholders.
    sqlite_sql = re.sub(r"%s", "?", sql)
    return conn.execute(sqlite_sql, params)


def _to_float_series(s: pd.Series) -> pd.Series:
    """Safely coerce any pandas Series to float, handling Arrow-backed types from Postgres."""
    return pd.to_numeric(s, errors="coerce").fillna(0.0).astype(float)


def init_db() -> None:
    serial = "SERIAL" if IS_POSTGRES else "INTEGER"
    autoincrement = "" if IS_POSTGRES else "AUTOINCREMENT"
    ddl_statements = [
        f"""
        CREATE TABLE IF NOT EXISTS accounts (
            id {serial} PRIMARY KEY {autoincrement},
            name TEXT NOT NULL UNIQUE,
            account_type TEXT NOT NULL,
            is_debt INTEGER NOT NULL DEFAULT 0,
            include_in_runway INTEGER NOT NULL DEFAULT 1,
            starting_balance REAL NOT NULL DEFAULT 0,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS transactions (
            id {serial} PRIMARY KEY {autoincrement},
            date TEXT NOT NULL,
            description TEXT NOT NULL,
            category TEXT NOT NULL,
            amount REAL NOT NULL,
            account_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            to_account_id INTEGER,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(account_id) REFERENCES accounts(id),
            FOREIGN KEY(to_account_id) REFERENCES accounts(id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS holdings (
            id {serial} PRIMARY KEY {autoincrement},
            symbol TEXT NOT NULL,
            display_name TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            account_id INTEGER NOT NULL,
            amount_invested REAL NOT NULL DEFAULT 0,
            quantity REAL NOT NULL DEFAULT 0,
            avg_price REAL NOT NULL DEFAULT 0,
            coingecko_id TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(account_id) REFERENCES accounts(id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS allocation_snapshots (
            id {serial} PRIMARY KEY {autoincrement},
            paycheck_amount REAL NOT NULL,
            run_date TEXT NOT NULL,
            debt_total REAL NOT NULL,
            food_reserved REAL NOT NULL,
            debt_reserved REAL NOT NULL,
            savings_reserved REAL NOT NULL,
            spending_reserved REAL NOT NULL,
            crypto_reserved REAL NOT NULL,
            taxable_reserved REAL NOT NULL,
            roth_reserved REAL NOT NULL,
            debt_breakdown_json TEXT NOT NULL,
            meta_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS price_cache (
            symbol TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            price REAL NOT NULL,
            previous_close REAL,
            currency TEXT NOT NULL DEFAULT 'USD',
            source TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY(symbol, asset_type)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS price_history (
            id {serial} PRIMARY KEY {autoincrement},
            symbol TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            price REAL NOT NULL,
            previous_close REAL,
            as_of_date TEXT NOT NULL,
            source TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
    ]
    conn = get_connection()
    try:
        for stmt in ddl_statements:
            db_execute(conn, stmt)
        for key, value in DEFAULT_SETTINGS.items():
            db_execute(
                conn,
                "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT(key) DO NOTHING",
                (key, value),
            )
        for account in DEFAULT_ACCOUNTS:
            db_execute(
                conn,
                """
                INSERT INTO accounts
                (name, account_type, is_debt, include_in_runway, starting_balance, sort_order)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT(name) DO NOTHING
                """,
                (
                    account["name"],
                    account["account_type"],
                    account["is_debt"],
                    account["include_in_runway"],
                    0.0,
                    account["sort_order"],
                ),
            )
        conn.commit()
    finally:
        conn.close()


def table_exists_with_rows(table_name: str) -> bool:
    conn = get_connection()
    try:
        cur = db_execute(conn, f"SELECT COUNT(*) AS count FROM {table_name}")
        row = cur.fetchone()
    finally:
        conn.close()
    return bool(row["count"])


def get_settings() -> dict[str, str]:
    conn = get_connection()
    try:
        cur = db_execute(conn, "SELECT key, value FROM settings")
        rows = cur.fetchall()
    finally:
        conn.close()
    return {row["key"]: row["value"] for row in rows}


def set_settings(settings: dict[str, Any]) -> None:
    conn = get_connection()
    try:
        for key, value in settings.items():
            db_execute(
                conn,
                """
                INSERT INTO settings (key, value) VALUES (%s, %s)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, str(value)),
            )
        conn.commit()
    finally:
        conn.close()


def load_accounts() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT id, name, account_type, is_debt, include_in_runway, starting_balance, sort_order
            FROM accounts
            ORDER BY sort_order, name
            """,
            conn,
        )
    finally:
        conn.close()
    for col in ("id", "is_debt", "include_in_runway", "sort_order"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    df["starting_balance"] = _to_float_series(df["starting_balance"])
    df = pd.DataFrame(df.to_dict("records"))  # ← only new line
    return df


def add_transaction(
    tx_date: date,
    description: str,
    category: str,
    amount: float,
    account_id: int,
    tx_type: str,
    to_account_id: int | None,
    notes: str,
) -> None:
    conn = get_connection()
    try:
        db_execute(
            conn,
            """
            INSERT INTO transactions
            (date, description, category, amount, account_id, type, to_account_id, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                tx_date.strftime(DATE_FMT),
                description.strip(),
                category,
                float(amount),
                int(account_id),
                tx_type,
                int(to_account_id) if to_account_id else None,
                notes.strip() or None,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def load_transactions() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT
                t.id,
                t.date,
                t.description,
                t.category,
                t.amount,
                t.type,
                t.notes,
                a.name AS account,
                a.id AS account_id,
                ta.name AS to_account,
                ta.id AS to_account_id
            FROM transactions t
            JOIN accounts a ON a.id = t.account_id
            LEFT JOIN accounts ta ON ta.id = t.to_account_id
            ORDER BY t.date DESC, t.id DESC
            """,
            conn,
        )
    finally:
        conn.close()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["amount"] = _to_float_series(df["amount"])
    df["account_id"] = pd.to_numeric(df["account_id"], errors="coerce").fillna(0).astype(int)
    df["to_account_id"] = pd.to_numeric(df["to_account_id"], errors="coerce")  # keep NaN for nulls
    return df


def load_holdings() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT
                h.id,
                h.symbol,
                h.display_name,
                h.asset_type,
                h.amount_invested,
                h.quantity,
                h.avg_price,
                h.coingecko_id,
                a.name AS account,
                a.id AS account_id
            FROM holdings h
            JOIN accounts a ON a.id = h.account_id
            ORDER BY a.sort_order, h.display_name
            """,
            conn,
        )
    finally:
        conn.close()
    if df.empty:
        return df
    for col in ("amount_invested", "quantity", "avg_price"):
        df[col] = _to_float_series(df[col])
    df["account_id"] = pd.to_numeric(df["account_id"], errors="coerce").fillna(0).astype(int)
    return df


def save_allocation_snapshot(payload: dict[str, Any]) -> None:
    conn = get_connection()
    try:
        db_execute(
            conn,
            """
            INSERT INTO allocation_snapshots (
                paycheck_amount, run_date, debt_total, food_reserved, debt_reserved,
                savings_reserved, spending_reserved, crypto_reserved, taxable_reserved,
                roth_reserved, debt_breakdown_json, meta_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                payload["paycheck_amount"],
                payload["run_date"],
                payload["debt_total"],
                payload["food_reserved"],
                payload["debt_reserved"],
                payload["savings_reserved"],
                payload["spending_reserved"],
                payload["crypto_reserved"],
                payload["taxable_reserved"],
                payload["roth_reserved"],
                json.dumps(payload["debt_breakdown"]),
                json.dumps(payload["meta"]),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def load_allocation_snapshots(limit: int = 10) -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT *
            FROM allocation_snapshots
            ORDER BY run_date DESC, id DESC
            LIMIT %s
            """ if IS_POSTGRES else """
            SELECT *
            FROM allocation_snapshots
            ORDER BY date(run_date) DESC, id DESC
            LIMIT ?
            """,
            conn,
            params=(limit,),
        )
    finally:
        conn.close()
    if df.empty:
        return df
    for col in ("paycheck_amount", "debt_total", "food_reserved", "debt_reserved",
                "savings_reserved", "spending_reserved", "crypto_reserved",
                "taxable_reserved", "roth_reserved"):
        if col in df.columns:
            df[col] = _to_float_series(df[col])
    return df


def upsert_price(symbol: str, asset_type: str, price: float, previous_close: float | None, source: str, as_of_date: str) -> None:
    fetched_at = datetime.now().isoformat(timespec="seconds")
    conn = get_connection()
    try:
        db_execute(
            conn,
            """
            INSERT INTO price_cache (symbol, asset_type, price, previous_close, currency, source, as_of_date, fetched_at)
            VALUES (%s, %s, %s, %s, 'USD', %s, %s, %s)
            ON CONFLICT(symbol, asset_type) DO UPDATE SET
                price = excluded.price,
                previous_close = excluded.previous_close,
                source = excluded.source,
                as_of_date = excluded.as_of_date,
                fetched_at = excluded.fetched_at
            """,
            (symbol, asset_type, price, previous_close, source, as_of_date, fetched_at),
        )
        db_execute(
            conn,
            """
            INSERT INTO price_history (symbol, asset_type, price, previous_close, as_of_date, source)
            SELECT %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM price_history
                WHERE symbol = %s AND asset_type = %s AND as_of_date = %s
            )
            """,
            (symbol, asset_type, price, previous_close, as_of_date, source, symbol, asset_type, as_of_date),
        )
        conn.commit()
    finally:
        conn.close()


def load_price_cache() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql_query("SELECT * FROM price_cache", conn)
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
        df = pd.read_sql_query(
            "SELECT symbol, asset_type, price, previous_close, as_of_date FROM price_history ORDER BY as_of_date",
            conn,
        )
    finally:
        conn.close()
    if df.empty:
        return df
    df["price"] = _to_float_series(df["price"])
    df["previous_close"] = _to_float_series(df["previous_close"])
    return df


def excel_serial_to_date(value: Any) -> date | None:
    if pd.isna(value) or value in ("", None):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        origin = datetime(1899, 12, 30)
        return (origin + timedelta(days=float(value))).date()
    try:
        return pd.to_datetime(value).date()
    except Exception:
        return None


def detect_workbook() -> Path | None:
    candidates = [
        Path.cwd() / "Budget Black Book copy.xlsx",
        Path.cwd() / "Budget Black Book.xlsx",
        Path.home() / "Downloads" / "Budget Black Book copy.xlsx",
        Path.home() / "Downloads" / "Budget Black Book.xlsx",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def normalize_account_name(name: Any) -> str:
    value = str(name or "").strip()
    mapping = {
        "Savor (CC)": "Savor",
        "Venture (CC)": "Venture",
        "Roth IRA (Fidelity)": "Roth IRA",
        "Investments (Fidelity)": "Investments",
    }
    return mapping.get(value, value)


def parse_home_settings(home_df: pd.DataFrame) -> tuple[dict[str, float], dict[str, str]]:
    settings_updates: dict[str, str] = {}
    account_balances: dict[str, float] = {}
    lookup_rows = {
        "Daily Budget": ("daily_food_budget", "numeric"),
        "Checking — Starting Balance": ("Checking", "account"),
        "Savings — Starting Balance": ("Savings", "account"),
        "Savor (CC) — Starting Balance": ("Savor", "account"),
        "Venture (CC) — Starting Balance": ("Venture", "account"),
        "Coinbase — Starting Balance": ("Coinbase", "account"),
        "Roth IRA (Fidelity) — Starting Balance": ("Roth IRA", "account"),
        "Investments (Fidelity) — Starting Balance": ("Investments", "account"),
    }

    for _, row in home_df.iterrows():
        label = str(row.iloc[2]).strip() if len(row) > 2 and pd.notna(row.iloc[2]) else ""
        value = row.iloc[4] if len(row) > 4 else None
        if label in lookup_rows and pd.notna(value):
            target, kind = lookup_rows[label]
            if kind == "numeric":
                settings_updates[target] = str(float(value))
            else:
                account_balances[target] = float(value)

    return account_balances, settings_updates


def ensure_account(conn, name: str) -> int:
    cur = db_execute(conn, "SELECT id FROM accounts WHERE name = %s", (name,))
    existing = cur.fetchone()
    if existing:
        return int(existing["id"])
    fallback = next((a for a in DEFAULT_ACCOUNTS if a["name"] == name), None)
    account_type = fallback["account_type"] if fallback else "cash"
    is_debt = fallback["is_debt"] if fallback else 0
    include_in_runway = fallback["include_in_runway"] if fallback else 1
    sort_order = fallback["sort_order"] if fallback else 99
    if IS_POSTGRES:
        cur = db_execute(
            conn,
            """
            INSERT INTO accounts (name, account_type, is_debt, include_in_runway, starting_balance, sort_order)
            VALUES (%s, %s, %s, %s, 0, %s)
            RETURNING id
            """,
            (name, account_type, is_debt, include_in_runway, sort_order),
        )
        return int(cur.fetchone()["id"])
    cur = db_execute(
        conn,
        """
        INSERT INTO accounts (name, account_type, is_debt, include_in_runway, starting_balance, sort_order)
        VALUES (%s, %s, %s, %s, 0, %s)
        """,
        (name, account_type, is_debt, include_in_runway, sort_order),
    )
    return int(cur.lastrowid)


def migrate_from_excel_if_needed() -> str | None:
    settings = get_settings()
    if settings.get("migration_completed") == "1":
        return None

    workbook_path = detect_workbook()
    if not workbook_path:
        return None

    try:
        home_df = pd.read_excel(workbook_path, sheet_name="Home", header=None, engine="openpyxl")
        spending_df = pd.read_excel(workbook_path, sheet_name="Spending Log", header=4, engine="openpyxl")
        investments_df = pd.read_excel(workbook_path, sheet_name="Investments", header=12, engine="openpyxl")
    except Exception as exc:
        return f"Workbook found at `{workbook_path}`, but import failed: {exc}"

    conn = get_connection()
    try:
        account_balances, settings_updates = parse_home_settings(home_df)
        for name, balance in account_balances.items():
            account_id = ensure_account(conn, name)
            db_execute(conn, "UPDATE accounts SET starting_balance = %s WHERE id = %s", (float(balance), account_id))
        for key, value in settings_updates.items():
            db_execute(
                conn,
                """
                INSERT INTO settings (key, value) VALUES (%s, %s)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

        if not table_exists_with_rows("transactions"):
            spending_df.columns = [str(col).strip() for col in spending_df.columns]
            spending_df = spending_df.dropna(how="all")
            for _, row in spending_df.iterrows():
                tx_date = excel_serial_to_date(row.get("Date"))
                description = str(row.get("Description") or "").strip()
                category = str(row.get("Category") or "Other").strip()
                amount = row.get("Amount")
                account = normalize_account_name(row.get("Account"))
                tx_type = str(row.get("Type") or "Expense").strip()
                to_account = normalize_account_name(row.get("To Account"))
                if not tx_date or not description or pd.isna(amount) or not account:
                    continue
                account_id = ensure_account(conn, account)
                to_account_id = ensure_account(conn, to_account) if to_account else None
                db_execute(
                    conn,
                    """
                    INSERT INTO transactions
                    (date, description, category, amount, account_id, type, to_account_id, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NULL)
                    """,
                    (tx_date.strftime(DATE_FMT), description, category, float(amount), account_id, tx_type, to_account_id),
                )

        if not table_exists_with_rows("holdings"):
            investments_df.columns = [str(col).strip() for col in investments_df.columns]
            investments_df = investments_df.dropna(how="all")
            rename_map = {
                "TICKER / NAME": "Ticker / Name",
                "ACCOUNT": "Account",
                "AMOUNT ($)": "Amount ($)",
                "HOLDING AMT": "Holding Amt",
                "AVG PRICE": "Avg Price",
            }
            investments_df = investments_df.rename(columns=rename_map)
            required_cols = {"Ticker / Name", "Account", "Amount ($)", "Holding Amt", "Avg Price"}
            if required_cols.issubset(set(investments_df.columns)):
                for _, row in investments_df.iterrows():
                    name = str(row.get("Ticker / Name") or "").strip()
                    account_name = normalize_account_name(row.get("Account"))
                    if not name or not account_name:
                        continue
                    amount_invested = coerce_float(row.get("Amount ($)"), 0.0)
                    quantity = coerce_float(row.get("Holding Amt"), 0.0)
                    avg_price = coerce_float(row.get("Avg Price"), 0.0)
                    account_id = ensure_account(conn, account_name)

                    if account_name == "Coinbase":
                        asset_type = "crypto" if "Cash" not in name else "cash"
                        symbol = name
                        coingecko_id = CRYPTO_NAME_TO_ID.get(name, "")
                    else:
                        asset_type = "etf" if any(tag in name for tag in ("QQQ", "SPY")) else "stock"
                        symbol = STOCK_NAME_TO_TICKER.get(name, name)
                        coingecko_id = None

                    db_execute(
                        conn,
                        """
                        INSERT INTO holdings
                        (symbol, display_name, asset_type, account_id, amount_invested, quantity, avg_price, coingecko_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (symbol, name, asset_type, account_id, amount_invested, quantity, avg_price, coingecko_id),
                    )

        db_execute(
            conn,
            """
            INSERT INTO settings (key, value) VALUES ('migration_completed', '1')
            ON CONFLICT(key) DO UPDATE SET value = '1'
            """,
        )
        conn.commit()
    finally:
        conn.close()

    return f"Imported workbook data from `{workbook_path}`."


def coerce_float(value: Any, fallback: float = 0.0) -> float:
    try:
        result = float(value)
        return fallback if math.isnan(result) else result
    except Exception:
        return fallback


def format_currency(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}${abs(value):,.2f}"


def format_percent(value: float) -> str:
    return f"{value * 100:.1f}%"


def safe_div(num: float, denom: float) -> float:
    return num / denom if denom else 0.0


def _chart_theme(fig) -> object:
    """Apply consistent terminal-style theme to a plotly figure."""
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#6b7280", size=10, family="SF Mono, Consolas, Courier New, monospace"),
        title_font=dict(color="#9ca3af", size=11),
        legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="rgba(255,255,255,0.06)", font=dict(size=10)),
        margin=dict(l=0, r=0, t=36, b=0),
    )
    fig.update_xaxes(gridcolor="rgba(255,255,255,0.04)", zerolinecolor="rgba(255,255,255,0.06)", tickfont=dict(size=9))
    fig.update_yaxes(gridcolor="rgba(255,255,255,0.04)", zerolinecolor="rgba(255,255,255,0.06)", tickfont=dict(size=9))
    return fig


def get_setting_float(settings: dict[str, str], key: str) -> float:
    return coerce_float(settings.get(key), 0.0)


def build_enriched_holdings(holdings_df: pd.DataFrame, price_cache_df: pd.DataFrame) -> pd.DataFrame:
    if holdings_df.empty:
        return holdings_df

    enriched = holdings_df.copy()
    # Ensure numeric types are clean before any arithmetic
    for col in ("amount_invested", "quantity", "avg_price"):
        enriched[col] = _to_float_series(enriched[col])

    price_map = {}
    if not price_cache_df.empty:
        price_map = {
            (row["symbol"], row["asset_type"]): row for _, row in price_cache_df.iterrows()
        }

    latest_price = []
    previous_close = []
    price_source = []
    fetched_at = []
    for _, row in enriched.iterrows():
        cache_row = price_map.get((row["symbol"], row["asset_type"]))
        if cache_row is not None:
            latest_price.append(float(cache_row["price"]))
            previous_close.append(coerce_float(cache_row["previous_close"], float(cache_row["price"])))
            price_source.append(cache_row["source"])
            fetched_at.append(cache_row["fetched_at"])
        else:
            fallback = 1.0 if row["asset_type"] == "cash" and row["display_name"] == "Cash (USD)" else float(row["avg_price"] or 0)
            latest_price.append(fallback)
            previous_close.append(fallback)
            price_source.append("fallback")
            fetched_at.append("")

    enriched["latest_price"] = latest_price
    enriched["previous_close"] = previous_close
    enriched["price_source"] = price_source
    enriched["fetched_at"] = fetched_at
    enriched["current_value"] = enriched["quantity"] * enriched["latest_price"]
    enriched["current_value"] = enriched["current_value"].where(enriched["quantity"] > 0, enriched["amount_invested"])
    enriched["total_pnl"] = enriched["current_value"] - enriched["amount_invested"]
    enriched["total_pnl_pct"] = enriched.apply(
        lambda row: safe_div(row["total_pnl"], row["amount_invested"]) if row["amount_invested"] else 0.0,
        axis=1,
    )
    enriched["tdy_pnl"] = (enriched["latest_price"] - enriched["previous_close"]) * enriched["quantity"]
    return enriched


def build_account_balances(accounts_df: pd.DataFrame, transactions_df: pd.DataFrame, holdings_df: pd.DataFrame, price_cache_df: pd.DataFrame) -> pd.DataFrame:
    balances = accounts_df.copy()
    # Use _to_float_series so Arrow-backed Postgres types don't crash
    balances["current_balance"] = _to_float_series(balances["starting_balance"])

    if transactions_df.empty:
        tx_df = pd.DataFrame(columns=["account_id", "to_account_id", "type", "amount", "date", "id"])
    else:
        tx_df = transactions_df.copy()
        tx_df["amount"] = _to_float_series(tx_df["amount"])

    debt_ids = set(balances.loc[balances["is_debt"] == 1, "id"].astype(int))
    current_balances = {int(row["id"]): coerce_float(row["starting_balance"]) for _, row in balances.iterrows()}

    for _, tx in tx_df.sort_values(by=["date", "id"]).iterrows():
        account_id = int(tx["account_id"])
        to_account_id = int(tx["to_account_id"]) if pd.notna(tx["to_account_id"]) else None
        amount = float(tx["amount"])
        tx_type = str(tx["type"])

        if account_id in debt_ids:
            if tx_type == "Expense":
                current_balances[account_id] += amount
            elif tx_type == "Income":
                current_balances[account_id] -= amount
            elif tx_type == "Transfer":
                current_balances[account_id] += amount
        else:
            if tx_type == "Expense":
                current_balances[account_id] -= amount
            elif tx_type == "Income":
                current_balances[account_id] += amount
            elif tx_type == "Transfer":
                current_balances[account_id] -= amount

        if to_account_id:
            if to_account_id in debt_ids:
                current_balances[to_account_id] -= amount
            else:
                current_balances[to_account_id] += amount

    balances["current_balance"] = balances["id"].map(current_balances).astype(float)

    holdings_value_by_account = {}
    if not holdings_df.empty:
        enriched = build_enriched_holdings(holdings_df, price_cache_df)
        holdings_value_by_account = (
            enriched.groupby("account_id", dropna=False)["current_value"].sum().to_dict()
        )

    for idx, row in balances.iterrows():
        if row["account_type"] == "investment" and row["id"] in holdings_value_by_account:
            balances.at[idx, "display_balance"] = holdings_value_by_account[row["id"]]
        else:
            balances.at[idx, "display_balance"] = row["current_balance"]

    return balances


def build_food_metrics(transactions_df: pd.DataFrame, settings: dict[str, str]) -> dict[str, Any]:
    daily_budget = get_setting_float(settings, "daily_food_budget")
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    food_df = transactions_df.loc[transactions_df["category"].eq("Food")].copy() if not transactions_df.empty else pd.DataFrame()

    today_spent = 0.0
    week_spent = 0.0
    total_food_spent = 0.0
    current_carry = 0.0
    lifetime_surplus = 0.0
    active_days = 0
    avg_daily_food = 0.0

    if not food_df.empty:
        food_df["date"] = pd.to_datetime(food_df["date"]).dt.date
        today_spent = float(food_df.loc[food_df["date"] == today, "amount"].sum())
        week_spent = float(food_df.loc[food_df["date"] >= week_start, "amount"].sum())
        total_food_spent = float(food_df["amount"].sum())

        start_day = min(food_df["date"].min(), today)
        all_days = pd.date_range(start=start_day, end=today, freq="D")
        daily_spend = food_df.groupby("date")["amount"].sum().reindex(all_days.date, fill_value=0.0)
        carry = 0.0
        lifetime = 0.0
        for day_spend in daily_spend:
            carry += daily_budget
            carry -= float(day_spend)
            lifetime += max(daily_budget - float(day_spend), 0.0)
        current_carry = carry
        lifetime_surplus = lifetime
        active_days = len(all_days)
        avg_daily_food = safe_div(total_food_spent, active_days)

    return {
        "daily_budget": daily_budget,
        "weekly_budget": daily_budget * 7,
        "food_spent_today": today_spent,
        "food_spent_week": week_spent,
        "remaining_today": daily_budget - today_spent,
        "remaining_week": (daily_budget * 7) - week_spent,
        "current_carry_surplus": current_carry,
        "lifetime_surplus": lifetime_surplus,
        "avg_daily_food_spend": avg_daily_food,
        "food_days_tracked": active_days,
        "transactions_today": int(
            0 if transactions_df.empty else (pd.to_datetime(transactions_df["date"]).dt.date == today).sum()
        ),
    }


def build_runway(transactions_df: pd.DataFrame, balances_df: pd.DataFrame, food_metrics: dict[str, Any]) -> dict[str, float]:
    liquid_cash = float(
        balances_df.loc[balances_df["include_in_runway"] == 1, "display_balance"].sum()
    )
    if transactions_df.empty:
        avg_daily_spending = food_metrics["avg_daily_food_spend"]
    else:
        spend_df = transactions_df.loc[transactions_df["type"].eq("Expense")].copy()
        spend_df["date"] = pd.to_datetime(spend_df["date"]).dt.date
        trailing_start = date.today() - timedelta(days=29)
        trailing_df = spend_df.loc[spend_df["date"] >= trailing_start]
        if trailing_df.empty:
            avg_daily_spending = food_metrics["avg_daily_food_spend"]
        else:
            total_spend = float(trailing_df["amount"].sum())
            avg_daily_spending = total_spend / 30.0
    runway_days = safe_div(liquid_cash, avg_daily_spending)
    return {
        "liquid_cash": liquid_cash,
        "avg_daily_spending": avg_daily_spending,
        "runway_days": runway_days,
    }


def build_debt_summary(balances_df: pd.DataFrame) -> dict[str, Any]:
    debt_df = balances_df.loc[balances_df["is_debt"] == 1, ["id", "name", "display_balance"]].copy()
    debt_df["display_balance"] = _to_float_series(debt_df["display_balance"])
    debt_df["display_balance"] = debt_df["display_balance"].clip(lower=0)
    return {
        "total_debt": float(debt_df["display_balance"].sum()),
        "by_account": debt_df.sort_values("display_balance", ascending=False),
    }


def build_signals(
    balances_df: pd.DataFrame,
    debt_summary: dict[str, Any],
    food_metrics: dict[str, Any],
    runway: dict[str, Any],
    settings: dict[str, str],
) -> list[Signal]:
    signals: list[Signal] = []
    checking_balance = float(
        balances_df.loc[balances_df["name"].eq("Checking"), "display_balance"].sum()
    )
    due_day = int(get_setting_float(settings, "due_day") or 27)
    statement_day = int(get_setting_float(settings, "statement_day") or 2)
    today = date.today()
    due_date = date(today.year, today.month, min(due_day, 28))
    if today.day > due_day:
        next_month = today.replace(day=28) + timedelta(days=4)
        due_date = date(next_month.year, next_month.month, min(due_day, 28))
    days_to_due = (due_date - today).days

    if food_metrics["remaining_today"] < 0:
        signals.append(Signal("danger", "⚠ Food overspent today", "You are over the daily food cap. Pull back the next meal or use carried surplus carefully."))
    elif food_metrics["remaining_week"] < 0:
        signals.append(Signal("warning", "⚠ Food budget is behind this week", "Weekly food spend is over budget. Use a cheaper stretch until payday."))

    if debt_summary["total_debt"] > runway["liquid_cash"] * 0.75 and debt_summary["total_debt"] > 0:
        signals.append(Signal("danger", "⚠ Debt pressure is high", "Credit card debt is large relative to liquid cash. Keep the next paycheck debt-heavy."))
    elif debt_summary["total_debt"] > 0:
        signals.append(Signal("warning", "⚠ Debt still needs room in the next allocation", "Debt is active. The paycheck engine will reserve a proportional payment first."))

    if days_to_due <= 5 and debt_summary["total_debt"] > 0:
        signals.append(Signal("warning", "📅 Payment due soon", f"Card payments are due in {days_to_due} day(s). Make sure the allocated debt payment gets posted on time."))

    if today.day <= statement_day + 2 and debt_summary["total_debt"] > 0:
        signals.append(Signal("warning", "🧾 Statement window is open", "Fresh statement balances are likely available. Review debt totals before locking in your next paycheck allocation."))

    if checking_balance < 50:
        signals.append(Signal("danger", "💸 Checking is running low", "Checking is below $50. Protect essentials and move cash only if it does not hurt debt timing."))

    if runway["runway_days"] < 14:
        signals.append(Signal("danger", "🛣 Runway is short", "Current liquid cash covers less than two weeks of spending. Keep discretionary spending tight."))
    elif runway["runway_days"] < 30:
        signals.append(Signal("warning", "🛣 Runway needs work", "You have less than a month of runway. Prioritize savings after food and debt."))

    if food_metrics["current_carry_surplus"] > food_metrics["daily_budget"] * 3:
        signals.append(Signal("success", "🍽 Food discipline is paying off", "You have built a healthy carried food surplus. Keep stacking the easy wins."))

    if not signals:
        signals.append(Signal("success", "💰 Budget looks steady", "No immediate pressure signals. Keep logging moves and let the system do the math."))
    severity = {"danger": 0, "warning": 1, "success": 2}
    return sorted(signals, key=lambda s: severity[s.level])


def compute_paycheck_allocation(
    paycheck_amount: float,
    settings: dict[str, str],
    debt_df: pd.DataFrame,
) -> dict[str, Any]:
    pay_period_days = int(get_setting_float(settings, "pay_period_days") or 14)
    food_reserved = max(get_setting_float(settings, "daily_food_budget") * pay_period_days, 0.0)
    remaining_after_food = max(paycheck_amount - food_reserved, 0.0)
    total_debt = float(debt_df["display_balance"].sum()) if not debt_df.empty else 0.0
    debt_reserved = min(total_debt, remaining_after_food)
    remaining_after_debt = max(remaining_after_food - debt_reserved, 0.0)

    savings_reserved = remaining_after_debt * get_setting_float(settings, "savings_pct")
    spending_reserved = remaining_after_debt * get_setting_float(settings, "spending_pct")
    crypto_reserved = remaining_after_debt * get_setting_float(settings, "crypto_pct")
    taxable_reserved = remaining_after_debt * get_setting_float(settings, "taxable_investing_pct")
    roth_reserved = remaining_after_debt * get_setting_float(settings, "roth_ira_pct")

    debt_breakdown: list[dict[str, Any]] = []
    if total_debt > 0 and debt_reserved > 0 and not debt_df.empty:
        debt_df = debt_df.copy()
        debt_df["share"] = debt_df["display_balance"] / total_debt
        for _, row in debt_df.iterrows():
            debt_breakdown.append(
                {
                    "account_id": int(row["id"]),
                    "account": row["name"],
                    "debt_balance": float(row["display_balance"]),
                    "allocation": float(debt_reserved * row["share"]),
                }
            )

    return {
        "paycheck_amount": paycheck_amount,
        "run_date": date.today().strftime(DATE_FMT),
        "debt_total": total_debt,
        "food_reserved": food_reserved,
        "debt_reserved": debt_reserved,
        "remaining_after_food": remaining_after_food,
        "remaining_after_debt": remaining_after_debt,
        "savings_reserved": savings_reserved,
        "spending_reserved": spending_reserved,
        "crypto_reserved": crypto_reserved,
        "taxable_reserved": taxable_reserved,
        "roth_reserved": roth_reserved,
        "debt_breakdown": debt_breakdown,
        "meta": {
            "pay_period_days": pay_period_days,
            "allocation_mode": settings.get("debt_allocation_mode", "proportional"),
        },
    }


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_yfinance_prices(symbols: tuple[str, ...]) -> dict[str, dict[str, float | None]]:
    prices: dict[str, dict[str, float | None]] = {}
    if not symbols:
        return prices
    tickers = yf.Tickers(" ".join(symbols))
    for symbol in symbols:
        info = {"price": None, "previous_close": None}
        try:
            hist = tickers.tickers[symbol].history(period="2d", interval="1d", auto_adjust=False)
            if not hist.empty:
                info["price"] = float(hist["Close"].iloc[-1])
                if len(hist) > 1:
                    info["previous_close"] = float(hist["Close"].iloc[-2])
                else:
                    info["previous_close"] = float(hist["Close"].iloc[-1])
        except Exception:
            pass
        prices[symbol] = info
    return prices


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_coingecko_prices(ids: tuple[str, ...]) -> dict[str, dict[str, float | None]]:
    prices: dict[str, dict[str, float | None]] = {coin_id: {"price": None, "previous_close": None} for coin_id in ids}
    ids = tuple(coin_id for coin_id in ids if coin_id)
    if not ids:
        return prices
    response = requests.get(
        "https://api.coingecko.com/api/v3/simple/price",
        params={
            "ids": ",".join(ids),
            "vs_currencies": "usd",
            "include_24hr_change": "true",
        },
        timeout=15,
    )
    response.raise_for_status()
    payload = response.json()
    for coin_id in ids:
        data = payload.get(coin_id, {})
        price = data.get("usd")
        change = data.get("usd_24h_change")
        previous_close = None
        if price is not None and change is not None:
            try:
                previous_close = float(price) / (1 + (float(change) / 100))
            except ZeroDivisionError:
                previous_close = float(price)
        prices[coin_id] = {
            "price": float(price) if price is not None else None,
            "previous_close": previous_close,
        }
    return prices


def maybe_refresh_prices(holdings_df: pd.DataFrame, force: bool = False) -> tuple[bool, str]:
    if holdings_df.empty:
        return False, "No holdings to refresh yet."

    settings = get_settings()
    last_refresh = settings.get("last_price_refresh_at", "")
    now = datetime.now()
    market_close_passed = now.time() >= time(hour=16, minute=15)
    already_refreshed_today = last_refresh.startswith(date.today().strftime(DATE_FMT))
    should_refresh = force or (market_close_passed and not already_refreshed_today)
    if not should_refresh:
        return False, "Using cached prices."

    stock_symbols = tuple(sorted(set(holdings_df.loc[holdings_df["asset_type"].isin(["stock", "etf"]), "symbol"].astype(str))))
    crypto_rows = holdings_df.loc[holdings_df["asset_type"].eq("crypto") & holdings_df["coingecko_id"].fillna("").ne("")]
    coin_ids = tuple(sorted(set(crypto_rows["coingecko_id"].astype(str))))

    stock_prices = fetch_yfinance_prices(stock_symbols)
    crypto_prices = {}
    if coin_ids:
        try:
            crypto_prices = fetch_coingecko_prices(coin_ids)
        except Exception:
            crypto_prices = {}

    as_of_date = date.today().strftime(DATE_FMT)
    refreshed = 0
    for _, row in holdings_df.iterrows():
        symbol = str(row["symbol"])
        asset_type = str(row["asset_type"])
        if asset_type in {"stock", "etf"}:
            price_info = stock_prices.get(symbol, {})
            price = price_info.get("price")
            previous_close = price_info.get("previous_close")
            source = "yfinance"
        elif asset_type == "crypto":
            coin_id = str(row.get("coingecko_id") or "")
            price_info = crypto_prices.get(coin_id, {})
            price = price_info.get("price")
            previous_close = price_info.get("previous_close")
            source = "coingecko"
        else:
            price = row["avg_price"] or 1.0
            previous_close = row["avg_price"] or 1.0
            source = "internal"

        if price is None:
            continue
        upsert_price(symbol, asset_type, float(price), float(previous_close) if previous_close else None, source, as_of_date)
        refreshed += 1

    set_settings({"last_price_refresh_at": now.isoformat(timespec="seconds")})
    return True, f"Refreshed {refreshed} holding price(s)."


def build_net_worth(balances_df: pd.DataFrame) -> dict[str, float]:
    assets = float(balances_df.loc[balances_df["is_debt"] == 0, "display_balance"].sum())
    debt = float(balances_df.loc[balances_df["is_debt"] == 1, "display_balance"].clip(lower=0).sum())
    return {"assets": assets, "debt": debt, "net_worth": assets - debt}


def prepare_report_frames(transactions_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if transactions_df.empty:
        empty = pd.DataFrame()
        return {"spending": empty, "food": empty}

    tx = transactions_df.copy()
    tx["date"] = pd.to_datetime(tx["date"])
    spend = tx.loc[tx["type"] == "Expense"].copy()
    food = tx.loc[tx["category"] == "Food"].copy()
    return {"spending": spend, "food": food}


def render_signal(signal: Signal) -> None:
    level_class = f"term-signal-{signal.level}"
    title_text = signal.title.split(" ", 1)[-1] if signal.title[0] in "⚠📅🧾💸🛣🍽💰💳" else signal.title
    st.markdown(
        f"""
        <div class="term-signal {level_class}">
            <span class="term-signal-title">{title_text}</span>
            <span class="term-signal-body">{signal.body}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_dashboard(
    settings: dict[str, str],
    transactions_df: pd.DataFrame,
    holdings_df: pd.DataFrame,
    balances_df: pd.DataFrame,
    price_cache_df: pd.DataFrame,
) -> None:
    food = build_food_metrics(transactions_df, settings)
    runway = build_runway(transactions_df, balances_df, food)
    debt = build_debt_summary(balances_df)
    net_worth = build_net_worth(balances_df)
    signals = build_signals(balances_df, debt, food, runway, settings)
    latest_allocation = load_allocation_snapshots(limit=1)

    st.title("Black Book")
    render_signal(signals[0])

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Daily Food Left", format_currency(food["remaining_today"]), format_currency(-food["food_spent_today"]))
    m2.metric("Weekly Food Left", format_currency(food["remaining_week"]), format_currency(-food["food_spent_week"]))
    m3.metric("Net Worth", format_currency(net_worth["net_worth"]))
    m4.metric("Runway", f"{runway['runway_days']:.0f} days", format_currency(runway["avg_daily_spending"]) + "/day")

    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Total Debt", format_currency(debt["total_debt"]))
    s2.metric("Food Surplus", format_currency(food["current_carry_surplus"]))
    s3.metric("Lifetime Food Surplus", format_currency(food["lifetime_surplus"]))
    s4.metric("Transactions Today", str(food["transactions_today"]))

    left, right = st.columns([1.1, 0.9])

    with left:
        st.subheader("Accounts")
        rows_html = """
        <div class="term-accounts">
            <div class="term-acct-header">
                <span>Account</span><span>Balance</span><span>Type</span>
            </div>
        """
        for _, row in balances_df.sort_values("sort_order").iterrows():
            bal = float(row["display_balance"])
            bal_class = "neg" if bal < 0 else ""
            debt_class = "term-acct-debt" if row["is_debt"] else ""
            acct_type = "Debt" if row["is_debt"] else row["account_type"].title()
            rows_html += f"""
            <div class="term-acct-row {debt_class}">
                <span class="term-acct-name">{row["name"]}</span>
                <span class="term-acct-bal {bal_class}">{format_currency(bal)}</span>
                <span class="term-acct-type">{acct_type}</span>
            </div>
            """
        rows_html += "</div>"
        st.markdown(rows_html, unsafe_allow_html=True)

        st.subheader("Recent Money Moves")
        recent = transactions_df.head(8).copy() if not transactions_df.empty else pd.DataFrame()
        if recent.empty:
            st.info("No transactions logged yet.")
        else:
            recent["date"] = pd.to_datetime(recent["date"]).dt.strftime("%Y-%m-%d")
            recent["amount"] = recent["amount"].map(format_currency)
            st.dataframe(
                recent[["date", "description", "category", "amount", "account", "type", "to_account"]],
                use_container_width=True,
                hide_index=True,
            )

    with right:
        reports = prepare_report_frames(transactions_df)
        if not reports["spending"].empty:
            st.subheader("Spending Mix")
            spend_cat = reports["spending"].groupby("category", as_index=False)["amount"].sum()
            fig = px.pie(spend_cat, names="category", values="amount", hole=0.5)
            _chart_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

        food_trend = reports["food"]
        if not food_trend.empty:
            st.subheader("Food Trend")
            daily_food = food_trend.groupby(food_trend["date"].dt.date, as_index=False)["amount"].sum()
            fig = px.bar(daily_food, x="date", y="amount", color_discrete_sequence=["#00c896"])
            _chart_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Last Paycheck")
        if latest_allocation.empty:
            debt_df = debt["by_account"]
            preview = compute_paycheck_allocation(580.0, settings, debt_df)
            st.caption("Preview — no snapshot saved yet.")
        else:
            preview = latest_allocation.iloc[0].to_dict()
        alloc_df = pd.DataFrame(
            [
                ("Food", preview["food_reserved"]),
                ("Debt", preview["debt_reserved"]),
                ("Savings", preview["savings_reserved"]),
                ("Spending", preview["spending_reserved"]),
                ("Crypto", preview["crypto_reserved"]),
                ("Taxable", preview.get("taxable_reserved", 0.0)),
                ("Roth IRA", preview.get("roth_reserved", 0.0)),
            ],
            columns=["Bucket", "Amount"],
        )
        st.dataframe(
            alloc_df.assign(Amount=alloc_df["Amount"].map(format_currency)),
            use_container_width=True,
            hide_index=True,
        )


def render_log_transaction(accounts_df: pd.DataFrame) -> None:
    st.title("Log Transaction")

    account_name_to_id = dict(zip(
        accounts_df.sort_values("sort_order")["name"],
        accounts_df.sort_values("sort_order")["id"],
    ))
    with st.form("transaction_form", clear_on_submit=True):
        c1, c2, c3 = st.columns([1, 2, 1])
        tx_date = c1.date_input("Date", value=date.today())
        description = c2.text_input("Description", placeholder="Chick-fil-A, paycheck, debt payment...")
        amount = c3.number_input("Amount", min_value=0.0, step=0.01, format="%.2f")

        c4, c5, c6 = st.columns(3)
        category = c4.selectbox("Category", COMMON_CATEGORIES, index=0)
        account = c5.selectbox("Account", list(account_name_to_id))
        tx_type = c6.selectbox("Type", ["Expense", "Income", "Transfer"])

        to_account = None
        notes = st.text_area("Notes", placeholder="Optional note...")
        if tx_type == "Transfer":
            to_account = st.selectbox("To Account", [""] + [name for name in account_name_to_id if name != account])

        submitted = st.form_submit_button("Save Transaction", type="primary")
        if submitted:
            if not description.strip():
                st.error("Description is required.")
            elif amount <= 0:
                st.error("Amount must be greater than zero.")
            elif tx_type == "Transfer" and not to_account:
                st.error("Transfers need a destination account.")
            else:
                add_transaction(
                    tx_date=tx_date,
                    description=description,
                    category=category,
                    amount=float(amount),
                    account_id=int(account_name_to_id[account]),
                    tx_type=tx_type,
                    to_account_id=int(account_name_to_id[to_account]) if to_account else None,
                    notes=notes,
                )
                st.success("Transaction saved.")
                st.rerun()


def render_paycheck_allocation(settings: dict[str, str], balances_df: pd.DataFrame) -> None:
    st.title("Paycheck Allocation")

    debt_df = build_debt_summary(balances_df)["by_account"]
    c1, c2 = st.columns([0.9, 1.1])
    with c1:
        paycheck_amount = st.number_input("Paycheck Amount", min_value=0.0, value=580.0, step=10.0, format="%.2f")
        allocation = compute_paycheck_allocation(float(paycheck_amount), settings, debt_df)
        st.metric("Debt Total", format_currency(allocation["debt_total"]))
        st.metric("Food Reserve", format_currency(allocation["food_reserved"]))
        st.metric("Remaining After Food", format_currency(allocation["remaining_after_food"]))
        st.metric("Remaining After Debt", format_currency(allocation["remaining_after_debt"]))
        if st.button("Save Allocation Snapshot", type="primary"):
            save_allocation_snapshot(allocation)
            st.success("Paycheck allocation saved.")
            st.rerun()

    with c2:
        alloc_rows = [
            ("Food", allocation["food_reserved"]),
            ("Debt", allocation["debt_reserved"]),
            ("Savings", allocation["savings_reserved"]),
            ("Spending", allocation["spending_reserved"]),
            ("Crypto", allocation["crypto_reserved"]),
            ("Taxable Stocks", allocation["taxable_reserved"]),
            ("Roth IRA", allocation["roth_reserved"]),
        ]
        alloc_df = pd.DataFrame(alloc_rows, columns=["Bucket", "Amount"])
        alloc_df["Share of Pay"] = alloc_df["Amount"].apply(lambda x: safe_div(x, allocation["paycheck_amount"]))
        st.dataframe(
            alloc_df.assign(
                Amount=alloc_df["Amount"].map(format_currency),
                **{"Share of Pay": alloc_df["Share of Pay"].map(format_percent)},
            ),
            use_container_width=True,
            hide_index=True,
        )
        if allocation["debt_breakdown"]:
            st.subheader("Debt Payment Split")
            debt_breakdown_df = pd.DataFrame(allocation["debt_breakdown"])
            st.dataframe(
                debt_breakdown_df.assign(
                    debt_balance=debt_breakdown_df["debt_balance"].map(format_currency),
                    allocation=debt_breakdown_df["allocation"].map(format_currency),
                )[["account", "debt_balance", "allocation"]],
                use_container_width=True,
                hide_index=True,
            )

    history = load_allocation_snapshots(limit=8)
    st.subheader("Recent Allocation Snapshots")
    if history.empty:
        st.info("Save a paycheck allocation to build your history.")
    else:
        display = history[[
            "run_date",
            "paycheck_amount",
            "food_reserved",
            "debt_reserved",
            "savings_reserved",
            "spending_reserved",
            "crypto_reserved",
            "taxable_reserved",
            "roth_reserved",
        ]].copy()
        for col in display.columns:
            if col != "run_date":
                display[col] = display[col].map(format_currency)
        st.dataframe(display, use_container_width=True, hide_index=True)


def render_investments(holdings_df: pd.DataFrame, price_cache_df: pd.DataFrame) -> None:
    st.title("Investments")

    if holdings_df.empty:
        st.info("No holdings available yet. Import your workbook or add holdings directly in the database.")
        return

    refreshed, message = maybe_refresh_prices(holdings_df, force=False)
    if refreshed:
        price_cache_df = load_price_cache()
    if message:
        st.caption(message)

    c1, c2 = st.columns([0.7, 0.3])
    with c2:
        if st.button("Refresh Prices Now", type="primary"):
            _, msg = maybe_refresh_prices(holdings_df, force=True)
            st.success(msg)
            st.rerun()

    enriched = build_enriched_holdings(holdings_df, price_cache_df)
    total_value = float(enriched["current_value"].sum())
    total_invested = float(enriched["amount_invested"].sum())
    total_pnl = float(enriched["total_pnl"].sum())
    today_pnl = float(enriched["tdy_pnl"].sum())

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Portfolio Value", format_currency(total_value))
    m2.metric("Cost Basis", format_currency(total_invested))
    m3.metric("Total PnL", format_currency(total_pnl), format_percent(safe_div(total_pnl, total_invested)))
    m4.metric("Tdy PnL", format_currency(today_pnl))

    tab1, tab2, tab3 = st.tabs(["Tdy PnL", "Total PnL", "Price"])

    with tab1:
        view = enriched[["display_name", "account", "quantity", "latest_price", "previous_close", "tdy_pnl"]].copy()
        view["latest_price"] = view["latest_price"].map(format_currency)
        view["previous_close"] = view["previous_close"].map(format_currency)
        view["tdy_pnl"] = view["tdy_pnl"].map(format_currency)
        st.dataframe(view, use_container_width=True, hide_index=True)

    with tab2:
        view = enriched[["display_name", "account", "amount_invested", "current_value", "total_pnl", "total_pnl_pct"]].copy()
        view["amount_invested"] = view["amount_invested"].map(format_currency)
        view["current_value"] = view["current_value"].map(format_currency)
        view["total_pnl"] = view["total_pnl"].map(format_currency)
        view["total_pnl_pct"] = view["total_pnl_pct"].map(format_percent)
        st.dataframe(view, use_container_width=True, hide_index=True)

    with tab3:
        view = enriched[["display_name", "symbol", "account", "latest_price", "price_source", "fetched_at"]].copy()
        view["latest_price"] = view["latest_price"].map(format_currency)
        st.dataframe(view, use_container_width=True, hide_index=True)

    st.subheader("Allocation")
    alloc_col1, alloc_col2 = st.columns(2)
    with alloc_col1:
        by_account = enriched.groupby("account", as_index=False)["current_value"].sum()
        fig = px.pie(by_account, names="account", values="current_value", title="By Account", hole=0.5)
        _chart_theme(fig)
        st.plotly_chart(fig, use_container_width=True)
    with alloc_col2:
        by_asset = enriched.groupby("asset_type", as_index=False)["current_value"].sum()
        fig = px.pie(by_asset, names="asset_type", values="current_value", title="By Asset Type", hole=0.5)
        _chart_theme(fig)
        st.plotly_chart(fig, use_container_width=True)

    history = load_price_history()
    if not history.empty:
        value_history = history.merge(
            holdings_df[["symbol", "asset_type", "quantity"]],
            on=["symbol", "asset_type"],
            how="left",
        )
        value_history["quantity"] = value_history["quantity"].fillna(0.0)
        value_history["portfolio_value"] = value_history["price"] * value_history["quantity"]
        merged = value_history.groupby("as_of_date", as_index=False)["portfolio_value"].sum()
        fig = px.line(merged, x="as_of_date", y="portfolio_value", title="Portfolio Value",
                      color_discrete_sequence=["#00c896"])
        _chart_theme(fig)
        st.plotly_chart(fig, use_container_width=True)

    csv = enriched.to_csv(index=False).encode("utf-8")
    st.download_button("Export Holdings CSV", data=csv, file_name="budget_black_book_holdings.csv", mime="text/csv")


def render_reports(settings: dict[str, str], transactions_df: pd.DataFrame, holdings_df: pd.DataFrame, price_cache_df: pd.DataFrame) -> None:
    st.title("Reports")

    if transactions_df.empty:
        st.info("Log a few transactions and your reports will light up.")
        return

    tx = transactions_df.copy()
    tx["date"] = pd.to_datetime(tx["date"])
    min_date = tx["date"].min().date()
    max_date = tx["date"].max().date()
    c1, c2 = st.columns(2)
    start_date = c1.date_input("Start Date", value=min_date, min_value=min_date, max_value=max_date)
    end_date = c2.date_input("End Date", value=max_date, min_value=min_date, max_value=max_date)
    filtered = tx.loc[(tx["date"].dt.date >= start_date) & (tx["date"].dt.date <= end_date)].copy()

    expense_df = filtered.loc[filtered["type"] == "Expense"].copy()
    food_df = filtered.loc[filtered["category"] == "Food"].copy()

    chart1, chart2 = st.columns(2)
    with chart1:
        if not expense_df.empty:
            by_category = expense_df.groupby("category", as_index=False)["amount"].sum()
            fig = px.pie(by_category, names="category", values="amount", title="Spending by Category", hole=0.5)
            _chart_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

    with chart2:
        if not food_df.empty:
            food_trend = food_df.groupby(food_df["date"].dt.date, as_index=False)["amount"].sum()
            fig = px.bar(food_trend, x="date", y="amount", title="Food Trend",
                         color_discrete_sequence=["#00c896"])
            fig.add_hline(
                y=get_setting_float(settings, "daily_food_budget"),
                line_dash="dash",
                line_color="#f0a500",
                annotation_text="daily cap",
                annotation_font_color="#f0a500",
                annotation_font_size=10,
            )
            _chart_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

    chart3, chart4 = st.columns(2)
    with chart3:
        if not expense_df.empty:
            weekly_budget = get_setting_float(settings, "daily_food_budget") * 7
            weekly_group = expense_df.loc[expense_df["category"] == "Food"].copy()
            weekly_group["week"] = weekly_group["date"].dt.to_period("W").astype(str)
            food_week = weekly_group.groupby("week", as_index=False)["amount"].sum()
            if not food_week.empty:
                fig = go.Figure()
                fig.add_bar(x=food_week["week"], y=food_week["amount"], name="Food Spend",
                            marker_color="#00c896")
                fig.add_scatter(x=food_week["week"], y=[weekly_budget] * len(food_week),
                                name="Weekly Budget", mode="lines",
                                line=dict(color="#f0a500", dash="dash", width=1))
                fig.update_layout(title="Food vs Weekly Budget")
                _chart_theme(fig)
                st.plotly_chart(fig, use_container_width=True)

    with chart4:
        if not holdings_df.empty:
            enriched = build_enriched_holdings(holdings_df, price_cache_df)
            by_asset = enriched.groupby("asset_type", as_index=False)["current_value"].sum()
            fig = px.bar(by_asset, x="asset_type", y="current_value", title="Portfolio by Asset",
                         color_discrete_sequence=["#4da6ff"])
            _chart_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

    csv = filtered.copy()
    csv["date"] = csv["date"].dt.strftime("%Y-%m-%d")
    st.download_button("Export Transactions CSV", data=csv.to_csv(index=False).encode("utf-8"), file_name="budget_black_book_transactions.csv", mime="text/csv")


def render_settings(settings: dict[str, str], accounts_df: pd.DataFrame) -> None:
    st.title("Settings")

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
        taxable_pct = c8.number_input("Taxable Stocks %", min_value=0.0, max_value=1.0, value=get_setting_float(settings, "taxable_investing_pct"), step=0.01, format="%.2f")
        roth_pct = st.number_input("Roth IRA %", min_value=0.0, max_value=1.0, value=get_setting_float(settings, "roth_ira_pct"), step=0.01, format="%.2f")

        if not math.isclose(savings_pct + spending_pct + crypto_pct + taxable_pct + roth_pct, 1.0, abs_tol=0.0001):
            st.warning("Post-debt percentages should add up to 1.00 for a clean split.")

        st.subheader("Account Starting Balances")
        updated_balances = {}
        cols = st.columns(2)
        accounts_sorted = accounts_df.sort_values("sort_order").reset_index(drop=True)
        for idx, (_, row) in enumerate(accounts_sorted.iterrows()):
            acct_name = str(row["name"])
            updated_balances[row["id"]] = cols[idx % 2].number_input(
                acct_name,
                value=float(row["starting_balance"]),
                step=10.0,
                format="%.2f",
                key=f"starting_balance_{idx}",
            )

        submitted = st.form_submit_button("Save Settings", type="primary")
        if submitted:
            set_settings(
                {
                    "daily_food_budget": daily_food_budget,
                    "pay_period_days": pay_period_days,
                    "statement_day": statement_day,
                    "due_day": due_day,
                    "savings_pct": savings_pct,
                    "spending_pct": spending_pct,
                    "crypto_pct": crypto_pct,
                    "taxable_investing_pct": taxable_pct,
                    "roth_ira_pct": roth_pct,
                }
            )
            conn = get_connection()
            try:
                for account_id, balance in updated_balances.items():
                    db_execute(
                        conn,
                        "UPDATE accounts SET starting_balance = %s WHERE id = %s",
                        (float(balance), int(account_id)),
                    )
                conn.commit()
            finally:
                conn.close()
            st.success("Settings saved.")
            st.rerun()

    st.subheader("Export")
    tx_df = load_transactions()
    holdings_df = load_holdings()
    st.download_button(
        "Export Transactions CSV",
        data=tx_df.to_csv(index=False).encode("utf-8"),
        file_name="budget_black_book_transactions.csv",
        mime="text/csv",
    )
    st.download_button(
        "Export Holdings CSV",
        data=holdings_df.to_csv(index=False).encode("utf-8"),
        file_name="budget_black_book_holdings.csv",
        mime="text/csv",
    )


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

    page = st.sidebar.radio(
        "Navigate",
        ["Dashboard", "Log Transaction", "Paycheck Allocation", "Investments", "Reports", "Settings"],
    )
    st.sidebar.markdown("---")
    st.sidebar.caption("📊 Budget Black Book")
    if IS_POSTGRES:
        st.sidebar.caption("DB: PostgreSQL (cloud)")
    else:
        st.sidebar.caption(f"DB: `{DB_PATH}`")

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
