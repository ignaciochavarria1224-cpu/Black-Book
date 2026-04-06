"""
Budget Black Book — Cloud Edition
"""

from __future__ import annotations

import json
import math
import os
import re
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
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

try:
    import google.generativeai as genai
    _GENAI_AVAILABLE = True
except ImportError:
    _GENAI_AVAILABLE = False

try:
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build as google_build
    _GOOGLE_AVAILABLE = True
except ImportError:
    _GOOGLE_AVAILABLE = False

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
    "debt_allocation_mode": "proportional", "migration_completed": "0",
    "last_price_refresh_at": "", "next_payday": "",
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
JOURNAL_TAGS = ["General", "Finance", "Reflection", "Decision", "Goals", "Other"]
CRYPTO_NAME_TO_ID = {
    "XRP": "ripple", "Bitcoin (BTC)": "bitcoin", "Bittensor (TAO)": "bittensor",
    "Worldcoin (WLD)": "worldcoin-wld", "Sui (SUI)": "sui",
    "Solana (SOL)": "solana", "Cash (USD)": "",
}
STOCK_NAME_TO_TICKER = {
    "NVIDIA (NVDA)": "NVDA", "Palantir (PLTR)": "PLTR", "Tesla (TSLA)": "TSLA",
    "Invesco QQQ (QQQ)": "QQQ", "SPDR S&P 500 (SPY)": "SPY",
}

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


# ── Utility: strip model thinking artifacts ───────────────────────────────────

def strip_thinking(text: str) -> str:
    """Remove <think>...</think> blocks, triple-backtick fences, and inline backticks."""
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'```[^`]*```', lambda m: re.sub(r'^```[^\n]*\n?', '', m.group().rstrip('`').rstrip()), text, flags=re.DOTALL)
    text = re.sub(r'`([^`]+)`', r'\1', text)
    return text.strip()


# ── CSS ───────────────────────────────────────────────────────────────────────

def inject_css() -> None:
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,700;0,900;1,700&family=JetBrains+Mono:wght@400;500;600&display=swap');

    /* ── Palette: Old Money meets New Age ── */
    :root {
        --gold: #C9A84C;
        --gold-dim: rgba(201,168,76,0.12);
        --gold-border: rgba(201,168,76,0.2);
        --cream: #F2EDE4;
        --cream-dim: #9A9080;
        --bg-deep: #080810;
        --bg-card: #0D0D18;
        --bg-elevated: #121220;
        --border: rgba(255,255,255,0.05);
        --green: #00c896;
        --red: #ff4d4d;
        --gold-accent: #f0a500;
    }

    .main > div { padding-top: 0.5rem; }

    /* ── Sidebar ── */
    section[data-testid="stSidebar"] {
        background: #07070F;
        border-right: 1px solid var(--gold-border);
    }
    section[data-testid="stSidebar"] > div {
        display: flex;
        flex-direction: column;
        height: 100vh;
        padding-bottom: 1rem;
    }

    /* ── Sidebar brand ── */
    .bb-sidebar-brand {
        font-family: 'Playfair Display', serif;
        font-size: 1.6rem;
        font-weight: 900;
        color: var(--gold);
        letter-spacing: 0.04em;
        line-height: 1.1;
        padding: 0.2rem 0 0.1rem 0;
    }
    .bb-sidebar-year {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.55rem;
        letter-spacing: 0.3em;
        color: rgba(201,168,76,0.35);
        text-transform: uppercase;
        margin-top: 0.15rem;
        margin-bottom: 0.8rem;
    }

    /* ── Sidebar nav ── */
    [data-testid="stRadio"] > div { gap: 0 !important; }
    [data-testid="stRadio"] label {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.68rem !important;
        letter-spacing: 0.1em !important;
        color: var(--cream-dim) !important;
        padding: 0.45rem 0.6rem !important;
        border-radius: 0 !important;
        border-left: 1px solid transparent !important;
        transition: all 0.15s ease !important;
        text-transform: uppercase !important;
    }
    [data-testid="stRadio"] label:hover {
        color: var(--cream) !important;
        border-left-color: var(--gold-border) !important;
        background: var(--gold-dim) !important;
    }
    [data-testid="stRadio"] label[data-selected="true"],
    [data-testid="stRadio"] label[aria-checked="true"] {
        color: var(--gold) !important;
        border-left: 1px solid var(--gold) !important;
        background: var(--gold-dim) !important;
    }

    /* ── Sidebar bottom label ── */
    .bb-sidebar-footer {
        margin-top: auto;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.55rem;
        letter-spacing: 0.15em;
        color: rgba(201,168,76,0.25);
        text-transform: uppercase;
        padding-top: 1rem;
        border-top: 1px solid var(--gold-border);
    }

    /* ── Page titles ── */
    .bb-title {
        font-family: 'Playfair Display', serif;
        font-size: 2.4rem;
        font-weight: 900;
        letter-spacing: 0.03em;
        color: var(--cream);
        line-height: 1;
        margin-bottom: 0;
    }
    .bb-subtitle {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.58rem;
        letter-spacing: 0.28em;
        color: rgba(201,168,76,0.3);
        text-transform: uppercase;
        margin-bottom: 1.2rem;
        border-bottom: 1px solid var(--gold-border);
        padding-bottom: 0.8rem;
    }

    /* ── Section headers ── */
    h2, h3 {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.6rem !important;
        font-weight: 500 !important;
        letter-spacing: 0.22em !important;
        text-transform: uppercase !important;
        color: rgba(201,168,76,0.4) !important;
        margin-top: 1.5rem !important;
        margin-bottom: 0.5rem !important;
    }

    /* ── Metrics ── */
    [data-testid="stMetric"] {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-top: 1px solid var(--gold-border);
        border-radius: 0;
        padding: 0.9rem 1rem 0.7rem 1rem;
        position: relative;
    }
    [data-testid="stMetricLabel"] p {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.58rem !important;
        text-transform: uppercase;
        letter-spacing: 0.15em;
        color: var(--cream-dim) !important;
    }
    [data-testid="stMetricValue"] {
        font-family: 'Playfair Display', serif !important;
        font-size: 1.3rem !important;
        color: var(--cream) !important;
    }
    [data-testid="stMetricDelta"] {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.68rem !important;
    }

    /* ── DataFrames ── */
    [data-testid="stDataFrame"] {
        border: 1px solid var(--gold-border) !important;
        border-radius: 0 !important;
    }

    /* ── Buttons ── */
    [data-testid="baseButton-primary"] {
        background: var(--gold) !important;
        color: #080810 !important;
        border: none !important;
        border-radius: 0 !important;
        letter-spacing: 0.12em;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.68rem !important;
        font-weight: 600 !important;
        text-transform: uppercase;
    }
    [data-testid="baseButton-primary"]:hover {
        background: #D4B55A !important;
    }
    [data-testid="baseButton-secondary"] {
        background: transparent !important;
        color: var(--cream-dim) !important;
        border: 1px solid rgba(255,255,255,0.08) !important;
        border-radius: 0 !important;
        letter-spacing: 0.1em;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.65rem !important;
        text-transform: uppercase;
        transition: all 0.15s ease !important;
    }
    [data-testid="baseButton-secondary"]:hover {
        border-color: var(--gold-border) !important;
        color: var(--cream) !important;
    }

    /* ── Cards ── */
    .bb-report-card {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-top: 1px solid var(--gold-border);
        border-radius: 0;
        padding: 1rem;
        margin-bottom: 0.75rem;
    }
    .bb-report-date {
        font-family: 'Playfair Display', serif;
        font-size: 1rem;
        color: var(--cream);
        margin-bottom: 0.5rem;
    }
    .bb-report-row {
        display: flex;
        justify-content: space-between;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.7rem;
        color: var(--cream-dim);
        padding: 0.15rem 0;
        border-bottom: 1px solid rgba(255,255,255,0.02);
    }
    .bb-report-val { color: var(--cream); }

    /* ── Journal ── */
    .bb-journal-entry {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-left: 2px solid var(--gold-border);
        border-radius: 0;
        padding: 1rem;
        margin-bottom: 0.75rem;
    }
    .bb-journal-header { display: flex; justify-content: space-between; margin-bottom: 0.5rem; }
    .bb-journal-date {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.62rem;
        letter-spacing: 0.1em;
        color: var(--cream-dim);
    }
    .bb-journal-tag {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.58rem;
        letter-spacing: 0.12em;
        color: var(--gold);
        text-transform: uppercase;
    }
    .bb-journal-body {
        font-size: 0.85rem;
        color: #9ca3af;
        line-height: 1.7;
        white-space: pre-wrap;
    }

    /* ── Memory entries ── */
    .bb-memory-entry {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-left: 2px solid rgba(201,168,76,0.25);
        border-radius: 0;
        padding: 0.7rem 0.9rem;
        margin-bottom: 0.5rem;
    }

    /* ── Advisor chat ── */
    .bb-advisor-controls {
        display: flex;
        gap: 0.5rem;
        justify-content: flex-end;
        margin-bottom: 1.5rem;
    }
    .bb-advisor-btn {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.62rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        padding: 0.45rem 1rem;
        border: 1px solid rgba(255,255,255,0.08);
        background: transparent;
        color: var(--cream-dim);
        cursor: pointer;
        transition: all 0.15s ease;
    }
    .bb-advisor-btn:hover { border-color: var(--gold-border); color: var(--cream); }
    .bb-advisor-btn.save { border-color: var(--gold-border); color: var(--gold); }

    /* ── Strip code block green styling from advisor responses ── */
    .bb-response code, .bb-response pre {
        background: transparent !important;
        color: inherit !important;
        font-family: inherit !important;
        font-size: inherit !important;
        border: none !important;
        padding: 0 !important;
    }

    /* ── Inputs ── */
    .stTextArea textarea {
        background: var(--bg-card) !important;
        border: 1px solid var(--gold-border) !important;
        border-radius: 0 !important;
        color: var(--cream) !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.85rem !important;
        resize: none !important;
    }
    .stTextArea textarea:focus {
        border-color: var(--gold) !important;
        box-shadow: none !important;
    }
    .stTextInput input {
        background: var(--bg-card) !important;
        border: 1px solid var(--gold-border) !important;
        border-radius: 0 !important;
        color: var(--cream) !important;
    }

    /* ── Tabs ── */
    [data-testid="stTabs"] [role="tablist"] {
        border-bottom: 1px solid var(--gold-border) !important;
        gap: 0 !important;
    }
    [data-testid="stTabs"] button[role="tab"] {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.62rem !important;
        letter-spacing: 0.15em !important;
        text-transform: uppercase !important;
        color: var(--cream-dim) !important;
        border-radius: 0 !important;
        border-bottom: 1px solid transparent !important;
        padding: 0.5rem 1.2rem !important;
        background: transparent !important;
    }
    [data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
        color: var(--gold) !important;
        border-bottom: 1px solid var(--gold) !important;
        background: transparent !important;
    }

    /* ── Dividers ── */
    hr { border-color: var(--gold-border) !important; }

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
    """Definitive fix for RealDictCursor + pandas — converts each row to plain dict first."""
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
        f"CREATE TABLE IF NOT EXISTS allocation_snapshots (id {serial} PRIMARY KEY {ai}, paycheck_amount REAL NOT NULL, run_date TEXT NOT NULL, debt_total REAL NOT NULL, food_reserved REAL NOT NULL, debt_reserved REAL NOT NULL, savings_reserved REAL NOT NULL, surplus_savings REAL NOT NULL DEFAULT 0, spending_reserved REAL NOT NULL, crypto_reserved REAL NOT NULL, taxable_reserved REAL NOT NULL, roth_reserved REAL NOT NULL, debt_breakdown_json TEXT NOT NULL, meta_json TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS price_cache (symbol TEXT NOT NULL, asset_type TEXT NOT NULL, price REAL NOT NULL, previous_close REAL, currency TEXT NOT NULL DEFAULT 'USD', source TEXT NOT NULL, as_of_date TEXT NOT NULL, fetched_at TEXT NOT NULL, PRIMARY KEY(symbol, asset_type))",
        f"CREATE TABLE IF NOT EXISTS price_history (id {serial} PRIMARY KEY {ai}, symbol TEXT NOT NULL, asset_type TEXT NOT NULL, price REAL NOT NULL, previous_close REAL, as_of_date TEXT NOT NULL, source TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        f"CREATE TABLE IF NOT EXISTS daily_reports (id {serial} PRIMARY KEY {ai}, report_date TEXT NOT NULL UNIQUE, snapshot_json TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        f"CREATE TABLE IF NOT EXISTS journal_entries (id {serial} PRIMARY KEY {ai}, entry_date TEXT NOT NULL, tag TEXT NOT NULL DEFAULT 'General', body TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        # Advisor memory — stored in DB so it persists across deployments on Streamlit Cloud
        f"CREATE TABLE IF NOT EXISTS advisor_memory (id {serial} PRIMARY KEY {ai}, memory_date TEXT NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        # Advisor conversation history
        f"CREATE TABLE IF NOT EXISTS advisor_conversations (id {serial} PRIMARY KEY {ai}, session_id TEXT NOT NULL, role TEXT NOT NULL, content TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)",
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


# ── Data access ───────────────────────────────────────────────────────────────

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
        df = _cursor_to_df(db_execute(conn, "SELECT id, name, account_type, is_debt, include_in_runway, starting_balance, sort_order FROM accounts ORDER BY sort_order, name"))
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


# ── Transactions ──────────────────────────────────────────────────────────────

def add_transaction(tx_date: date, description: str, category: str, amount: float,
                    account_id: int, tx_type: str, to_account_id: int | None, notes: str) -> None:
    conn = get_connection()
    try:
        db_execute(conn, "INSERT INTO transactions (date, description, category, amount, account_id, type, to_account_id, notes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                   (tx_date.strftime(DATE_FMT), description.strip(), category, float(amount),
                    int(account_id), tx_type, int(to_account_id) if to_account_id else None, notes.strip() or None))
        conn.commit()
    finally:
        conn.close()


def delete_transaction(tx_id: int) -> None:
    conn = get_connection()
    try:
        db_execute(conn, "DELETE FROM transactions WHERE id = %s", (int(tx_id),))
        conn.commit()
    finally:
        conn.close()


def load_transactions() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = _cursor_to_df(db_execute(conn, """
            SELECT t.id, t.date, t.description, t.category, t.amount, t.type, t.notes,
                   a.name AS account, a.id AS account_id,
                   ta.name AS to_account, ta.id AS to_account_id
            FROM transactions t
            JOIN accounts a ON a.id = t.account_id
            LEFT JOIN accounts ta ON ta.id = t.to_account_id
            ORDER BY t.date DESC, t.id DESC"""))
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


# ── Holdings ──────────────────────────────────────────────────────────────────

def add_holding(symbol: str, display_name: str, asset_type: str, account_id: int,
                amount_invested: float, quantity: float, avg_price: float, coingecko_id: str) -> None:
    conn = get_connection()
    try:
        db_execute(conn, "INSERT INTO holdings (symbol, display_name, asset_type, account_id, amount_invested, quantity, avg_price, coingecko_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                   (symbol.upper().strip(), display_name.strip(), asset_type, int(account_id),
                    float(amount_invested), float(quantity), float(avg_price), coingecko_id.strip() or None))
        conn.commit()
    finally:
        conn.close()


def update_holding(holding_id: int, amount_invested: float, quantity: float, avg_price: float) -> None:
    conn = get_connection()
    try:
        db_execute(conn, "UPDATE holdings SET amount_invested=%s, quantity=%s, avg_price=%s, updated_at=CURRENT_TIMESTAMP WHERE id=%s",
                   (float(amount_invested), float(quantity), float(avg_price), int(holding_id)))
        conn.commit()
    finally:
        conn.close()


def delete_holding(holding_id: int) -> None:
    conn = get_connection()
    try:
        db_execute(conn, "DELETE FROM holdings WHERE id = %s", (int(holding_id),))
        conn.commit()
    finally:
        conn.close()


def load_holdings() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = _cursor_to_df(db_execute(conn, """
            SELECT h.id, h.symbol, h.display_name, h.asset_type,
                   h.amount_invested, h.quantity, h.avg_price, h.coingecko_id,
                   a.name AS account, a.id AS account_id
            FROM holdings h JOIN accounts a ON a.id = h.account_id
            ORDER BY a.sort_order, h.display_name"""))
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


# ── Allocation snapshots ──────────────────────────────────────────────────────

def save_allocation_snapshot(payload: dict[str, Any]) -> None:
    conn = get_connection()
    try:
        db_execute(conn, """INSERT INTO allocation_snapshots
            (paycheck_amount, run_date, debt_total, food_reserved, debt_reserved,
             savings_reserved, surplus_savings, spending_reserved, crypto_reserved,
             taxable_reserved, roth_reserved, debt_breakdown_json, meta_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                   (payload["paycheck_amount"], payload["run_date"], payload["debt_total"],
                    payload["food_reserved"], payload["debt_reserved"], payload["savings_reserved"],
                    payload.get("surplus_savings", 0.0), payload["spending_reserved"],
                    payload["crypto_reserved"], payload["taxable_reserved"],
                    payload["roth_reserved"], json.dumps(payload["debt_breakdown"]), json.dumps(payload["meta"])))
        conn.commit()
    finally:
        conn.close()


def load_allocation_snapshots(limit: int = 10) -> pd.DataFrame:
    conn = get_connection()
    try:
        sql = "SELECT * FROM allocation_snapshots ORDER BY run_date DESC, id DESC LIMIT %s" if IS_POSTGRES \
              else "SELECT * FROM allocation_snapshots ORDER BY date(run_date) DESC, id DESC LIMIT ?"
        df = _cursor_to_df(db_execute(conn, sql, (limit,)))
    finally:
        conn.close()
    if df.empty:
        return df
    for col in ("paycheck_amount", "debt_total", "food_reserved", "debt_reserved", "savings_reserved",
                "surplus_savings", "spending_reserved", "crypto_reserved", "taxable_reserved", "roth_reserved"):
        if col in df.columns:
            df[col] = _to_float_series(df[col])
    return df


# ── Price data ────────────────────────────────────────────────────────────────

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


# ── Daily reports ─────────────────────────────────────────────────────────────

def save_daily_report(report_date: str, snapshot: dict[str, Any]) -> None:
    conn = get_connection()
    try:
        db_execute(conn, "INSERT INTO daily_reports (report_date, snapshot_json) VALUES (%s, %s) ON CONFLICT(report_date) DO NOTHING",
                   (report_date, json.dumps(snapshot)))
        conn.commit()
    finally:
        conn.close()


def load_daily_reports(limit: int = 30) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        sql = "SELECT report_date, snapshot_json FROM daily_reports ORDER BY report_date DESC LIMIT %s" if IS_POSTGRES \
              else "SELECT report_date, snapshot_json FROM daily_reports ORDER BY report_date DESC LIMIT ?"
        rows = db_execute(conn, sql, (limit,)).fetchall()
    finally:
        conn.close()
    result = []
    for r in rows:
        try:
            snap = json.loads(r["snapshot_json"])
            snap["report_date"] = r["report_date"]
            result.append(snap)
        except Exception:
            pass
    return result


def delete_daily_report(report_date: str) -> None:
    conn = get_connection()
    try:
        db_execute(conn, "DELETE FROM daily_reports WHERE report_date = %s", (report_date,))
        conn.commit()
    finally:
        conn.close()


def update_daily_report(report_date: str, snapshot: dict[str, Any]) -> None:
    conn = get_connection()
    try:
        db_execute(conn, "UPDATE daily_reports SET snapshot_json = %s WHERE report_date = %s",
                   (json.dumps(snapshot), report_date))
        conn.commit()
    finally:
        conn.close()


def report_exists(report_date: str) -> bool:
    conn = get_connection()
    try:
        row = db_execute(conn, "SELECT COUNT(*) AS count FROM daily_reports WHERE report_date = %s", (report_date,)).fetchone()
    finally:
        conn.close()
    return bool(int(row["count"]))


def maybe_generate_yesterday_report(accounts_df, transactions_df, holdings_df, price_cache_df, settings) -> None:
    yesterday = (date.today() - timedelta(days=1)).strftime(DATE_FMT)
    if report_exists(yesterday):
        return
    try:
        tx_yesterday = transactions_df[pd.to_datetime(transactions_df["date"]).dt.date <= date.today() - timedelta(days=1)].copy() if not transactions_df.empty else transactions_df
        balances = build_account_balances(accounts_df, tx_yesterday, holdings_df, price_cache_df)
        food = build_food_metrics(tx_yesterday, settings)
        runway = build_runway(tx_yesterday, balances, food)
        debt = build_debt_summary(balances)
        nw = build_net_worth(balances)
        enriched_h = build_enriched_holdings(holdings_df, price_cache_df) if not holdings_df.empty else pd.DataFrame()
        snapshot = {
            "net_worth": nw["net_worth"], "assets": nw["assets"], "debt": debt["total_debt"],
            "runway_days": round(runway["runway_days"], 1), "liquid_cash": nw["assets"],
            "food_spent": food["food_spent_today"], "food_surplus": food["current_carry_surplus"],
            "portfolio_value": float(enriched_h["current_value"].sum()) if not enriched_h.empty else 0.0,
            "portfolio_pnl": float(enriched_h["total_pnl"].sum()) if not enriched_h.empty else 0.0,
            "txn_count": len(tx_yesterday[pd.to_datetime(tx_yesterday["date"]).dt.date == date.today() - timedelta(days=1)]) if not tx_yesterday.empty else 0,
            "accounts": {str(r["name"]): round(float(r["display_balance"]), 2) for _, r in balances.iterrows()},
        }
        save_daily_report(yesterday, snapshot)
    except Exception:
        pass


# ── Journal ───────────────────────────────────────────────────────────────────

def save_journal_entry(entry_date: date, tag: str, body: str) -> None:
    conn = get_connection()
    try:
        db_execute(conn, "INSERT INTO journal_entries (entry_date, tag, body) VALUES (%s, %s, %s)",
                   (entry_date.strftime(DATE_FMT), tag, body.strip()))
        conn.commit()
    finally:
        conn.close()


def load_journal_entries(limit: int = 50, tag_filter: str = "All") -> pd.DataFrame:
    conn = get_connection()
    try:
        if tag_filter == "All":
            sql = "SELECT id, entry_date, tag, body FROM journal_entries ORDER BY entry_date DESC, id DESC LIMIT %s" if IS_POSTGRES \
                  else "SELECT id, entry_date, tag, body FROM journal_entries ORDER BY entry_date DESC, id DESC LIMIT ?"
            df = _cursor_to_df(db_execute(conn, sql, (limit,)))
        else:
            sql = "SELECT id, entry_date, tag, body FROM journal_entries WHERE tag = %s ORDER BY entry_date DESC, id DESC LIMIT %s" if IS_POSTGRES \
                  else "SELECT id, entry_date, tag, body FROM journal_entries WHERE tag = ? ORDER BY entry_date DESC, id DESC LIMIT ?"
            df = _cursor_to_df(db_execute(conn, sql, (tag_filter, limit)))
    finally:
        conn.close()
    return df


def delete_journal_entry(entry_id: int) -> None:
    conn = get_connection()
    try:
        db_execute(conn, "DELETE FROM journal_entries WHERE id = %s", (int(entry_id),))
        conn.commit()
    finally:
        conn.close()


# ── Advisor memory (stored in DB — filesystem is read-only on Streamlit Cloud) ──

def save_advisor_memory_to_db(body: str) -> None:
    """Append a memory entry to the database."""
    conn = get_connection()
    try:
        db_execute(conn, "INSERT INTO advisor_memory (memory_date, body) VALUES (%s, %s)",
                   (date.today().strftime(DATE_FMT), body.strip()))
        conn.commit()
    finally:
        conn.close()


def load_advisor_memory(limit: int = 50) -> str:
    """Load all memory entries as a single formatted string for the system prompt."""
    conn = get_connection()
    try:
        sql = "SELECT memory_date, body FROM advisor_memory ORDER BY memory_date DESC, id DESC LIMIT %s" if IS_POSTGRES \
              else "SELECT memory_date, body FROM advisor_memory ORDER BY memory_date DESC, id DESC LIMIT ?"
        rows = db_execute(conn, sql, (limit,)).fetchall()
    finally:
        conn.close()
    if not rows:
        return "No memory entries yet."
    return "\n\n".join(f"[{r['memory_date']}]\n{r['body']}" for r in rows)


def load_advisor_memory_df(limit: int = 50) -> pd.DataFrame:
    """Load memory entries as DataFrame for display."""
    conn = get_connection()
    try:
        sql = "SELECT id, memory_date, body FROM advisor_memory ORDER BY memory_date DESC, id DESC LIMIT %s" if IS_POSTGRES \
              else "SELECT id, memory_date, body FROM advisor_memory ORDER BY memory_date DESC, id DESC LIMIT ?"
        df = _cursor_to_df(db_execute(conn, sql, (limit,)))
    finally:
        conn.close()
    return df


def delete_advisor_memory_entry(entry_id: int) -> None:
    conn = get_connection()
    try:
        db_execute(conn, "DELETE FROM advisor_memory WHERE id = %s", (int(entry_id),))
        conn.commit()
    finally:
        conn.close()


# ── Advisor conversation history ──────────────────────────────────────────────

def save_conversation_message(session_id: str, role: str, content: str) -> None:
    conn = get_connection()
    try:
        db_execute(conn, "INSERT INTO advisor_conversations (session_id, role, content) VALUES (%s, %s, %s)",
                   (session_id, role, content))
        conn.commit()
    finally:
        conn.close()


def load_conversation_history(session_id: str) -> list[dict]:
    conn = get_connection()
    try:
        sql = "SELECT role, content FROM advisor_conversations WHERE session_id = %s ORDER BY created_at ASC, id ASC" if IS_POSTGRES \
              else "SELECT role, content FROM advisor_conversations WHERE session_id = ? ORDER BY created_at ASC, id ASC"
        rows = db_execute(conn, sql, (session_id,)).fetchall()
    finally:
        conn.close()
    return [{"role": str(r["role"]), "content": str(r["content"])} for r in rows]


def list_conversation_sessions(limit: int = 20) -> pd.DataFrame:
    conn = get_connection()
    try:
        sql = """SELECT session_id,
                        MIN(created_at) AS created_at,
                        (SELECT content FROM advisor_conversations c2
                         WHERE c2.session_id = c1.session_id AND c2.role = 'user'
                         ORDER BY c2.created_at ASC, c2.id ASC LIMIT 1) AS first_message
                 FROM advisor_conversations c1
                 GROUP BY session_id
                 ORDER BY created_at DESC
                 LIMIT %s""" if IS_POSTGRES else \
              """SELECT session_id,
                        MIN(created_at) AS created_at,
                        (SELECT content FROM advisor_conversations c2
                         WHERE c2.session_id = c1.session_id AND c2.role = 'user'
                         ORDER BY c2.created_at ASC, c2.id ASC LIMIT 1) AS first_message
                 FROM advisor_conversations c1
                 GROUP BY session_id
                 ORDER BY created_at DESC
                 LIMIT ?"""
        df = _cursor_to_df(db_execute(conn, sql, (limit,)))
    finally:
        conn.close()
    return df


def delete_conversation_session(session_id: str) -> None:
    conn = get_connection()
    try:
        db_execute(conn, "DELETE FROM advisor_conversations WHERE session_id = %s", (session_id,))
        conn.commit()
    finally:
        conn.close()


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
              Path.home() / "Downloads" / "Budget Black Book copy.xlsx",
              Path.home() / "Downloads" / "Budget Black Book.xlsx"]:
        if c.exists(): return c
    return None


def normalize_account_name(name: Any) -> str:
    value = str(name or "").strip()
    return {"Savor (CC)": "Savor", "Venture (CC)": "Venture",
            "Roth IRA (Fidelity)": "Roth IRA", "Investments (Fidelity)": "Investments"}.get(value, value)


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
    except Exception as exc:
        return f"Import failed: {exc}"
    conn = get_connection()
    try:
        lu = {"Daily Budget": ("daily_food_budget", "numeric"),
              "Checking — Starting Balance": ("Checking", "account"),
              "Savings — Starting Balance": ("Savings", "account")}
        su: dict[str, str] = {}; ab: dict[str, float] = {}
        for _, row in home_df.iterrows():
            label = str(row.iloc[2]).strip() if len(row) > 2 and pd.notna(row.iloc[2]) else ""
            value = row.iloc[4] if len(row) > 4 else None
            if label in lu and pd.notna(value):
                target, kind = lu[label]
                if kind == "numeric": su[target] = str(float(value))
                else: ab[target] = float(value)
        for name, balance in ab.items():
            db_execute(conn, "UPDATE accounts SET starting_balance = %s WHERE id = %s",
                       (float(balance), ensure_account(conn, name)))
        for k, v in su.items():
            db_execute(conn, "INSERT INTO settings (key, value) VALUES (%s,%s) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (k, v))
        if not table_exists_with_rows("transactions"):
            spending_df.columns = [str(c).strip() for c in spending_df.columns]
            spending_df = spending_df.dropna(how="all")
            for _, row in spending_df.iterrows():
                tx_date = excel_serial_to_date(row.get("Date"))
                desc = str(row.get("Description") or "").strip()
                cat = str(row.get("Category") or "Other").strip()
                amount = row.get("Amount")
                acct = normalize_account_name(row.get("Account"))
                ttype = str(row.get("Type") or "Expense").strip()
                to_acct = normalize_account_name(row.get("To Account"))
                if not tx_date or not desc or pd.isna(amount) or not acct: continue
                aid = ensure_account(conn, acct)
                taid = ensure_account(conn, to_acct) if to_acct else None
                db_execute(conn, "INSERT INTO transactions (date, description, category, amount, account_id, type, to_account_id, notes) VALUES (%s,%s,%s,%s,%s,%s,%s,NULL)",
                           (tx_date.strftime(DATE_FMT), desc, cat, float(amount), aid, ttype, taid))
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
    fig.update_layout(
        title=dict(text=title, font=dict(family="JetBrains Mono, monospace", size=10, color="#374151"),
                   x=0, xanchor="left", pad=dict(l=0, b=8)) if title else dict(text=""),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#6b7280", size=10, family="JetBrains Mono, monospace"),
        legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="rgba(255,255,255,0.04)",
                    borderwidth=1, font=dict(size=9, family="JetBrains Mono, monospace")),
        margin=dict(l=0, r=0, t=28 if title else 4, b=0),
        hoverlabel=dict(bgcolor="#0d1117", bordercolor="rgba(255,255,255,0.1)",
                        font=dict(family="JetBrains Mono, monospace", size=10)),
    )
    fig.update_xaxes(gridcolor="rgba(255,255,255,0.03)", zerolinecolor="rgba(255,255,255,0.05)",
                     tickfont=dict(size=9, family="JetBrains Mono, monospace"), showline=False)
    fig.update_yaxes(gridcolor="rgba(255,255,255,0.03)", zerolinecolor="rgba(255,255,255,0.05)",
                     tickfont=dict(size=9, family="JetBrains Mono, monospace"), showline=False)
    return fig


def _pie_chart(labels, values, title="") -> go.Figure:
    fig = go.Figure(go.Pie(
        labels=labels, values=values, hole=0.6,
        marker=dict(colors=CHART_PALETTE, line=dict(color="#060810", width=2)),
        textfont=dict(family="JetBrains Mono, monospace", size=9), textposition="outside",
        hovertemplate="<b>%{label}</b><br>%{value:,.2f}<br>%{percent}<extra></extra>",
    ))
    _chart_theme(fig, title)
    fig.update_layout(showlegend=True, legend=dict(orientation="v", x=1.02, y=0.5))
    return fig


def _bar_chart(x, y, color=C_GREEN, title="", hline=None) -> go.Figure:
    fig = go.Figure(go.Bar(x=x, y=y, marker=dict(color=color, opacity=0.85, line=dict(width=0)),
                           hovertemplate="%{x}<br><b>$%{y:,.2f}</b><extra></extra>"))
    if hline is not None:
        fig.add_hline(y=hline, line_dash="dot", line_color=C_GOLD, line_width=1,
                      annotation_text="cap", annotation_font=dict(color=C_GOLD, size=8,
                      family="JetBrains Mono, monospace"), annotation_position="top right")
    _chart_theme(fig, title)
    return fig


def _line_chart(x, y, color=C_GREEN, title="") -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x, y=y, mode="lines", line=dict(color=color, width=2),
                             fill="tozeroy", fillcolor="rgba(0,200,150,0.06)",
                             hovertemplate="<b>$%{y:,.2f}</b><extra></extra>"))
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
    enriched["total_pnl_pct"] = enriched.apply(
        lambda r: safe_div(r["total_pnl"], r["amount_invested"]) if r["amount_invested"] else 0.0, axis=1)
    enriched["tdy_pnl"] = (enriched["latest_price"] - enriched["previous_close"]) * enriched["quantity"]
    return enriched


def build_account_balances(accounts_df, transactions_df, holdings_df, price_cache_df) -> pd.DataFrame:
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
        "transactions_today": int(0 if transactions_df.empty else
                                  (pd.to_datetime(transactions_df["date"]).dt.date == today).sum()),
    }


def build_runway(transactions_df, balances_df, food_metrics) -> dict[str, float]:
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
    return {"total_debt": float(ddf["display_balance"].sum()),
            "by_account": ddf.sort_values("display_balance", ascending=False)}


def build_net_worth(balances_df: pd.DataFrame) -> dict[str, float]:
    assets = float(balances_df.loc[balances_df["is_debt"] == 0, "display_balance"].sum())
    debt = float(balances_df.loc[balances_df["is_debt"] == 1, "display_balance"].clip(lower=0).sum())
    return {"assets": assets, "debt": debt, "net_worth": assets - debt}


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
    if food_metrics["current_carry_surplus"] > food_metrics["daily_budget"] * 3:
        signals.append(Signal("success", "🍽 Food discipline paying off", "Healthy carry surplus built."))
    if not signals: signals.append(Signal("success", "💰 Budget steady", "No pressure signals. Keep logging."))
    return sorted(signals, key=lambda s: {"danger": 0, "warning": 1, "success": 2}[s.level])


def compute_paycheck_allocation(paycheck_amount: float, settings: dict[str, str],
                                 debt_df: pd.DataFrame, food_surplus: float = 0.0) -> dict[str, Any]:
    ppd = int(get_setting_float(settings, "pay_period_days") or 14)
    due_day = int(get_setting_float(settings, "due_day") or 27)
    today = date.today()
    due_date = date(today.year, today.month, min(due_day, 28))
    if today.day > due_day:
        nm = today.replace(day=28) + timedelta(days=4)
        due_date = date(nm.year, nm.month, min(due_day, 28))
    days_to_due = (due_date - today).days
    payment_due_this_period = days_to_due <= ppd
    fr = max(get_setting_float(settings, "daily_food_budget") * ppd, 0.0)
    raf = max(paycheck_amount - fr, 0.0)
    td = float(debt_df["display_balance"].sum()) if not debt_df.empty else 0.0
    dr = min(td, raf) if payment_due_this_period and td > 0 else min(td * 0.5, raf)
    rad = max(raf - dr, 0.0)
    surplus_savings = max(food_surplus, 0.0)
    bd: list[dict[str, Any]] = []
    if td > 0 and dr > 0 and not debt_df.empty:
        tmp = debt_df.copy(); tmp["share"] = tmp["display_balance"] / td
        for _, r in tmp.iterrows():
            bd.append({"account_id": int(r["id"]), "account": r["name"],
                       "debt_balance": float(r["display_balance"]), "allocation": float(dr * r["share"])})
    savings_r = rad * get_setting_float(settings, "savings_pct")
    spending_r = rad * get_setting_float(settings, "spending_pct")
    crypto_r = rad * get_setting_float(settings, "crypto_pct")
    taxable_r = rad * get_setting_float(settings, "taxable_investing_pct")
    roth_r = rad * get_setting_float(settings, "roth_ira_pct")
    dca_targets = []
    if crypto_r > 0: dca_targets.append({"platform": "Coinbase", "amount": crypto_r, "note": "Buy crypto — execute manually on Coinbase"})
    if taxable_r > 0: dca_targets.append({"platform": "Fidelity (Taxable)", "amount": taxable_r, "note": "Buy index funds — execute manually on Fidelity"})
    if roth_r > 0: dca_targets.append({"platform": "Fidelity (Roth IRA)", "amount": roth_r, "note": "Buy into Roth — execute manually on Fidelity"})
    return {
        "paycheck_amount": paycheck_amount, "run_date": today.strftime(DATE_FMT),
        "debt_total": td, "food_reserved": fr, "debt_reserved": dr,
        "remaining_after_food": raf, "remaining_after_debt": rad,
        "savings_reserved": savings_r, "surplus_savings": surplus_savings,
        "spending_reserved": spending_r, "crypto_reserved": crypto_r,
        "taxable_reserved": taxable_r, "roth_reserved": roth_r,
        "debt_breakdown": bd, "dca_targets": dca_targets,
        "payment_due_this_period": payment_due_this_period, "days_to_due": days_to_due,
        "meta": {"pay_period_days": ppd, "payment_due_this_period": payment_due_this_period,
                 "debt_allocation_mode": "full" if payment_due_this_period else "maintenance"},
    }


# ── Price fetching ────────────────────────────────────────────────────────────

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
    stock_syms = tuple(sorted(set(holdings_df.loc[holdings_df["asset_type"].isin(["stock", "etf"]), "symbol"].astype(str))))
    cr = holdings_df.loc[holdings_df["asset_type"].eq("crypto") & holdings_df["coingecko_id"].fillna("").ne("")]
    coin_ids = tuple(sorted(set(cr["coingecko_id"].astype(str))))
    sp = fetch_yfinance_prices(stock_syms); cp = {}
    if coin_ids:
        try: cp = fetch_coingecko_prices(coin_ids)
        except Exception: pass
    aod = date.today().strftime(DATE_FMT); refreshed = 0
    for _, row in holdings_df.iterrows():
        sym = str(row["symbol"]); at = str(row["asset_type"])
        if at in {"stock", "etf"}: pi=sp.get(sym,{}); price=pi.get("price"); prev=pi.get("previous_close"); src="yfinance"
        elif at == "crypto": cid=str(row.get("coingecko_id") or ""); pi=cp.get(cid,{}); price=pi.get("price"); prev=pi.get("previous_close"); src="coingecko"
        else: price=row["avg_price"] or 1.0; prev=row["avg_price"] or 1.0; src="internal"
        if price is None: continue
        upsert_price(sym, at, float(price), float(prev) if prev else None, src, aod); refreshed += 1
    set_settings({"last_price_refresh_at": now.isoformat(timespec="seconds")})
    return True, f"Refreshed {refreshed} holding price(s)."


def prepare_report_frames(transactions_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if transactions_df.empty: return {"spending": pd.DataFrame(), "food": pd.DataFrame()}
    tx = transactions_df.copy(); tx["date"] = pd.to_datetime(tx["date"])
    return {"spending": tx.loc[tx["type"] == "Expense"].copy(), "food": tx.loc[tx["category"] == "Food"].copy()}


# ── Google Calendar ───────────────────────────────────────────────────────────

def get_google_calendar_events() -> list[dict]:
    if not _GOOGLE_AVAILABLE: return []
    try:
        client_id = st.secrets.get("GOOGLE_CLIENT_ID", "")
        client_secret = st.secrets.get("GOOGLE_CLIENT_SECRET", "")
        refresh_token = st.secrets.get("GOOGLE_REFRESH_TOKEN", "")
        if not all([client_id, client_secret, refresh_token]): return []
        creds = Credentials(token=None, refresh_token=refresh_token,
                            token_uri="https://oauth2.googleapis.com/token",
                            client_id=client_id, client_secret=client_secret,
                            scopes=["https://www.googleapis.com/auth/calendar.readonly"])
        service = google_build("calendar", "v3", credentials=creds, cache_discovery=False)
        now_iso = datetime.utcnow().isoformat() + "Z"
        week_end = (datetime.utcnow() + timedelta(days=7)).isoformat() + "Z"
        result = service.events().list(calendarId="primary", timeMin=now_iso, timeMax=week_end,
                                        maxResults=20, singleEvents=True, orderBy="startTime").execute()
        return result.get("items", [])
    except Exception:
        return []


# ── Capital One reconciliation ────────────────────────────────────────────────

def parse_capital_one_csv(uploaded_file) -> pd.DataFrame:
    try:
        df = pd.read_csv(uploaded_file)
        df.columns = [c.strip() for c in df.columns]
        col_map = {}
        for c in df.columns:
            cl = c.lower().strip()
            if "transaction date" in cl or cl == "date": col_map[c] = "date"
            elif "description" in cl: col_map[c] = "description"
            elif "debit" in cl: col_map[c] = "debit"
            elif "credit" in cl: col_map[c] = "credit"
        df = df.rename(columns=col_map)
        if "date" not in df.columns: return pd.DataFrame()
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        df = df.dropna(subset=["date"])
        if "debit" in df.columns and "credit" in df.columns:
            df["debit"] = pd.to_numeric(df["debit"], errors="coerce").fillna(0.0)
            df["credit"] = pd.to_numeric(df["credit"], errors="coerce").fillna(0.0)
            df["amount"] = df["debit"].where(df["debit"] > 0, df["credit"])
            df["direction"] = df.apply(lambda r: "debit" if r["debit"] > 0 else "credit", axis=1)
        elif "amount" in df.columns:
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0).abs()
            df["direction"] = "debit"
        df["description"] = df.get("description", pd.Series([""] * len(df))).astype(str).str.strip()
        return df[["date", "description", "amount", "direction"]].copy()
    except Exception:
        return pd.DataFrame()


def reconcile_transactions(cap_one_df, logged_df, account_name) -> pd.DataFrame:
    if cap_one_df.empty: return pd.DataFrame()
    account_logged = logged_df[logged_df["account"] == account_name].copy() if not logged_df.empty else pd.DataFrame()
    results = []
    for _, cap_row in cap_one_df.iterrows():
        cap_date = cap_row["date"]; cap_amount = float(cap_row["amount"]); cap_desc = str(cap_row["description"])
        match = None
        if not account_logged.empty:
            candidates = account_logged[
                (abs(pd.to_datetime(account_logged["date"]).dt.date.apply(lambda d: (d - cap_date).days)) <= 2) &
                (abs(account_logged["amount"] - cap_amount) < 0.02)]
            if not candidates.empty: match = candidates.iloc[0]
        if match is not None:
            results.append({"Status": "✅ Matched", "Statement Date": str(cap_date), "Statement Desc": cap_desc[:40],
                            "Statement Amount": format_currency(cap_amount), "Logged Date": str(match["date"]),
                            "Logged Desc": str(match["description"])[:40], "Logged Amount": format_currency(float(match["amount"])), "Delta": "$0.00"})
        else:
            results.append({"Status": "❌ Missing", "Statement Date": str(cap_date), "Statement Desc": cap_desc[:40],
                            "Statement Amount": format_currency(cap_amount), "Logged Date": "—",
                            "Logged Desc": "Not logged", "Logged Amount": "—", "Delta": format_currency(cap_amount)})
    return pd.DataFrame(results)


# ── Advisor ───────────────────────────────────────────────────────────────────

def build_advisor_context(transactions_df, balances_df, holdings_df, price_cache_df, settings, food_metrics) -> str:
    """Lightweight briefing — tools fetch live detail on demand."""
    today = date.today().strftime("%B %d, %Y")
    nw = build_net_worth(balances_df)
    debt = build_debt_summary(balances_df)
    runway = build_runway(transactions_df, balances_df, food_metrics)
    enriched_h = build_enriched_holdings(holdings_df, price_cache_df) if not holdings_df.empty else pd.DataFrame()
    portfolio_total = format_currency(float(enriched_h["current_value"].sum())) if not enriched_h.empty else "$0.00"
    signals = []
    if debt["total_debt"] > runway["liquid_cash"] * 0.75 and debt["total_debt"] > 0:
        signals.append("debt pressure high")
    if runway["runway_days"] < 14:
        signals.append("runway under 2 weeks")
    elif runway["runway_days"] < 30:
        signals.append("runway under 1 month")
    if food_metrics["remaining_today"] < 0:
        signals.append("food over daily cap")
    if not signals:
        signals.append("no pressure signals")
    return f"""Today is {today}.
Net Worth: {format_currency(nw['net_worth'])} | Debt: {format_currency(debt['total_debt'])} | Runway: {runway['runway_days']:.1f} days
Portfolio: {portfolio_total} | Food surplus: {format_currency(food_metrics['current_carry_surplus'])}
Top signals: {', '.join(signals)}
Use your tools to fetch live account balances, transactions, holdings, and spending detail."""


# ── Advisor tool functions ────────────────────────────────────────────────────

def advisor_get_account_balances() -> list[dict]:
    accounts_df = load_accounts()
    transactions_df = load_transactions()
    holdings_df = load_holdings()
    price_cache_df = load_price_cache()
    balances = build_account_balances(accounts_df, transactions_df, holdings_df, price_cache_df)
    return [
        {"name": str(r["name"]), "balance": round(float(r["display_balance"]), 2),
         "type": str(r["account_type"]), "is_debt": bool(int(r["is_debt"]))}
        for _, r in balances.iterrows()
    ]


def advisor_get_recent_transactions(limit: int = 20, category: str = None, account: str = None) -> list[dict]:
    df = load_transactions()
    if df.empty:
        return []
    if category:
        df = df[df["category"].str.lower() == category.lower()]
    if account:
        df = df[df["account"].str.lower() == account.lower()]
    df = df.head(limit)
    return [
        {"date": str(r["date"]), "description": str(r["description"]), "category": str(r["category"]),
         "amount": round(float(r["amount"]), 2), "account": str(r["account"]), "type": str(r["type"])}
        for _, r in df.iterrows()
    ]


def advisor_get_spending_by_category(days: int = 30) -> list[dict]:
    df = load_transactions()
    if df.empty:
        return []
    cutoff = date.today() - timedelta(days=days)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[(df["type"] == "Expense") & (df["date"] >= cutoff)]
    if df.empty:
        return []
    grouped = df.groupby("category", as_index=False)["amount"].sum()
    grouped = grouped.sort_values("amount", ascending=False)
    return [{"category": str(r["category"]), "total": round(float(r["amount"]), 2)} for _, r in grouped.iterrows()]


def advisor_get_food_metrics() -> dict:
    transactions_df = load_transactions()
    settings = get_settings()
    return build_food_metrics(transactions_df, settings)


def advisor_get_portfolio_summary() -> list[dict]:
    holdings_df = load_holdings()
    price_cache_df = load_price_cache()
    if holdings_df.empty:
        return []
    enriched = build_enriched_holdings(holdings_df, price_cache_df)
    return [
        {"name": str(r["display_name"]), "account": str(r["account"]),
         "current_value": round(float(r["current_value"]), 2),
         "amount_invested": round(float(r["amount_invested"]), 2),
         "total_pnl": round(float(r["total_pnl"]), 2),
         "tdy_pnl": round(float(r["tdy_pnl"]), 2)}
        for _, r in enriched.iterrows()
    ]


def advisor_get_net_worth_history(limit: int = 30) -> list[dict]:
    reports = load_daily_reports(limit=limit)
    result = []
    for r in reversed(reports):
        result.append({
            "date": r.get("report_date", ""),
            "net_worth": round(float(r.get("net_worth", 0)), 2),
            "debt": round(float(r.get("debt", 0)), 2),
            "portfolio_value": round(float(r.get("portfolio_value", 0)), 2),
        })
    return result


def advisor_get_paycheck_allocation(amount: float) -> dict:
    accounts_df = load_accounts()
    transactions_df = load_transactions()
    holdings_df = load_holdings()
    price_cache_df = load_price_cache()
    settings = get_settings()
    balances = build_account_balances(accounts_df, transactions_df, holdings_df, price_cache_df)
    food = build_food_metrics(transactions_df, settings)
    debt_df = build_debt_summary(balances)["by_account"]
    alloc = compute_paycheck_allocation(float(amount), settings, debt_df, food_surplus=food["current_carry_surplus"])
    return {k: v for k, v in alloc.items() if k not in ("debt_breakdown", "dca_targets", "meta")}


def advisor_log_transaction(date_str: str, description: str, category: str, amount: float,
                             account_name: str, tx_type: str) -> dict:
    try:
        accounts_df = load_accounts()
        account_map = {str(r["name"]).lower(): int(r["id"]) for _, r in accounts_df.iterrows()}
        account_id = account_map.get(account_name.lower())
        if account_id is None:
            return {"success": False, "message": f"Account '{account_name}' not found."}
        tx_date = datetime.strptime(date_str, DATE_FMT).date()
        if category not in COMMON_CATEGORIES:
            category = "Other"
        if tx_type not in ("Expense", "Income", "Transfer"):
            tx_type = "Expense"
        add_transaction(tx_date, description, category, float(amount), account_id, tx_type, None, "")
        return {"success": True, "message": f"Logged {tx_type} of ${amount:.2f} — {description}."}
    except Exception as e:
        return {"success": False, "message": str(e)}


# ── Advisor tools schema ──────────────────────────────────────────────────────

def _build_advisor_tools():
    if not _GENAI_AVAILABLE:
        return None
    try:
        from google.generativeai.types import Tool, FunctionDeclaration
        declarations = [
            FunctionDeclaration(
                name="advisor_get_account_balances",
                description="Get current balances for all accounts including cash, credit cards, and investments.",
                parameters={"type": "object", "properties": {}, "required": []},
            ),
            FunctionDeclaration(
                name="advisor_get_recent_transactions",
                description="Get recent transactions, optionally filtered by category or account name.",
                parameters={"type": "object", "properties": {
                    "limit": {"type": "integer", "description": "Max number of transactions to return (default 20)"},
                    "category": {"type": "string", "description": "Filter by category name (e.g. Food, Bills)"},
                    "account": {"type": "string", "description": "Filter by account name (e.g. Checking, Savor)"},
                }, "required": []},
            ),
            FunctionDeclaration(
                name="advisor_get_spending_by_category",
                description="Get total spending grouped by category for the last N days.",
                parameters={"type": "object", "properties": {
                    "days": {"type": "integer", "description": "Number of days to look back (default 30)"},
                }, "required": []},
            ),
            FunctionDeclaration(
                name="advisor_get_food_metrics",
                description="Get current food budget metrics including daily spend, weekly spend, and carry surplus.",
                parameters={"type": "object", "properties": {}, "required": []},
            ),
            FunctionDeclaration(
                name="advisor_get_portfolio_summary",
                description="Get current investment portfolio with current value, PnL, and today's PnL per holding.",
                parameters={"type": "object", "properties": {}, "required": []},
            ),
            FunctionDeclaration(
                name="advisor_get_net_worth_history",
                description="Get historical net worth, debt, and portfolio value from daily reports.",
                parameters={"type": "object", "properties": {
                    "limit": {"type": "integer", "description": "Number of days to return (default 30)"},
                }, "required": []},
            ),
            FunctionDeclaration(
                name="advisor_get_paycheck_allocation",
                description="Compute paycheck allocation breakdown for a given paycheck amount.",
                parameters={"type": "object", "properties": {
                    "amount": {"type": "number", "description": "Paycheck amount in dollars"},
                }, "required": ["amount"]},
            ),
            FunctionDeclaration(
                name="advisor_log_transaction",
                description="Log a new transaction to the database on behalf of the user.",
                parameters={"type": "object", "properties": {
                    "date_str": {"type": "string", "description": "Date in YYYY-MM-DD format"},
                    "description": {"type": "string", "description": "Transaction description"},
                    "category": {"type": "string", "description": "Category (Food, Bills, Income, etc.)"},
                    "amount": {"type": "number", "description": "Amount in dollars (positive)"},
                    "account_name": {"type": "string", "description": "Account name (e.g. Checking, Savor)"},
                    "tx_type": {"type": "string", "description": "Type: Expense, Income, or Transfer"},
                }, "required": ["date_str", "description", "category", "amount", "account_name", "tx_type"]},
            ),
        ]
        return Tool(function_declarations=declarations)
    except Exception:
        return None


_ADVISOR_TOOL_DISPATCH = {
    "advisor_get_account_balances": lambda args: advisor_get_account_balances(),
    "advisor_get_recent_transactions": lambda args: advisor_get_recent_transactions(**args),
    "advisor_get_spending_by_category": lambda args: advisor_get_spending_by_category(**args),
    "advisor_get_food_metrics": lambda args: advisor_get_food_metrics(),
    "advisor_get_portfolio_summary": lambda args: advisor_get_portfolio_summary(),
    "advisor_get_net_worth_history": lambda args: advisor_get_net_worth_history(**args),
    "advisor_get_paycheck_allocation": lambda args: advisor_get_paycheck_allocation(**args),
    "advisor_log_transaction": lambda args: advisor_log_transaction(**args),
}


def ask_advisor(question: str, context: str, conversation_history: list) -> tuple[str, list[str]]:
    """Returns (response_text, tools_used_list)."""
    if not _GENAI_AVAILABLE:
        return "google-generativeai not installed. Add it to requirements.txt.", []
    api_key = st.secrets.get("GOOGLE_API_KEY", "") or os.environ.get("GOOGLE_API_KEY", "")
    if not api_key:
        return "GOOGLE_API_KEY not found in secrets.", []

    genai.configure(api_key=api_key)

    context_file = ""
    try:
        for ctx_candidate in [
            Path("context.md"),
            Path(__file__).parent / "context.md",
            Path("/mount/src") / "context.md",
        ]:
            if ctx_candidate.exists():
                context_file = ctx_candidate.read_text(encoding="utf-8")
                break
    except Exception:
        pass

    memory_str = load_advisor_memory(limit=50)

    system_prompt = f"""You are the Black Book Advisor built specifically for Ignacio Chavarria — 18, Christian, Miami/Uruguayan, finance major at Florida State University, currently traveling. His parents run an architecture firm. He is building toward $1M by 27, owns a business by 30, family mid-30s. He is aggressive with risk at his current investment amounts. He has two active projects: Black Book (this system) and Olympus (an autonomous trading system, currently paused until summer). He does not have a job — his income comes from paychecks likely from family support or part-time work. His financial situation is that of a college student, not someone with full income. He knows this. Do not treat his numbers as alarming — treat them as exactly where an 18-year-old with his awareness and discipline should be.

PERMANENT CONTEXT FILE:
{context_file if context_file else "context.md not loaded — use the facts above."}

MEMORY FROM PAST SESSIONS:
{memory_str}

LIVE BRIEFING:
{context}

You have tools available to fetch live financial data from the database — account balances, transactions, spending by category, food metrics, portfolio, net worth history, paycheck allocation, and transaction logging. Use them whenever the question requires specific numbers rather than guessing from the briefing. Call as many as needed.

Respond the way you'd talk to someone you know well. No structure for structure's sake. No bullet points. No filler opener. Start with the actual point. If the answer is one sentence, one sentence. If it needs more room, use it. Reference his real numbers when they matter. Say what the data actually shows, including what he probably doesn't want to hear. Never use backtick formatting, code blocks, or markdown syntax. Write in plain prose only."""

    history = []
    for msg in conversation_history[-10:]:
        role = "user" if msg["role"] == "user" else "model"
        history.append({"role": role, "parts": [msg["content"]]})

    tools_used: list[str] = []
    advisor_tool = _build_advisor_tools()

    try:
        model = genai.GenerativeModel(
            model_name="gemini-1.5-flash",
            system_instruction=system_prompt,
            tools=[advisor_tool] if advisor_tool else [],
        )
        chat = model.start_chat(history=history)
        response = chat.send_message(question)

        # Tool-calling loop — max 5 iterations
        for _ in range(5):
            fn_calls = [p for p in response.parts if hasattr(p, "function_call") and p.function_call.name]
            if not fn_calls:
                break
            tool_results = []
            for part in fn_calls:
                fn_name = part.function_call.name
                fn_args = dict(part.function_call.args) if part.function_call.args else {}
                tools_used.append(fn_name)
                try:
                    result = _ADVISOR_TOOL_DISPATCH[fn_name](fn_args) if fn_name in _ADVISOR_TOOL_DISPATCH else {"error": f"Unknown tool: {fn_name}"}
                except Exception as exc:
                    result = {"error": str(exc)}
                tool_results.append(
                    genai.protos.Part(
                        function_response=genai.protos.FunctionResponse(
                            name=fn_name,
                            response={"result": json.dumps(result, default=str)},
                        )
                    )
                )
            response = chat.send_message(tool_results)

        text = response.text if hasattr(response, "text") else ""
        return strip_thinking(text), tools_used
    except Exception as e:
        return f"Error: {str(e)}", tools_used


def extract_and_save_memory(conversation_history: list) -> str:
    if not _GENAI_AVAILABLE or len(conversation_history) < 2:
        return "Nothing to save."
    api_key = st.secrets.get("GOOGLE_API_KEY", "") or os.environ.get("GOOGLE_API_KEY", "")
    if not api_key:
        return "No API key."
    try:
        genai.configure(api_key=api_key)
        convo_text = "\n".join(f"{m['role'].upper()}: {m['content']}" for m in conversation_history[-10:])
        prompt = f"""Extract the key points from this conversation worth remembering long term.
Focus on: new goals mentioned, decisions made, patterns identified, anything said about life or systems not already in journals or financial data.
Be extremely concise. Write in third person. Plain paragraphs, not bullet points.
If nothing significant was discussed, respond with exactly: NOTHING_TO_SAVE

CONVERSATION:
{convo_text}"""
        model = genai.GenerativeModel(model_name="gemini-1.5-flash")
        response = model.generate_content(prompt)
        summary = strip_thinking(response.text.strip())
        if summary and summary != "NOTHING_TO_SAVE":
            save_advisor_memory_to_db(summary)
            return "Memory saved."
        return "Nothing significant to save."
    except Exception as e:
        return f"Error: {str(e)}"


# ── Render helpers ────────────────────────────────────────────────────────────

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


# ── Page renderers ────────────────────────────────────────────────────────────

def render_dashboard(settings, transactions_df, holdings_df, balances_df, price_cache_df) -> None:
    food = build_food_metrics(transactions_df, settings)
    runway = build_runway(transactions_df, balances_df, food)
    debt = build_debt_summary(balances_df)
    net_worth = build_net_worth(balances_df)
    signals = build_signals(balances_df, debt, food, runway, settings)
    latest_allocation = load_allocation_snapshots(limit=1)

    st.markdown('<div class="bb-title">Black Book</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="bb-subtitle">Personal Finance · {date.today().strftime("%B %d, %Y")}</div>', unsafe_allow_html=True)
    render_signal(signals[0])

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
            st.plotly_chart(_pie_chart(sc["category"].tolist(), sc["amount"].tolist()), use_container_width=True)
        if not reports["food"].empty:
            st.subheader("Food Trend")
            daily_food = reports["food"].groupby(reports["food"]["date"].dt.date, as_index=False)["amount"].sum()
            st.plotly_chart(_bar_chart(daily_food["date"].tolist(), daily_food["amount"].tolist(),
                             color=C_GREEN, hline=get_setting_float(settings, "daily_food_budget")), use_container_width=True)
        st.subheader("Last Paycheck")
        if latest_allocation.empty:
            preview = compute_paycheck_allocation(580.0, settings, debt["by_account"], food_surplus=food["current_carry_surplus"])
            st.caption("Preview — no snapshot saved yet.")
        else:
            preview = latest_allocation.iloc[0].to_dict()
        alloc_df = pd.DataFrame([
            ("Food", preview["food_reserved"]), ("Debt", preview["debt_reserved"]),
            ("Savings", preview["savings_reserved"]), ("Surplus Savings", preview.get("surplus_savings", 0.0)),
            ("Spending", preview["spending_reserved"]), ("Crypto", preview["crypto_reserved"]),
            ("Taxable", preview.get("taxable_reserved", 0.0)), ("Roth IRA", preview.get("roth_reserved", 0.0)),
        ], columns=["Bucket", "Amount"])
        st.dataframe(alloc_df.assign(Amount=alloc_df["Amount"].map(format_currency)), use_container_width=True, hide_index=True)


def render_log_transaction(accounts_df: pd.DataFrame, transactions_df: pd.DataFrame) -> None:
    st.markdown('<div class="bb-title">Log Transaction</div>', unsafe_allow_html=True)
    st.markdown('<div class="bb-subtitle">Record a money move</div>', unsafe_allow_html=True)
    tab1, tab2 = st.tabs(["Add Transaction", "Delete Transaction"])
    sorted_accts = accounts_df.sort_values("sort_order")
    names = [str(x) for x in sorted_accts["name"].tolist()]
    ids = [int(x) for x in sorted_accts["id"].tolist()]
    account_name_to_id = dict(zip(names, ids))
    with tab1:
        with st.form("transaction_form", clear_on_submit=True):
            c1, c2, c3 = st.columns([1, 2, 1])
            tx_date = c1.date_input("Date", value=date.today())
            description = c2.text_input("Description", placeholder="Chick-fil-A, paycheck, rent...")
            amount = c3.number_input("Amount", min_value=0.0, step=0.01, format="%.2f")
            c4, c5, c6 = st.columns(3)
            category = c4.selectbox("Category", COMMON_CATEGORIES, index=0)
            account = c5.selectbox("From Account", list(account_name_to_id))
            tx_type = c6.selectbox("Type", ["Expense", "Income", "Transfer"])
            to_account = None
            notes = st.text_area("Notes", placeholder="Optional note...")
            if tx_type in ("Transfer", "Expense"):
                to_account = st.selectbox(
                    "To Account" if tx_type == "Transfer" else "To Account (optional)",
                    [""] + [n for n in account_name_to_id if n != account])
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
    with tab2:
        if transactions_df.empty:
            st.info("No transactions to delete yet.")
        else:
            recent = transactions_df.head(50).copy()
            recent["label"] = (recent["date"].astype(str) + " · " + recent["description"].str[:30] + " · " +
                                recent["amount"].map(format_currency) + " · " + recent["account"])
            label_to_id = dict(zip(recent["label"].tolist(), [int(x) for x in recent["id"].tolist()]))
            selected = st.selectbox("Select transaction", [""] + recent["label"].tolist())
            if selected and selected in label_to_id:
                tx_id = label_to_id[selected]
                row = recent[recent["id"] == tx_id].iloc[0]
                st.markdown(f"""
                <div style="background:#0d1117;border:1px solid {C_RED}33;border-radius:2px;padding:0.8rem 1rem;margin:0.5rem 0">
                <div style="font-family:JetBrains Mono,monospace;font-size:0.7rem;color:#6b7280">SELECTED</div>
                <div style="font-family:JetBrains Mono,monospace;font-size:0.85rem;color:#e2e8f0;margin-top:0.3rem">
                {row['date']} · {row['description']} · {format_currency(float(row['amount']))} · {row['account']}
                </div></div>""", unsafe_allow_html=True)
                if st.checkbox("I confirm I want to permanently delete this transaction"):
                    if st.button("Delete Transaction", type="primary"):
                        delete_transaction(tx_id); st.success("Deleted."); st.rerun()


def render_paycheck_allocation(settings, balances_df, food_metrics) -> None:
    st.markdown('<div class="bb-title">Paycheck Allocation</div>', unsafe_allow_html=True)
    st.markdown('<div class="bb-subtitle">Break down your next deposit</div>', unsafe_allow_html=True)
    debt_df = build_debt_summary(balances_df)["by_account"]
    c1, c2 = st.columns([0.9, 1.1])
    with c1:
        paycheck_amount = st.number_input("Paycheck Amount", min_value=0.0, value=580.0, step=10.0, format="%.2f")
        food_surplus = food_metrics.get("current_carry_surplus", 0.0)
        allocation = compute_paycheck_allocation(float(paycheck_amount), settings, debt_df, food_surplus=food_surplus)
        if allocation["payment_due_this_period"]:
            st.markdown(f'<div style="border-left:2px solid {C_RED};padding:0.4rem 0.8rem;background:rgba(255,77,77,0.05);margin-bottom:0.5rem;font-family:JetBrains Mono,monospace;font-size:0.65rem;color:{C_RED}">PAYMENT DUE IN {allocation["days_to_due"]} DAYS — FULL DEBT RESERVED</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div style="border-left:2px solid {C_GOLD};padding:0.4rem 0.8rem;background:rgba(240,165,0,0.05);margin-bottom:0.5rem;font-family:JetBrains Mono,monospace;font-size:0.65rem;color:{C_GOLD}">PAYMENT DUE IN {allocation["days_to_due"]} DAYS — MAINTENANCE RESERVE</div>', unsafe_allow_html=True)
        st.metric("Debt Total", format_currency(allocation["debt_total"]))
        st.metric("Food Reserve", format_currency(allocation["food_reserved"]))
        st.metric("Food Surplus → Savings", format_currency(food_surplus))
        st.metric("After Food", format_currency(allocation["remaining_after_food"]))
        st.metric("After Debt", format_currency(allocation["remaining_after_debt"]))
        if st.button("Save Snapshot", type="primary"):
            save_allocation_snapshot(allocation); st.success("Saved."); st.rerun()
    with c2:
        alloc_rows = [("Food", allocation["food_reserved"]), ("Debt", allocation["debt_reserved"]),
                      ("Savings", allocation["savings_reserved"]), ("Surplus Savings ✦", allocation["surplus_savings"]),
                      ("Spending", allocation["spending_reserved"]), ("Crypto", allocation["crypto_reserved"]),
                      ("Taxable", allocation["taxable_reserved"]), ("Roth IRA", allocation["roth_reserved"])]
        alloc_df = pd.DataFrame(alloc_rows, columns=["Bucket", "Amount"])
        alloc_df["Share"] = alloc_df["Amount"].apply(lambda x: safe_div(x, allocation["paycheck_amount"]))
        st.dataframe(alloc_df.assign(Amount=alloc_df["Amount"].map(format_currency), Share=alloc_df["Share"].map(format_percent)),
                     use_container_width=True, hide_index=True)
        if allocation["debt_breakdown"]:
            st.subheader("Debt Split")
            dbd = pd.DataFrame(allocation["debt_breakdown"])
            st.dataframe(dbd.assign(debt_balance=dbd["debt_balance"].map(format_currency), allocation=dbd["allocation"].map(format_currency))[["account","debt_balance","allocation"]],
                         use_container_width=True, hide_index=True)
        if allocation["dca_targets"]:
            st.subheader("DCA Checklist — Execute Manually")
            for target in allocation["dca_targets"]:
                st.markdown(f'<div style="display:flex;justify-content:space-between;align-items:center;background:#0d1117;border:1px solid rgba(255,255,255,0.05);border-radius:2px;padding:0.6rem 0.8rem;margin-bottom:0.4rem"><div><div style="font-family:JetBrains Mono,monospace;font-size:0.72rem;color:#e2e8f0">{target["platform"]}</div><div style="font-family:JetBrains Mono,monospace;font-size:0.6rem;color:#374151">{target["note"]}</div></div><div style="font-family:JetBrains Mono,monospace;font-size:1rem;color:{C_GREEN}">{format_currency(target["amount"])}</div></div>', unsafe_allow_html=True)
    history = load_allocation_snapshots(limit=8)
    st.subheader("Recent Snapshots")
    if history.empty:
        st.info("Save a paycheck allocation to build your history.")
    else:
        display = history[["run_date","paycheck_amount","food_reserved","debt_reserved","savings_reserved","surplus_savings","spending_reserved","crypto_reserved","taxable_reserved","roth_reserved"]].copy()
        for col in display.columns:
            if col != "run_date": display[col] = display[col].map(format_currency)
        st.dataframe(display, use_container_width=True, hide_index=True)


def render_investments(holdings_df, price_cache_df, accounts_df) -> None:
    st.markdown('<div class="bb-title">Investments</div>', unsafe_allow_html=True)
    st.markdown('<div class="bb-subtitle">Portfolio overview</div>', unsafe_allow_html=True)
    tab1, tab2 = st.tabs(["Portfolio", "Manage Holdings"])
    with tab1:
        if holdings_df.empty:
            st.info("No holdings yet. Add them in the Manage Holdings tab.")
        else:
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
            m1.metric("Portfolio Value", format_currency(tv)); m2.metric("Cost Basis", format_currency(ti))
            m3.metric("Total PnL", format_currency(tp), format_percent(safe_div(tp, ti))); m4.metric("Today PnL", format_currency(tdp))
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
            ac1, ac2 = st.columns(2)
            with ac1:
                ba = enriched.groupby("account", as_index=False)["current_value"].sum()
                st.plotly_chart(_pie_chart(ba["account"].tolist(), ba["current_value"].tolist(), "By Account"), use_container_width=True)
            with ac2:
                bv = enriched.groupby("asset_type", as_index=False)["current_value"].sum()
                st.plotly_chart(_pie_chart(bv["asset_type"].tolist(), bv["current_value"].tolist(), "By Asset Type"), use_container_width=True)
            history = load_price_history()
            if not history.empty:
                vh = history.merge(holdings_df[["symbol","asset_type","quantity"]], on=["symbol","asset_type"], how="left")
                vh["quantity"]=vh["quantity"].fillna(0.0); vh["portfolio_value"]=vh["price"]*vh["quantity"]
                merged = vh.groupby("as_of_date", as_index=False)["portfolio_value"].sum()
                st.plotly_chart(_line_chart(merged["as_of_date"].tolist(), merged["portfolio_value"].tolist(), title="Portfolio Value"), use_container_width=True)
            st.download_button("Export Holdings CSV", data=enriched.to_csv(index=False).encode("utf-8"), file_name="holdings.csv", mime="text/csv")
    with tab2:
        st.subheader("Add New Holding")
        inv_accounts = accounts_df[accounts_df["account_type"] == "investment"]
        if inv_accounts.empty:
            st.warning("No investment accounts found.")
        else:
            inv_map = dict(zip([str(x) for x in inv_accounts["name"].tolist()], [int(x) for x in inv_accounts["id"].tolist()]))
            with st.form("add_holding_form", clear_on_submit=True):
                hc1, hc2 = st.columns(2)
                h_symbol = hc1.text_input("Symbol / Name", placeholder="NVDA, BTC, SPY...")
                h_name = hc2.text_input("Display Name", placeholder="NVIDIA (NVDA), Bitcoin (BTC)...")
                hc3, hc4 = st.columns(2)
                h_asset_type = hc3.selectbox("Asset Type", ["stock", "etf", "crypto", "cash"])
                h_account = hc4.selectbox("Account", list(inv_map))
                hc5, hc6, hc7 = st.columns(3)
                h_amount = hc5.number_input("Amount Invested ($)", min_value=0.0, step=0.01, format="%.2f")
                h_qty = hc6.number_input("Quantity", min_value=0.0, step=0.000001, format="%.6f")
                h_avg = hc7.number_input("Avg Buy Price ($)", min_value=0.0, step=0.01, format="%.2f")
                h_cg = st.text_input("CoinGecko ID (crypto only)", placeholder="bitcoin, ripple, solana...")
                if st.form_submit_button("Add Holding", type="primary"):
                    if not h_symbol.strip(): st.error("Symbol is required.")
                    elif not h_name.strip(): st.error("Display name is required.")
                    else:
                        add_holding(h_symbol, h_name, h_asset_type, int(inv_map[h_account]), float(h_amount), float(h_qty), float(h_avg), h_cg)
                        st.success(f"Added {h_name}."); st.rerun()
        if not holdings_df.empty:
            st.subheader("Edit / Delete Holdings")
            holding_labels = [f"{r['display_name']} · {r['account']}" for _, r in holdings_df.iterrows()]
            label_to_hid = dict(zip(holding_labels, [int(r["id"]) for _, r in holdings_df.iterrows()]))
            selected_h = st.selectbox("Select holding", [""] + holding_labels, key="edit_holding_select")
            if selected_h and selected_h in label_to_hid:
                hid = label_to_hid[selected_h]
                hrow = holdings_df[holdings_df["id"] == hid].iloc[0]
                with st.form("edit_holding_form"):
                    ec1, ec2, ec3 = st.columns(3)
                    new_amount = ec1.number_input("Amount Invested ($)", value=float(hrow["amount_invested"]), step=0.01, format="%.2f")
                    new_qty = ec2.number_input("Quantity", value=float(hrow["quantity"]), step=0.000001, format="%.6f")
                    new_avg = ec3.number_input("Avg Buy Price ($)", value=float(hrow["avg_price"]), step=0.01, format="%.2f")
                    if st.form_submit_button("Update Holding", type="primary"):
                        update_holding(hid, float(new_amount), float(new_qty), float(new_avg)); st.success("Updated."); st.rerun()
                if st.checkbox(f"Confirm delete {hrow['display_name']}"):
                    if st.button("Delete Holding", type="primary"):
                        delete_holding(hid); st.success("Deleted."); st.rerun()


def render_reports(settings, transactions_df, holdings_df, price_cache_df, balances_df) -> None:
    st.markdown('<div class="bb-title">Reports</div>', unsafe_allow_html=True)
    st.markdown('<div class="bb-subtitle">Daily snapshots & analysis</div>', unsafe_allow_html=True)
    tab1, tab2 = st.tabs(["Daily Snapshots", "Spending Analysis"])
    with tab1:
        daily = load_daily_reports(limit=30)
        food = build_food_metrics(transactions_df, settings)
        nw = build_net_worth(balances_df); debt = build_debt_summary(balances_df)
        runway = build_runway(transactions_df, balances_df, food)
        enriched_h = build_enriched_holdings(holdings_df, price_cache_df) if not holdings_df.empty else pd.DataFrame()
        sorted_bal = balances_df.sort_values("sort_order")
        acct_rows = "".join(f'<div class="bb-report-row"><span>{str(r["name"])}</span><span class="bb-report-val">{format_currency(float(r["display_balance"]))}</span></div>' for _, r in sorted_bal.iterrows())
        st.markdown(f"""
        <div class="bb-report-card" style="border-color:{C_GREEN}33">
        <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:0.6rem">
            <div class="bb-report-date" style="color:{C_GREEN}">{date.today().strftime("%B %d, %Y")}</div>
            <div style="font-family:JetBrains Mono,monospace;font-size:0.6rem;color:{C_GREEN};letter-spacing:0.15em;text-transform:uppercase">Live · In Progress</div>
        </div>
        <div class="bb-report-row"><span>Net Worth</span><span class="bb-report-val">{format_currency(nw['net_worth'])}</span></div>
        <div class="bb-report-row"><span>Total Debt</span><span class="bb-report-val">{format_currency(debt['total_debt'])}</span></div>
        <div class="bb-report-row"><span>Liquid Cash</span><span class="bb-report-val">{format_currency(nw['assets'])}</span></div>
        <div class="bb-report-row"><span>Runway</span><span class="bb-report-val">{runway['runway_days']:.1f} days</span></div>
        <div class="bb-report-row"><span>Food Spent Today</span><span class="bb-report-val">{format_currency(food['food_spent_today'])}</span></div>
        <div class="bb-report-row"><span>Food Surplus</span><span class="bb-report-val">{format_currency(food['current_carry_surplus'])}</span></div>
        <div class="bb-report-row"><span>Portfolio Value</span><span class="bb-report-val">{format_currency(float(enriched_h['current_value'].sum()) if not enriched_h.empty else 0)}</span></div>
        {acct_rows}</div>""", unsafe_allow_html=True)
        if not daily:
            st.caption("Historical reports will appear here each morning starting tomorrow.")
        else:
            st.markdown("---")
            for snap in daily:
                report_date_str = snap.get("report_date", "")
                try: rd = datetime.strptime(report_date_str, DATE_FMT).strftime("%B %d, %Y")
                except Exception: rd = report_date_str
                acct_html = "".join(f'<div class="bb-report-row"><span>{k}</span><span class="bb-report-val">{format_currency(v)}</span></div>' for k, v in snap.get("accounts", {}).items())
                st.markdown(f"""
                <div class="bb-report-card">
                <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:0.6rem">
                    <div class="bb-report-date">{rd}</div>
                    <div style="font-family:JetBrains Mono,monospace;font-size:0.6rem;color:#374151;letter-spacing:0.15em">LOCKED</div>
                </div>
                <div class="bb-report-row"><span>Net Worth</span><span class="bb-report-val">{format_currency(snap.get('net_worth',0))}</span></div>
                <div class="bb-report-row"><span>Total Debt</span><span class="bb-report-val">{format_currency(snap.get('debt',0))}</span></div>
                <div class="bb-report-row"><span>Runway</span><span class="bb-report-val">{snap.get('runway_days',0):.1f} days</span></div>
                <div class="bb-report-row"><span>Food Spent</span><span class="bb-report-val">{format_currency(snap.get('food_spent',0))}</span></div>
                <div class="bb-report-row"><span>Food Surplus</span><span class="bb-report-val">{format_currency(snap.get('food_surplus',0))}</span></div>
                <div class="bb-report-row"><span>Portfolio Value</span><span class="bb-report-val">{format_currency(snap.get('portfolio_value',0))}</span></div>
                <div class="bb-report-row"><span>Portfolio PnL</span><span class="bb-report-val">{format_currency(snap.get('portfolio_pnl',0))}</span></div>
                {acct_html}</div>""", unsafe_allow_html=True)
                with st.expander(f"Edit or delete — {rd}"):
                    edit_col, del_col = st.columns([3, 1])
                    with edit_col:
                        new_nw  = st.number_input("Net Worth",       value=float(snap.get("net_worth",0)),       step=0.01, key=f"e_nw_{report_date_str}")
                        new_debt = st.number_input("Total Debt",     value=float(snap.get("debt",0)),            step=0.01, key=f"e_debt_{report_date_str}")
                        new_port = st.number_input("Portfolio Value", value=float(snap.get("portfolio_value",0)),step=0.01, key=f"e_port_{report_date_str}")
                        new_food = st.number_input("Food Spent",     value=float(snap.get("food_spent",0)),      step=0.01, key=f"e_food_{report_date_str}")
                        new_surp = st.number_input("Food Surplus",   value=float(snap.get("food_surplus",0)),    step=0.01, key=f"e_surp_{report_date_str}")
                        if st.button("Save Changes", key=f"save_{report_date_str}", type="primary"):
                            updated = snap.copy()
                            updated.update({"net_worth": new_nw, "debt": new_debt, "portfolio_value": new_port, "food_spent": new_food, "food_surplus": new_surp})
                            updated.pop("report_date", None)
                            update_daily_report(report_date_str, updated); st.success("Updated."); st.rerun()
                    with del_col:
                        if st.checkbox("Confirm delete", key=f"del_confirm_{report_date_str}"):
                            if st.button("Delete", key=f"del_{report_date_str}", type="primary"):
                                delete_daily_report(report_date_str); st.success("Deleted."); st.rerun()
    with tab2:
        if transactions_df.empty:
            st.info("Log transactions to see analysis."); return
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
                st.plotly_chart(_pie_chart(bc["category"].tolist(), bc["amount"].tolist(), "Spending by Category"), use_container_width=True)
        with ch2:
            if not food_df.empty:
                ft = food_df.groupby(food_df["date"].dt.date, as_index=False)["amount"].sum()
                st.plotly_chart(_bar_chart(ft["date"].tolist(), ft["amount"].tolist(), color=C_GREEN, title="Food Trend", hline=get_setting_float(settings, "daily_food_budget")), use_container_width=True)
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
                    fig.add_scatter(x=fw["week"].tolist(), y=[wb]*len(fw), name="Budget", mode="lines", line=dict(color=C_GOLD, dash="dot", width=1))
                    _chart_theme(fig, "Food vs Weekly Budget"); st.plotly_chart(fig, use_container_width=True)
        with ch4:
            if not holdings_df.empty:
                enriched = build_enriched_holdings(holdings_df, price_cache_df)
                ba = enriched.groupby("asset_type", as_index=False)["current_value"].sum()
                st.plotly_chart(_bar_chart(ba["asset_type"].tolist(), ba["current_value"].tolist(), color=C_BLUE, title="Portfolio by Asset Type"), use_container_width=True)
        csv = filtered.copy(); csv["date"] = csv["date"].dt.strftime("%Y-%m-%d")
        st.download_button("Export CSV", data=csv.to_csv(index=False).encode("utf-8"), file_name="transactions.csv", mime="text/csv")


def render_reconcile(transactions_df, accounts_df) -> None:
    st.markdown('<div class="bb-title">Reconcile</div>', unsafe_allow_html=True)
    st.markdown('<div class="bb-subtitle">Capital One statement verification</div>', unsafe_allow_html=True)
    st.markdown("Upload a Capital One CSV. Black Book compares it against your log. **Nothing changes automatically.**")
    cap_one_accounts = ["Checking", "Savor", "Venture"]
    existing_accounts = [str(x) for x in accounts_df["name"].tolist()]
    available = [a for a in cap_one_accounts if a in existing_accounts]
    if not available:
        st.warning("No Capital One accounts found."); return
    c1, c2 = st.columns([1, 2])
    selected_account = c1.selectbox("Account to reconcile", available)
    uploaded = c2.file_uploader("Upload Capital One CSV", type=["csv"])
    if uploaded is not None:
        cap_df = parse_capital_one_csv(uploaded)
        if cap_df.empty:
            st.error("Could not parse. Make sure it's a Capital One export."); return
        st.caption(f"Parsed {len(cap_df)} transactions.")
        min_d = cap_df["date"].min(); max_d = cap_df["date"].max()
        fc1, fc2 = st.columns(2)
        from_date = fc1.date_input("From", value=min_d); to_date = fc2.date_input("To", value=max_d)
        cap_df = cap_df[(cap_df["date"] >= from_date) & (cap_df["date"] <= to_date)]
        results = reconcile_transactions(cap_df, transactions_df, selected_account)
        if results.empty:
            st.info("No results."); return
        matched = len(results[results["Status"].str.startswith("✅")])
        missing = len(results[results["Status"].str.startswith("❌")])
        m1, m2, m3 = st.columns(3)
        m1.metric("Total in Statement", len(results)); m2.metric("Matched", matched); m3.metric("Missing", missing)
        if missing > 0: st.warning(f"{missing} transaction(s) not in Black Book. Log them manually.")
        st.dataframe(results, use_container_width=True, hide_index=True)
        st.download_button("Export Reconciliation CSV", data=results.to_csv(index=False).encode("utf-8"),
                           file_name=f"reconcile_{selected_account}_{date.today()}.csv", mime="text/csv")


def render_journal() -> None:
    st.markdown('<div class="bb-title">Journal</div>', unsafe_allow_html=True)
    st.markdown('<div class="bb-subtitle">Private dated log</div>', unsafe_allow_html=True)
    tab1, tab2 = st.tabs(["Write", "Read"])
    with tab1:
        st.markdown("""
        <div style="background:#0d1117;border:1px solid rgba(255,255,255,0.05);border-radius:2px;padding:1rem 1.2rem;margin-bottom:1.2rem">
            <div style="font-family:JetBrains Mono,monospace;font-size:0.6rem;letter-spacing:0.2em;text-transform:uppercase;color:#374151;margin-bottom:0.8rem">Daily Prompts</div>
            <div style="font-family:JetBrains Mono,monospace;font-size:0.72rem;color:#6b7280;line-height:2.2">
                <span style="color:#374151">STATE &nbsp;&nbsp;</span>1. How am I actually feeling today — physically and mentally?<br>
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2. What is the dominant emotion or energy I am carrying right now?<br>
                <span style="color:#374151">MONEY &nbsp;&nbsp;</span>3. Did I make any financial decisions today? What was the reasoning?<br>
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;4. Is there any financial stress or clarity I am feeling right now?<br>
                <span style="color:#374151">MIND &nbsp;&nbsp;&nbsp;</span>5. What is the most significant thought I had today?<br>
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;6. Did I act in alignment with who I want to be today? Where did I fall short?<br>
                <span style="color:#374151">PATTERN</span> 7. What do I want to remember about today?
            </div>
        </div>""", unsafe_allow_html=True)
        with st.form("journal_form", clear_on_submit=True):
            jc1, jc2 = st.columns([1, 1])
            j_date = jc1.date_input("Date", value=date.today())
            j_tag = jc2.selectbox("Tag", JOURNAL_TAGS)
            j_body = st.text_area("Entry", placeholder="1. ...\n2. ...\n3. ...\n4. ...\n5. ...\n6. ...\n7. ...", height=220)
            if st.form_submit_button("Save Entry", type="primary"):
                if not j_body.strip(): st.error("Entry cannot be empty.")
                else:
                    save_journal_entry(j_date, j_tag, j_body); st.success("Entry saved."); st.rerun()
    with tab2:
        tag_filter = st.selectbox("Filter by tag", ["All"] + JOURNAL_TAGS, key="journal_filter")
        entries = load_journal_entries(limit=50, tag_filter=tag_filter)
        if entries.empty:
            st.info("No entries yet.")
        else:
            for _, row in entries.iterrows():
                entry_id = int(row["id"])
                try: display_date = datetime.strptime(str(row["entry_date"]), DATE_FMT).strftime("%B %d, %Y")
                except Exception: display_date = str(row["entry_date"])
                st.markdown(f"""
                <div class="bb-journal-entry">
                <div class="bb-journal-header">
                    <span class="bb-journal-date">{display_date}</span>
                    <span class="bb-journal-tag">{row['tag']}</span>
                </div>
                <div class="bb-journal-body">{str(row['body'])}</div>
                </div>""", unsafe_allow_html=True)
                with st.expander("", expanded=False):
                    if st.checkbox(f"Delete this entry", key=f"jdel_confirm_{entry_id}"):
                        if st.button("Delete", key=f"jdel_btn_{entry_id}", type="primary"):
                            delete_journal_entry(entry_id); st.rerun()


def render_agenda() -> None:
    st.markdown('<div class="bb-title">Agenda</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="bb-subtitle">Week of {date.today().strftime("%B %d, %Y")}</div>', unsafe_allow_html=True)
    has_google = all([st.secrets.get("GOOGLE_CLIENT_ID",""), st.secrets.get("GOOGLE_CLIENT_SECRET",""), st.secrets.get("GOOGLE_REFRESH_TOKEN","")])
    if not has_google or not _GOOGLE_AVAILABLE:
        st.info("Google Calendar not connected."); return
    with st.spinner("Loading..."):
        events = get_google_calendar_events()
    if not events:
        st.info("No events in the next 7 days."); return
    days: dict[str, list[dict]] = defaultdict(list)
    for event in events:
        start = event.get("start", {}); start_str = start.get("dateTime", start.get("date", ""))
        try:
            if "T" in start_str:
                dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                day_key = dt.strftime("%A, %B %d"); time_label = dt.strftime("%I:%M %p")
            else:
                dt = datetime.strptime(start_str, "%Y-%m-%d"); day_key = dt.strftime("%A, %B %d"); time_label = "All day"
        except Exception:
            day_key = start_str[:10]; time_label = ""
        days[day_key].append({"time": time_label, "title": event.get("summary", "No title")})
    for day, evts in days.items():
        st.markdown(f'<div style="margin-bottom:0.2rem;margin-top:1rem;font-family:JetBrains Mono,monospace;font-size:0.65rem;letter-spacing:0.18em;text-transform:uppercase;color:#374151">{day}</div>', unsafe_allow_html=True)
        for evt in evts:
            st.markdown(f'<div style="display:flex;gap:1.5rem;align-items:baseline;background:#0d1117;border:1px solid rgba(255,255,255,0.04);border-radius:2px;padding:0.55rem 0.8rem;margin-bottom:0.3rem"><span style="font-family:JetBrains Mono,monospace;font-size:0.7rem;color:#374151;min-width:70px">{evt["time"]}</span><span style="font-size:0.85rem;color:#e2e8f0">{evt["title"]}</span></div>', unsafe_allow_html=True)


def render_advisor(transactions_df, balances_df, holdings_df, price_cache_df, settings, food_metrics) -> None:
    st.markdown('<div class="bb-title">Advisor</div>', unsafe_allow_html=True)
    st.markdown('<div class="bb-subtitle">Your private financial advisor</div>', unsafe_allow_html=True)
    if not _GENAI_AVAILABLE:
        st.error("Add `google-generativeai` to requirements.txt and redeploy."); return
    if not st.secrets.get("GOOGLE_API_KEY", ""):
        st.error("Add GOOGLE_API_KEY to Streamlit secrets."); return

    # ── Session state init ────────────────────────────────────────────────────
    if "advisor_history" not in st.session_state:
        st.session_state.advisor_history = []
    if "advisor_tools_used" not in st.session_state:
        st.session_state.advisor_tools_used = {}  # message_index -> list[str]

    # Generate session_id on first load
    if "advisor_session_id" not in st.session_state:
        st.session_state.advisor_session_id = (
            date.today().strftime("%Y%m%d") + "-" + uuid.uuid4().hex[:6]
        )
        # Load history from DB if session already has messages
        existing = load_conversation_history(st.session_state.advisor_session_id)
        if existing:
            st.session_state.advisor_history = existing

    # Always rebuild context on each message cycle (live)
    st.session_state.advisor_context = build_advisor_context(
        transactions_df, balances_df, holdings_df, price_cache_df, settings, food_metrics)

    # ── Controls ──────────────────────────────────────────────────────────────
    ctrl1, ctrl2, ctrl3 = st.columns([1, 1, 1])
    with ctrl1:
        if st.button("↻  Refresh Data", use_container_width=True):
            st.session_state.advisor_context = build_advisor_context(
                transactions_df, balances_df, holdings_df, price_cache_df, settings, food_metrics)
            st.success("Data refreshed.")
    with ctrl2:
        if st.button("◈  Save Memory", use_container_width=True):
            with st.spinner("Extracting key points..."):
                msg = extract_and_save_memory(st.session_state.advisor_history)
            st.success(msg)
    with ctrl3:
        if st.button("✕  Clear Chat", use_container_width=True):
            st.session_state.advisor_history = []
            st.session_state.advisor_tools_used = {}
            st.session_state.advisor_session_id = (
                date.today().strftime("%Y%m%d") + "-" + uuid.uuid4().hex[:6]
            )
            st.rerun()
    st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

    # ── Suggested prompts on empty state ──────────────────────────────────────
    if not st.session_state.advisor_history:
        st.markdown('<div style="font-family:JetBrains Mono,monospace;font-size:0.6rem;letter-spacing:0.2em;text-transform:uppercase;color:#374151;margin-bottom:0.6rem">Suggested</div>', unsafe_allow_html=True)
        suggestions = [
            "Give me an honest assessment of my current financial position.",
            "What patterns do you notice in my spending and journal entries?",
            "Based on my numbers, what should my next paycheck allocation look like?",
            "What's my biggest financial risk right now?",
        ]
        cols = st.columns(2)
        for i, s in enumerate(suggestions):
            if cols[i % 2].button(s, key=f"suggestion_{i}", use_container_width=True):
                with st.spinner("Thinking..."):
                    response_text, tools_used = ask_advisor(s, st.session_state.advisor_context, st.session_state.advisor_history)
                user_idx = len(st.session_state.advisor_history)
                st.session_state.advisor_history.append({"role": "user", "content": s})
                st.session_state.advisor_history.append({"role": "assistant", "content": response_text})
                if tools_used:
                    st.session_state.advisor_tools_used[user_idx + 1] = tools_used
                save_conversation_message(st.session_state.advisor_session_id, "user", s)
                save_conversation_message(st.session_state.advisor_session_id, "assistant", response_text)
                st.rerun()

    # ── Conversation display ──────────────────────────────────────────────────
    for idx, msg in enumerate(st.session_state.advisor_history):
        if msg["role"] == "user":
            st.markdown(
                f'<div style="display:flex;justify-content:flex-end;margin-bottom:0.8rem">'
                f'<div style="background:#14141F;border:1px solid rgba(201,168,76,0.1);'
                f'padding:0.7rem 1rem;max-width:72%;font-size:0.85rem;color:#F2EDE4;line-height:1.6">'
                f'{msg["content"].replace(chr(10), "<br>")}</div></div>',
                unsafe_allow_html=True)
        else:
            clean = strip_thinking(msg["content"]).replace("\n", "<br>")
            st.markdown(
                f'<div style="display:flex;justify-content:flex-start;margin-bottom:0.4rem">'
                f'<div class="bb-response" style="background:#0D0D18;border:1px solid rgba(255,255,255,0.04);'
                f'border-left:2px solid #C9A84C;'
                f'padding:0.8rem 1.1rem;max-width:88%;font-size:0.88rem;color:#9A9080;line-height:1.75">'
                f'{clean}</div></div>',
                unsafe_allow_html=True)
            # Tools used indicator
            tools_for_msg = st.session_state.advisor_tools_used.get(idx, [])
            if tools_for_msg:
                unique_tools = list(dict.fromkeys(tools_for_msg))
                tools_label = "  ·  ".join(t.replace("advisor_", "") for t in unique_tools)
                st.markdown(
                    f'<div style="font-family:JetBrains Mono,monospace;font-size:0.58rem;'
                    f'color:#374151;letter-spacing:0.08em;margin-bottom:0.8rem;padding-left:2px">'
                    f'tools: {tools_label}</div>',
                    unsafe_allow_html=True)
            else:
                st.markdown("<div style='margin-bottom:0.8rem'></div>", unsafe_allow_html=True)

    # ── Input ─────────────────────────────────────────────────────────────────
    st.markdown("<div style='margin-top:1.5rem'></div>", unsafe_allow_html=True)
    with st.form("advisor_form", clear_on_submit=True):
        question = st.text_area(
            "", placeholder="Ask anything...",
            label_visibility="collapsed", height=110)
        submitted = st.form_submit_button("Send", type="primary", use_container_width=True)
        if submitted and question.strip():
            with st.spinner("Thinking..."):
                response_text, tools_used = ask_advisor(
                    question.strip(), st.session_state.advisor_context, st.session_state.advisor_history)
            user_idx = len(st.session_state.advisor_history)
            st.session_state.advisor_history.append({"role": "user", "content": question.strip()})
            st.session_state.advisor_history.append({"role": "assistant", "content": response_text})
            if tools_used:
                st.session_state.advisor_tools_used[user_idx + 1] = tools_used
            save_conversation_message(st.session_state.advisor_session_id, "user", question.strip())
            save_conversation_message(st.session_state.advisor_session_id, "assistant", response_text)
            st.rerun()

    # ── Memory viewer ─────────────────────────────────────────────────────────
    mem_df = load_advisor_memory_df(limit=20)
    if not mem_df.empty:
        st.subheader("Memory Log")
        for _, row in mem_df.iterrows():
            mid = int(row["id"])
            try: display_date = datetime.strptime(str(row["memory_date"]), DATE_FMT).strftime("%B %d, %Y")
            except Exception: display_date = str(row["memory_date"])
            st.markdown(f'<div class="bb-memory-entry"><div style="font-family:JetBrains Mono,monospace;font-size:0.6rem;color:#374151;margin-bottom:0.3rem">{display_date}</div><div style="font-size:0.82rem;color:#9ca3af;line-height:1.5">{str(row["body"])}</div></div>', unsafe_allow_html=True)
            if st.button("Delete", key=f"mem_del_{mid}"):
                delete_advisor_memory_entry(mid); st.rerun()

    # ── Past Sessions ─────────────────────────────────────────────────────────
    sessions_df = list_conversation_sessions(limit=20)
    if not sessions_df.empty:
        with st.expander("Past Sessions"):
            for _, srow in sessions_df.iterrows():
                sid = str(srow["session_id"])
                if sid == st.session_state.advisor_session_id:
                    continue  # skip current session
                snippet = str(srow.get("first_message") or "")[:80]
                created = str(srow.get("created_at", ""))[:16]
                col_a, col_b, col_c = st.columns([3, 1, 1])
                col_a.markdown(
                    f'<div style="font-family:JetBrains Mono,monospace;font-size:0.7rem;color:#9A9080">'
                    f'<span style="color:#374151">{created}</span>  {snippet}</div>',
                    unsafe_allow_html=True)
                if col_b.button("Load", key=f"load_sess_{sid}"):
                    hist = load_conversation_history(sid)
                    st.session_state.advisor_history = hist
                    st.session_state.advisor_tools_used = {}
                    st.session_state.advisor_session_id = sid
                    st.rerun()
                if col_c.button("Delete", key=f"del_sess_{sid}"):
                    delete_conversation_session(sid); st.rerun()


def render_settings(settings, accounts_df) -> None:
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
        acct_ids   = [int(x)   for x in accounts_df["id"].tolist()]
        acct_names = [str(x)   for x in accounts_df["name"].tolist()]
        acct_bals  = [float(x) for x in accounts_df["starting_balance"].tolist()]
        acct_sorts = [int(x)   for x in accounts_df["sort_order"].tolist()]
        combined = sorted(zip(acct_sorts, acct_ids, acct_names, acct_bals), key=lambda x: x[0])
        for idx, (_, acct_id, acct_name, acct_bal) in enumerate(combined):
            updated_balances[acct_id] = cols[idx % 2].number_input(acct_name, value=acct_bal, step=10.0, format="%.2f", key=f"bal_{idx}")
        submitted = st.form_submit_button("Save Settings", type="primary")
        if submitted:
            set_settings({"daily_food_budget": daily_food_budget, "pay_period_days": pay_period_days,
                          "statement_day": statement_day, "due_day": due_day, "savings_pct": savings_pct,
                          "spending_pct": spending_pct, "crypto_pct": crypto_pct,
                          "taxable_investing_pct": taxable_pct, "roth_ira_pct": roth_pct})
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
                add_account(new_name.strip(), new_type, new_is_debt, new_runway); st.success(f"'{new_name}' added."); st.rerun()
    st.subheader("Export Data")
    tx_df = load_transactions(); hld_df = load_holdings()
    journal_df = load_journal_entries(limit=10000); daily = load_daily_reports(limit=365)
    alloc = load_allocation_snapshots(limit=100); settings_current = get_settings()
    c1, c2 = st.columns(2)
    c1.download_button("Transactions CSV", data=tx_df.to_csv(index=False).encode("utf-8"), file_name="transactions.csv", mime="text/csv")
    c2.download_button("Holdings CSV", data=hld_df.to_csv(index=False).encode("utf-8"), file_name="holdings.csv", mime="text/csv")
    st.markdown("---")
    st.caption("Full export — use this to feed the AI layer when ready.")
    full_export = {
        "exported_at": datetime.now().isoformat(), "settings": settings_current,
        "accounts": load_accounts().to_dict("records"),
        "transactions": [{k: str(v) if isinstance(v, date) else v for k, v in row.items()} for row in tx_df.to_dict("records")],
        "holdings": hld_df.to_dict("records"),
        "journal_entries": [{"date": str(r["entry_date"]), "tag": str(r["tag"]), "body": str(r["body"])} for _, r in journal_df.iterrows()] if not journal_df.empty else [],
        "daily_reports": daily,
        "allocation_snapshots": alloc.to_dict("records") if not alloc.empty else [],
    }
    st.download_button("⬇ Full Black Book Export (JSON)", data=json.dumps(full_export, indent=2, default=str).encode("utf-8"),
                       file_name=f"blackbook_export_{date.today().strftime('%Y%m%d')}.json", mime="application/json",
                       type="primary", use_container_width=True)
    st.caption(f"Includes {len(tx_df)} transactions · {len(journal_df) if not journal_df.empty else 0} journal entries · {len(daily)} daily reports · {len(hld_df)} holdings")


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
    food_metrics = build_food_metrics(transactions_df, settings)

    maybe_generate_yesterday_report(accounts_df, transactions_df, holdings_df, price_cache_df, settings)

    NAV_ITEMS = ["Dashboard", "Log Transaction", "Paycheck Allocation",
                 "Investments", "Reports", "Reconcile", "Journal", "Agenda", "Advisor", "Settings"]

    with st.sidebar:
        st.markdown('<div class="bb-sidebar-brand">Black Book</div>', unsafe_allow_html=True)
        st.markdown('<div class="bb-sidebar-year">Est. 2025 · Personal OS</div>', unsafe_allow_html=True)
        page = st.radio("", NAV_ITEMS, label_visibility="collapsed")
        st.markdown(
            f'<div class="bb-sidebar-footer">{"PostgreSQL · Cloud" if IS_POSTGRES else f"SQLite · Local"}</div>',
            unsafe_allow_html=True)

    if page == "Dashboard":
        render_dashboard(settings, transactions_df, holdings_df, balances_df, price_cache_df)
    elif page == "Log Transaction":
        render_log_transaction(accounts_df, transactions_df)
    elif page == "Paycheck Allocation":
        render_paycheck_allocation(settings, balances_df, food_metrics)
    elif page == "Investments":
        render_investments(holdings_df, price_cache_df, accounts_df)
    elif page == "Reports":
        render_reports(settings, transactions_df, holdings_df, price_cache_df, balances_df)
    elif page == "Reconcile":
        render_reconcile(transactions_df, accounts_df)
    elif page == "Journal":
        render_journal()
    elif page == "Agenda":
        render_agenda()
    elif page == "Advisor":
        render_advisor(transactions_df, balances_df, holdings_df, price_cache_df, settings, food_metrics)
    elif page == "Settings":
        render_settings(settings, accounts_df)


if __name__ == "__main__":
    main()
