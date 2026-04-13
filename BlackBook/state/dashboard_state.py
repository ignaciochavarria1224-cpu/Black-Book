"""
state/dashboard_state.py — Dashboard and account metrics.
Uses rx.Base typed models for display data.
"""
from __future__ import annotations

import reflex as rx

from BlackBook.db import queries


class TxSummary(rx.Base):
    date: str = ""
    description: str = ""
    category: str = ""
    account: str = ""
    sign: str = ""
    amount_display: str = ""
    amount_css: str = ""


class DashboardState(rx.State):
    accounts: list[dict] = []
    recent_txns: list[TxSummary] = []
    settings: dict[str, str] = {}
    net_worth: float = 0.0
    total_assets: float = 0.0
    total_debt: float = 0.0
    daily_reports: list[dict] = []
    loading: bool = False
    error: str = ""

    @rx.event
    async def load(self) -> None:
        self.loading = True
        self.error = ""
        try:
            self.accounts = queries.load_accounts()
            raw_txns = queries.load_transactions(limit=10)
            self.settings = queries.get_settings()
            self.daily_reports = queries.load_daily_reports(limit=7)
            self.recent_txns = [
                TxSummary(
                    date=str(t.get("date") or ""),
                    description=str(t.get("description") or ""),
                    category=str(t.get("category") or ""),
                    account=str(t.get("account") or ""),
                    sign="+" if str(t.get("type")) == "income" else "-",
                    amount_display=f"${abs(float(t.get('amount') or 0)):.2f}",
                    amount_css="pos" if str(t.get("type")) == "income" else "neg",
                )
                for t in raw_txns
            ]
            txns = queries.load_transactions(limit=5000)
            self._compute_balances(txns)
        except Exception as e:
            self.error = str(e)
        finally:
            self.loading = False

    def _compute_balances(self, txns: list[dict]) -> None:
        balances: dict[int, float] = {}
        for acct in self.accounts:
            aid = int(acct["id"])
            sb = float(acct.get("starting_balance") or 0)
            override = acct.get("current_balance_override")
            balances[aid] = float(override) if override is not None else sb

        for tx in txns:
            aid = int(tx.get("account_id") or 0)
            taid = tx.get("to_account_id")
            amt = float(tx.get("amount") or 0)
            tx_type = str(tx.get("type") or "")
            if tx_type == "income":
                balances[aid] = balances.get(aid, 0) + amt
            elif tx_type == "expense":
                balances[aid] = balances.get(aid, 0) - amt
            elif tx_type == "transfer" and taid:
                taid = int(taid)
                balances[aid] = balances.get(aid, 0) - amt
                balances[taid] = balances.get(taid, 0) + amt

        assets = 0.0
        debt = 0.0
        for acct in self.accounts:
            aid = int(acct["id"])
            bal = balances.get(aid, 0.0)
            if int(acct.get("is_debt") or 0):
                debt += abs(bal)
            else:
                assets += bal

        self.total_assets = round(assets, 2)
        self.total_debt = round(debt, 2)
        self.net_worth = round(assets - debt, 2)

    @rx.var
    def net_worth_display(self) -> str:
        return f"${self.net_worth:,.2f}"

    @rx.var
    def assets_display(self) -> str:
        return f"${self.total_assets:,.2f}"

    @rx.var
    def debt_display(self) -> str:
        return f"${self.total_debt:,.2f}"
