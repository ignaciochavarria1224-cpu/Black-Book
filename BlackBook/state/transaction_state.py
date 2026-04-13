"""
state/transaction_state.py — Transaction log and form state.
Uses rx.Base typed models so Reflex knows field types for class_name etc.
"""
from __future__ import annotations

from datetime import date

import reflex as rx

from BlackBook.db import queries


class TxDisplay(rx.Base):
    id: int = 0
    date: str = ""
    description: str = ""
    category: str = ""
    account: str = ""
    type: str = ""
    amount_display: str = ""
    amount_css: str = ""


class TransactionState(rx.State):
    transactions: list[TxDisplay] = []
    accounts: list[dict] = []
    loading: bool = False
    error: str = ""
    success: str = ""

    # Form fields
    form_date: str = ""
    form_description: str = ""
    form_category: str = "Food"
    form_amount: str = ""
    form_account: str = ""
    form_type: str = "expense"
    form_to_account: str = ""
    form_notes: str = ""

    # Filters
    filter_category: str = "All"
    filter_account: str = "All"

    @rx.event
    async def load(self) -> None:
        self.loading = True
        self.error = ""
        try:
            raw = queries.load_transactions(limit=200)
            self.transactions = [_to_tx_display(t) for t in raw]
            self.accounts = queries.load_accounts()
            if not self.form_date:
                self.form_date = date.today().isoformat()
        except Exception as e:
            self.error = str(e)
        finally:
            self.loading = False

    @rx.event
    async def submit_transaction(self) -> None:
        self.error = ""
        self.success = ""
        try:
            amt = float(self.form_amount or "0")
            if amt <= 0:
                self.error = "Amount must be positive."
                return
            acct = next((a for a in self.accounts if a["name"] == self.form_account), None)
            if not acct:
                self.error = "Select an account."
                return
            to_acct = next((a for a in self.accounts if a["name"] == self.form_to_account), None)
            queries.add_transaction(
                tx_date=date.fromisoformat(self.form_date),
                description=self.form_description or "—",
                category=self.form_category,
                amount=amt,
                account_id=int(acct["id"]),
                tx_type=self.form_type,
                to_account_id=int(to_acct["id"]) if to_acct else None,
                notes=self.form_notes,
            )
            self.success = "Transaction saved."
            self.form_description = ""
            self.form_amount = ""
            self.form_notes = ""
            raw = queries.load_transactions(limit=200)
            self.transactions = [_to_tx_display(t) for t in raw]
        except Exception as e:
            self.error = str(e)

    @rx.event
    async def delete_transaction(self, tx_id: int) -> None:
        try:
            queries.delete_transaction(tx_id)
            raw = queries.load_transactions(limit=200)
            self.transactions = [_to_tx_display(t) for t in raw]
        except Exception as e:
            self.error = str(e)

    def set_form_date(self, v: str) -> None:
        self.form_date = v

    def set_form_description(self, v: str) -> None:
        self.form_description = v

    def set_form_category(self, v: str) -> None:
        self.form_category = v

    def set_form_amount(self, v: str) -> None:
        self.form_amount = v

    def set_form_account(self, v: str) -> None:
        self.form_account = v

    def set_form_type(self, v: str) -> None:
        self.form_type = v

    def set_form_to_account(self, v: str) -> None:
        self.form_to_account = v

    def set_form_notes(self, v: str) -> None:
        self.form_notes = v

    def set_filter_category(self, v: str) -> None:
        self.filter_category = v

    def set_filter_account(self, v: str) -> None:
        self.filter_account = v

    @rx.var
    def filtered_transactions(self) -> list[TxDisplay]:
        txns = self.transactions
        if self.filter_category != "All":
            txns = [t for t in txns if t.category == self.filter_category]
        if self.filter_account != "All":
            txns = [t for t in txns if t.account == self.filter_account]
        return txns

    @rx.var
    def account_names(self) -> list[str]:
        return [str(a["name"]) for a in self.accounts]


def _to_tx_display(t: dict) -> TxDisplay:
    amt = float(t.get("amount") or 0)
    tx_type = str(t.get("type") or "")
    return TxDisplay(
        id=int(t.get("id") or 0),
        date=str(t.get("date") or ""),
        description=str(t.get("description") or ""),
        category=str(t.get("category") or ""),
        account=str(t.get("account") or ""),
        type=tx_type,
        amount_display=f"${amt:.2f}",
        amount_css="pos" if tx_type == "income" else "neg",
    )
