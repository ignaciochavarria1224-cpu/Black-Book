"""
pages/reconcile.py — Account balance reconciliation.
Uses a flat list so per-row inputs work cleanly in Reflex.
"""
from __future__ import annotations

import reflex as rx

from BlackBook.db import queries


class ReconcileState(rx.State):
    # Each item: {id, name, account_type, override_str}
    rows: list[dict] = []
    loading: bool = False
    success: str = ""
    error: str = ""

    @rx.event
    async def load(self) -> None:
        self.loading = True
        try:
            accounts = queries.load_accounts()
            self.rows = [
                {
                    "idx": i,
                    "id": int(a["id"]),
                    "name": str(a["name"]),
                    "account_type": str(a.get("account_type", "")),
                    "override_str": str(a.get("current_balance_override") or ""),
                }
                for i, a in enumerate(accounts)
            ]
        except Exception as e:
            self.error = str(e)
        finally:
            self.loading = False

    @rx.event
    def update_row(self, idx: int, value: str) -> None:
        rows = list(self.rows)
        if 0 <= idx < len(rows):
            rows[idx] = {**rows[idx], "override_str": value}
        self.rows = rows

    @rx.var
    def indexed_rows(self) -> list[dict]:
        return self.rows

    @rx.event
    async def save_overrides(self) -> None:
        self.error = ""
        self.success = ""
        try:
            for row in self.rows:
                val_str = row.get("override_str", "").strip()
                override = float(val_str) if val_str else None
                queries.update_account_balance_override(int(row["id"]), override)
            self.success = "Balances updated."
        except Exception as e:
            self.error = str(e)


def account_row(row: dict) -> rx.Component:
    return rx.el.div(
        rx.el.div(
            rx.el.span(row["name"], style={"color": "var(--t0)", "font_size": "0.78rem"}),
            rx.el.span(row["account_type"], class_name="bb-tag"),
            style={"display": "flex", "align_items": "center", "gap": "0.6rem", "margin_bottom": "0.4rem"},
        ),
        rx.el.div(
            rx.el.label("Balance Override ($) — leave blank for calculated", class_name="bb-label"),
            rx.el.input(
                type="number",
                placeholder="e.g. 1250.00",
                value=row["override_str"],
                on_change=ReconcileState.update_row(row["idx"]),
                class_name="bb-input",
            ),
        ),
        class_name="bb-card",
        style={"margin_bottom": "0.75rem"},
    )


def reconcile_page() -> rx.Component:
    return rx.fragment(
        rx.el.div(
            rx.el.h1("Reconcile", class_name="bb-title"),
            rx.el.p("SET ACTUAL ACCOUNT BALANCES", class_name="bb-subtitle"),
        ),

        rx.cond(ReconcileState.error != "", rx.el.div(ReconcileState.error, class_name="bb-error")),
        rx.cond(ReconcileState.success != "", rx.el.div(ReconcileState.success, class_name="bb-success")),

        rx.cond(
            ReconcileState.loading,
            rx.el.div("Loading...", class_name="bb-section"),
            rx.fragment(
                rx.foreach(ReconcileState.indexed_rows, account_row),
                rx.el.button(
                    "Save All Balances",
                    class_name="bb-btn bb-btn-primary",
                    on_click=ReconcileState.save_overrides,
                    style={"margin_top": "0.5rem"},
                ),
            ),
        ),
        on_mount=ReconcileState.load,
    )
