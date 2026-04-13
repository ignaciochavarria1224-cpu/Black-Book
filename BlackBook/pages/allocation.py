"""
pages/allocation.py — Paycheck allocation calculator.
"""
from __future__ import annotations

import reflex as rx

from BlackBook.db import queries


class AllocationState(rx.State):
    paycheck: str = ""
    settings: dict[str, str] = {}
    accounts: list[dict] = []
    result: dict = {}
    loading: bool = False
    error: str = ""
    success: str = ""

    @rx.event
    async def load(self) -> None:
        self.loading = True
        try:
            self.settings = queries.get_settings()
            self.accounts = queries.load_accounts()
        except Exception as e:
            self.error = str(e)
        finally:
            self.loading = False

    def set_paycheck(self, v: str) -> None:
        self.paycheck = v

    @rx.event
    async def calculate(self) -> None:
        self.error = ""
        self.result = {}
        try:
            amt = float(self.paycheck or "0")
            if amt <= 0:
                self.error = "Enter a paycheck amount."
                return
            s = self.settings
            savings_pct = float(s.get("savings_pct", "0.30"))
            spending_pct = float(s.get("spending_pct", "0.40"))
            crypto_pct = float(s.get("crypto_pct", "0.10"))
            taxable_pct = float(s.get("taxable_investing_pct", "0.10"))
            roth_pct = float(s.get("roth_ira_pct", "0.10"))
            food_daily = float(s.get("daily_food_budget", "30"))
            pay_days = int(s.get("pay_period_days", "14"))

            food = food_daily * pay_days
            remainder = amt - food
            savings = remainder * savings_pct
            spending = remainder * spending_pct
            crypto = remainder * crypto_pct
            taxable = remainder * taxable_pct
            roth = remainder * roth_pct

            self.result = {
                "paycheck": amt,
                "food_reserved": round(food, 2),
                "savings": round(savings, 2),
                "spending": round(spending, 2),
                "crypto": round(crypto, 2),
                "taxable": round(taxable, 2),
                "roth": round(roth, 2),
                "total": round(food + savings + spending + crypto + taxable + roth, 2),
            }
        except Exception as e:
            self.error = str(e)

    @rx.var
    def result_ready(self) -> bool:
        return bool(self.result)

    @rx.var
    def food_display(self) -> str:
        return f"${self.result.get('food_reserved', 0):.2f}"

    @rx.var
    def savings_display(self) -> str:
        return f"${self.result.get('savings', 0):.2f}"

    @rx.var
    def spending_display(self) -> str:
        return f"${self.result.get('spending', 0):.2f}"

    @rx.var
    def crypto_display(self) -> str:
        return f"${self.result.get('crypto', 0):.2f}"

    @rx.var
    def taxable_display(self) -> str:
        return f"${self.result.get('taxable', 0):.2f}"

    @rx.var
    def roth_display(self) -> str:
        return f"${self.result.get('roth', 0):.2f}"

    @rx.event
    async def save_snapshot(self) -> None:
        if not self.result:
            return
        try:
            from datetime import date
            payload = {
                "paycheck_amount": self.result["paycheck"],
                "run_date": date.today().isoformat(),
                "debt_total": 0.0,
                "food_reserved": self.result["food_reserved"],
                "debt_reserved": 0.0,
                "savings_reserved": self.result["savings"],
                "surplus_savings": 0.0,
                "spending_reserved": self.result["spending"],
                "crypto_reserved": self.result["crypto"],
                "taxable_reserved": self.result["taxable"],
                "roth_reserved": self.result["roth"],
                "debt_breakdown": {},
                "meta": {},
            }
            queries.save_allocation_snapshot(payload)
            self.success = "Snapshot saved."
        except Exception as e:
            self.error = str(e)


def alloc_row(label: str, value: rx.Var) -> rx.Component:
    return rx.el.div(
        rx.el.span(label, style={"color": "var(--t2)", "font_size": "0.62rem"}),
        rx.el.span(value, style={"color": "var(--t0)", "font_size": "0.78rem", "font_weight": "600"}),
        style={"display": "flex", "justify_content": "space-between", "padding": "0.4rem 0", "border_bottom": "1px solid var(--b1)"},
    )


def allocation_page() -> rx.Component:
    return rx.fragment(
        rx.el.div(
            rx.el.h1("Paycheck Allocation", class_name="bb-title"),
            rx.el.p("DISTRIBUTE EACH DOLLAR WITH INTENT", class_name="bb-subtitle"),
        ),

        rx.cond(AllocationState.error != "", rx.el.div(AllocationState.error, class_name="bb-error")),
        rx.cond(AllocationState.success != "", rx.el.div(AllocationState.success, class_name="bb-success")),

        rx.el.div(
            rx.el.div("Paycheck Amount", class_name="bb-section", style={"margin_top": "0"}),
            rx.el.div(
                rx.el.input(
                    type="number",
                    placeholder="Enter net paycheck...",
                    value=AllocationState.paycheck,
                    on_change=AllocationState.set_paycheck,
                    class_name="bb-input",
                    style={"max_width": "280px"},
                ),
                rx.el.button(
                    "Calculate",
                    class_name="bb-btn bb-btn-primary",
                    on_click=AllocationState.calculate,
                ),
                style={"display": "flex", "gap": "0.8rem", "align_items": "center"},
            ),
            class_name="bb-card",
        ),

        rx.cond(
            AllocationState.result_ready,
            rx.el.div(
                rx.el.div("Allocation Breakdown", class_name="bb-section", style={"margin_top": "0"}),
                alloc_row("Food (reserved)", AllocationState.food_display),
                alloc_row("Savings", AllocationState.savings_display),
                alloc_row("Spending", AllocationState.spending_display),
                alloc_row("Crypto", AllocationState.crypto_display),
                alloc_row("Taxable Investing", AllocationState.taxable_display),
                alloc_row("Roth IRA", AllocationState.roth_display),
                rx.el.button(
                    "Save Snapshot",
                    class_name="bb-btn bb-btn-ghost",
                    on_click=AllocationState.save_snapshot,
                    style={"margin_top": "0.8rem"},
                ),
                class_name="bb-card",
            ),
        ),
        on_mount=AllocationState.load,
    )
