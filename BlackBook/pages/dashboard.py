"""
pages/dashboard.py — Dashboard: net worth, recent transactions, daily reports.
"""
from __future__ import annotations

import reflex as rx

from BlackBook.state.dashboard_state import DashboardState, TxSummary


def recent_tx_row(tx: TxSummary) -> rx.Component:
    return rx.el.tr(
        rx.el.td(tx.date),
        rx.el.td(tx.description),
        rx.el.td(tx.category),
        rx.el.td(tx.account),
        rx.el.td(
            rx.el.span(tx.sign, tx.amount_display, class_name=tx.amount_css)
        ),
    )


def dashboard_page() -> rx.Component:
    return rx.fragment(
        rx.el.div(
            rx.el.h1("Dashboard", class_name="bb-title"),
            rx.el.p("PERSONAL FINANCIAL OS", class_name="bb-subtitle"),
        ),
        rx.cond(
            DashboardState.loading,
            rx.el.div("Loading...", class_name="bb-section"),
        ),
        rx.cond(
            DashboardState.error != "",
            rx.el.div(DashboardState.error, class_name="bb-error"),
        ),
        # KPI row
        rx.el.div(
            rx.el.div(
                rx.el.div("Net Worth", class_name="bb-stat-label"),
                rx.el.div(
                    DashboardState.net_worth_display,
                    class_name=rx.cond(
                        DashboardState.net_worth >= 0,
                        "bb-stat-value pos",
                        "bb-stat-value neg",
                    ),
                ),
                class_name="bb-stat",
            ),
            rx.el.div(
                rx.el.div("Total Assets", class_name="bb-stat-label"),
                rx.el.div(DashboardState.assets_display, class_name="bb-stat-value"),
                class_name="bb-stat",
            ),
            rx.el.div(
                rx.el.div("Total Debt", class_name="bb-stat-label"),
                rx.el.div(DashboardState.debt_display, class_name="bb-stat-value neg"),
                class_name="bb-stat",
            ),
            class_name="bb-stat-grid",
        ),
        # Recent transactions
        rx.el.div("Recent Activity", class_name="bb-section"),
        rx.el.div(
            rx.el.table(
                rx.el.thead(
                    rx.el.tr(
                        rx.el.th("Date"),
                        rx.el.th("Description"),
                        rx.el.th("Category"),
                        rx.el.th("Account"),
                        rx.el.th("Amount"),
                    )
                ),
                rx.el.tbody(
                    rx.foreach(DashboardState.recent_txns, recent_tx_row),
                ),
                class_name="bb-table",
            ),
            class_name="bb-table-wrap",
        ),
        on_mount=DashboardState.load,
    )
