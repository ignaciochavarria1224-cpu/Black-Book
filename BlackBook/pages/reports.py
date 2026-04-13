"""
pages/reports.py — Daily financial reports.
"""
from __future__ import annotations

import reflex as rx

from BlackBook.db import queries


class ReportDisplay(rx.Base):
    report_date: str = ""
    nw_display: str = ""
    nw_css: str = ""
    portfolio_display: str = ""
    txn_display: str = ""


class ReportsState(rx.State):
    reports: list[ReportDisplay] = []
    loading: bool = False
    error: str = ""

    @rx.event
    async def load(self) -> None:
        self.loading = True
        try:
            raw = queries.load_daily_reports(limit=30)
            self.reports = [_to_report_display(r) for r in raw]
        except Exception as e:
            self.error = str(e)
        finally:
            self.loading = False

    @rx.event
    async def delete_report(self, report_date: str) -> None:
        try:
            queries.delete_daily_report(report_date)
            raw = queries.load_daily_reports(limit=30)
            self.reports = [_to_report_display(r) for r in raw]
        except Exception as e:
            self.error = str(e)


def _to_report_display(r: dict) -> ReportDisplay:
    nw = float(r.get("net_worth", 0))
    return ReportDisplay(
        report_date=str(r.get("report_date", "")),
        nw_display=f"${nw:,.2f}",
        nw_css="pos" if nw >= 0 else "neg",
        portfolio_display=f"${float(r.get('portfolio_value', 0)):,.2f}",
        txn_display=str(r.get("txn_count", 0)),
    )


def report_card(r: ReportDisplay) -> rx.Component:
    return rx.el.div(
        rx.el.div(
            rx.el.span(r.report_date, style={"font_family": "'Syne', sans-serif", "font_size": "1rem", "color": "var(--t0)"}),
            rx.el.button(
                "✕",
                class_name="bb-btn bb-btn-danger",
                on_click=ReportsState.delete_report(r.report_date),
            ),
            style={"display": "flex", "justify_content": "space-between", "align_items": "center", "margin_bottom": "0.6rem"},
        ),
        rx.el.div(
            rx.el.div(
                rx.el.span("Net Worth", style={"color": "var(--t2)", "font_size": "0.58rem"}),
                rx.el.span(r.nw_display, class_name=r.nw_css),
                style={"display": "flex", "justify_content": "space-between", "padding": "0.3rem 0", "border_bottom": "1px solid var(--b1)"},
            ),
            rx.el.div(
                rx.el.span("Portfolio", style={"color": "var(--t2)", "font_size": "0.58rem"}),
                rx.el.span(r.portfolio_display),
                style={"display": "flex", "justify_content": "space-between", "padding": "0.3rem 0", "border_bottom": "1px solid var(--b1)"},
            ),
            rx.el.div(
                rx.el.span("Transactions", style={"color": "var(--t2)", "font_size": "0.58rem"}),
                rx.el.span(r.txn_display),
                style={"display": "flex", "justify_content": "space-between", "padding": "0.3rem 0"},
            ),
            style={"font_family": "'JetBrains Mono', monospace", "font_size": "0.72rem", "color": "var(--t1)"},
        ),
        class_name="bb-card",
    )


def reports_page() -> rx.Component:
    return rx.fragment(
        rx.el.div(
            rx.el.h1("Reports", class_name="bb-title"),
            rx.el.p("DAILY SNAPSHOTS · FINANCIAL HISTORY", class_name="bb-subtitle"),
        ),

        rx.cond(ReportsState.error != "", rx.el.div(ReportsState.error, class_name="bb-error")),
        rx.cond(
            ReportsState.loading,
            rx.el.div("Loading...", class_name="bb-section"),
            rx.cond(
                ReportsState.reports.length() == 0,
                rx.el.div("No reports yet. Reports are auto-generated daily.", style={"color": "var(--t2)", "font_size": "0.8rem"}),
                rx.foreach(ReportsState.reports, report_card),
            ),
        ),
        on_mount=ReportsState.load,
    )
