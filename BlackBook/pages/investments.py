"""
pages/investments.py — Portfolio holdings and live prices.
"""
from __future__ import annotations

import reflex as rx

from BlackBook.state.investment_state import InvestmentState, HoldingDisplay


def holding_row(h: HoldingDisplay) -> rx.Component:
    return rx.el.tr(
        rx.el.td(h.display_name),
        rx.el.td(h.account),
        rx.el.td(h.asset_type),
        rx.el.td(h.price_display),
        rx.el.td(h.quantity),
        rx.el.td(h.value_display),
        rx.el.td(rx.el.span(h.pnl_display, class_name=h.pnl_css)),
        rx.el.td(rx.el.span(h.pnl_pct_display, class_name=h.pnl_css)),
    )


def investments_page() -> rx.Component:
    return rx.fragment(
        rx.el.div(
            rx.el.h1("Investments", class_name="bb-title"),
            rx.el.p("PORTFOLIO · CRYPTO · STOCKS", class_name="bb-subtitle"),
        ),

        # KPI row
        rx.el.div(
            rx.el.div(
                rx.el.div("Portfolio Value", class_name="bb-stat-label"),
                rx.el.div(InvestmentState.portfolio_value_display, class_name="bb-stat-value"),
                class_name="bb-stat",
            ),
            rx.el.div(
                rx.el.div("Total P&L", class_name="bb-stat-label"),
                rx.el.div(
                    InvestmentState.portfolio_pnl_display,
                    class_name=InvestmentState.portfolio_pnl_css,
                ),
                class_name="bb-stat",
            ),
            rx.el.div(
                rx.el.div("Last Refresh", class_name="bb-stat-label"),
                rx.el.div(
                    rx.cond(InvestmentState.last_refresh != "", InvestmentState.last_refresh, "Never"),
                    class_name="bb-stat-value",
                    style={"font_size": "0.9rem"},
                ),
                class_name="bb-stat",
            ),
            class_name="bb-stat-grid",
        ),

        rx.cond(
            InvestmentState.error != "",
            rx.el.div(InvestmentState.error, class_name="bb-error"),
        ),

        rx.el.button(
            rx.cond(InvestmentState.price_loading, "Fetching Prices...", "Refresh Prices"),
            class_name="bb-btn bb-btn-primary",
            on_click=InvestmentState.refresh_prices,
            disabled=InvestmentState.price_loading,
            style={"margin_bottom": "1.2rem"},
        ),

        rx.el.div("Holdings", class_name="bb-section"),
        rx.el.div(
            rx.el.table(
                rx.el.thead(
                    rx.el.tr(
                        rx.el.th("Name"),
                        rx.el.th("Account"),
                        rx.el.th("Type"),
                        rx.el.th("Price"),
                        rx.el.th("Qty"),
                        rx.el.th("Value"),
                        rx.el.th("P&L"),
                        rx.el.th("P&L %"),
                    )
                ),
                rx.el.tbody(
                    rx.foreach(InvestmentState.enriched_holdings, holding_row),
                ),
                class_name="bb-table",
            ),
            class_name="bb-table-wrap",
        ),
        on_mount=InvestmentState.load,
    )
