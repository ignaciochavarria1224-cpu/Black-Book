"""
pages/transactions.py — Log and view transactions.
"""
from __future__ import annotations

import reflex as rx

from BlackBook.db.queries import COMMON_CATEGORIES
from BlackBook.state.transaction_state import TransactionState, TxDisplay


def tx_row(tx: TxDisplay) -> rx.Component:
    return rx.el.tr(
        rx.el.td(tx.date),
        rx.el.td(tx.description),
        rx.el.td(tx.category),
        rx.el.td(tx.account),
        rx.el.td(tx.type),
        rx.el.td(rx.el.span(tx.amount_display, class_name=tx.amount_css)),
        rx.el.td(
            rx.el.button(
                "✕",
                class_name="bb-btn bb-btn-danger",
                on_click=TransactionState.delete_transaction(tx.id),
            )
        ),
    )


def option(val: str) -> rx.Component:
    return rx.el.option(val, value=val)


def transactions_page() -> rx.Component:
    return rx.fragment(
        rx.el.div(
            rx.el.h1("Log Transaction", class_name="bb-title"),
            rx.el.p("TRACK EVERY MOVE", class_name="bb-subtitle"),
        ),

        rx.cond(TransactionState.error != "", rx.el.div(TransactionState.error, class_name="bb-error")),
        rx.cond(TransactionState.success != "", rx.el.div(TransactionState.success, class_name="bb-success")),

        # Form
        rx.el.div(
            rx.el.div("New Transaction", class_name="bb-section", style={"margin_top": "0"}),
            rx.el.div(
                rx.el.div(
                    rx.el.label("Date", class_name="bb-label"),
                    rx.el.input(type="date", value=TransactionState.form_date, on_change=TransactionState.set_form_date, class_name="bb-input"),
                    class_name="bb-field",
                ),
                rx.el.div(
                    rx.el.label("Description", class_name="bb-label"),
                    rx.el.input(placeholder="e.g. Chipotle", value=TransactionState.form_description, on_change=TransactionState.set_form_description, class_name="bb-input"),
                    class_name="bb-field",
                ),
                rx.el.div(
                    rx.el.label("Amount ($)", class_name="bb-label"),
                    rx.el.input(type="number", placeholder="0.00", value=TransactionState.form_amount, on_change=TransactionState.set_form_amount, class_name="bb-input"),
                    class_name="bb-field",
                ),
                rx.el.div(
                    rx.el.label("Type", class_name="bb-label"),
                    rx.el.select(
                        rx.el.option("Expense", value="expense"),
                        rx.el.option("Income", value="income"),
                        rx.el.option("Transfer", value="transfer"),
                        value=TransactionState.form_type, on_change=TransactionState.set_form_type, class_name="bb-select",
                    ),
                    class_name="bb-field",
                ),
                rx.el.div(
                    rx.el.label("Category", class_name="bb-label"),
                    rx.el.select(
                        *[rx.el.option(c, value=c) for c in COMMON_CATEGORIES],
                        value=TransactionState.form_category, on_change=TransactionState.set_form_category, class_name="bb-select",
                    ),
                    class_name="bb-field",
                ),
                rx.el.div(
                    rx.el.label("Account", class_name="bb-label"),
                    rx.el.select(
                        rx.el.option("Select account", value=""),
                        rx.foreach(TransactionState.account_names, option),
                        value=TransactionState.form_account, on_change=TransactionState.set_form_account, class_name="bb-select",
                    ),
                    class_name="bb-field",
                ),
                rx.el.div(
                    rx.el.label("Notes (optional)", class_name="bb-label"),
                    rx.el.input(placeholder="Optional note", value=TransactionState.form_notes, on_change=TransactionState.set_form_notes, class_name="bb-input"),
                    class_name="bb-field",
                ),
                style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))", "gap": "0 1.5rem"},
            ),
            rx.el.button("Save Transaction", class_name="bb-btn bb-btn-primary", on_click=TransactionState.submit_transaction, style={"margin_top": "0.8rem"}),
            class_name="bb-card",
        ),

        rx.el.div("Transaction Log", class_name="bb-section"),
        rx.el.div(
            rx.el.table(
                rx.el.thead(
                    rx.el.tr(
                        rx.el.th("Date"), rx.el.th("Description"), rx.el.th("Category"),
                        rx.el.th("Account"), rx.el.th("Type"), rx.el.th("Amount"), rx.el.th(""),
                    )
                ),
                rx.el.tbody(rx.foreach(TransactionState.filtered_transactions, tx_row)),
                class_name="bb-table",
            ),
            class_name="bb-table-wrap",
        ),
        on_mount=TransactionState.load,
    )
