"""
BlackBook.py — Main Reflex app entry point.

Single-page app with client-side navigation via AppState.page.
All 10 sections rendered conditionally inside the shell layout.
"""
from __future__ import annotations

import reflex as rx

from BlackBook.state.app_state import AppState
from BlackBook.components.sidebar import sidebar

# Page imports
from BlackBook.pages.dashboard import dashboard_page
from BlackBook.pages.transactions import transactions_page
from BlackBook.pages.investments import investments_page
from BlackBook.pages.allocation import allocation_page
from BlackBook.pages.reports import reports_page
from BlackBook.pages.journal import journal_page
from BlackBook.pages.reconcile import reconcile_page
from BlackBook.pages.agenda import agenda_page
from BlackBook.pages.advisor import advisor_page
from BlackBook.pages.meridian import meridian_page
from BlackBook.pages.settings import settings_page


def page_content() -> rx.Component:
    """Render the active page via conditional display."""
    return rx.el.main(
        # Each page wrapped in a cond — only the active one renders
        rx.cond(AppState.page == "dashboard",    dashboard_page()),
        rx.cond(AppState.page == "transactions", transactions_page()),
        rx.cond(AppState.page == "investments",  investments_page()),
        rx.cond(AppState.page == "allocation",   allocation_page()),
        rx.cond(AppState.page == "reports",      reports_page()),
        rx.cond(AppState.page == "journal",      journal_page()),
        rx.cond(AppState.page == "reconcile",    reconcile_page()),
        rx.cond(AppState.page == "agenda",       agenda_page()),
        rx.cond(AppState.page == "advisor",      advisor_page()),
        rx.cond(AppState.page == "meridian",     meridian_page()),
        rx.cond(AppState.page == "settings",     settings_page()),
        class_name="bb-main",
    )


def index() -> rx.Component:
    return rx.el.div(
        sidebar(),
        page_content(),
        class_name="bb-shell",
    )


app = rx.App(
    stylesheets=["/bb.css"],
    head_components=[
        rx.el.meta(charset="UTF-8"),
        rx.el.meta(name="viewport", content="width=device-width, initial-scale=1.0"),
    ],
)
app.add_page(index, route="/", title="Black Book")
