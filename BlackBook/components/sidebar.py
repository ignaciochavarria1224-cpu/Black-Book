"""
components/sidebar.py — Navigation sidebar.
"""
from __future__ import annotations

import reflex as rx

from BlackBook.state.app_state import AppState, PAGES, PAGE_LABELS


def nav_item(page: str) -> rx.Component:
    return rx.el.button(
        PAGE_LABELS[page],
        class_name=rx.cond(
            AppState.page == page,
            "bb-nav-item active",
            "bb-nav-item",
        ),
        on_click=AppState.set_page(page),
    )


def sidebar() -> rx.Component:
    return rx.el.aside(
        rx.el.div(
            rx.el.div("Budget Black Book", class_name="bb-sidebar-brand"),
            rx.el.div("Personal OS — 2026", class_name="bb-sidebar-year"),
        ),
        rx.el.nav(
            *[nav_item(p) for p in PAGES],
        ),
        rx.el.div(
            rx.el.span("v2 · Reflex"),
            rx.el.br(),
            rx.el.span("Powered by Claude"),
            class_name="bb-sidebar-footer",
        ),
        class_name="bb-sidebar",
    )
