"""
state/app_state.py — Root application state.
Handles navigation and top-level data seeding.
"""
from __future__ import annotations

import reflex as rx

PAGES = [
    "dashboard",
    "transactions",
    "allocation",
    "investments",
    "reports",
    "journal",
    "reconcile",
    "agenda",
    "advisor",
    "meridian",
    "settings",
]

PAGE_LABELS = {
    "dashboard":    "Dashboard",
    "transactions": "Log Transaction",
    "allocation":   "Paycheck Allocation",
    "investments":  "Investments",
    "reports":      "Reports",
    "journal":      "Journal",
    "reconcile":    "Reconcile",
    "agenda":       "Agenda",
    "advisor":      "Advisor",
    "meridian":     "Meridian",
    "settings":     "Settings",
}


class AppState(rx.State):
    """Root state — tracks active page for client-side nav."""
    page: str = "dashboard"

    def set_page(self, page: str) -> None:
        self.page = page
