"""
pages/settings.py — App configuration.
"""
from __future__ import annotations

import reflex as rx

from BlackBook.db.queries import DEFAULT_SETTINGS
from BlackBook.state.app_state import AppState


class SettingsState(rx.State):
    settings: dict[str, str] = {}
    loading: bool = False
    success: str = ""
    error: str = ""

    @rx.event
    async def load(self) -> None:
        self.loading = True
        try:
            from BlackBook.db import queries
            self.settings = queries.get_settings()
        except Exception as e:
            self.error = str(e)
        finally:
            self.loading = False

    @rx.event
    async def save(self) -> None:
        self.success = ""
        self.error = ""
        try:
            from BlackBook.db import queries
            queries.set_settings(self.settings)
            self.success = "Settings saved."
        except Exception as e:
            self.error = str(e)

    def set_value(self, key: str, value: str) -> None:
        self.settings = {**self.settings, key: value}


FIELD_LABELS = {
    "daily_food_budget": "Daily Food Budget ($)",
    "pay_period_days": "Pay Period (days)",
    "statement_day": "Credit Card Statement Day",
    "due_day": "Credit Card Due Day",
    "savings_pct": "Savings % (0.30 = 30%)",
    "spending_pct": "Spending % (0.40 = 40%)",
    "crypto_pct": "Crypto % (0.10 = 10%)",
    "taxable_investing_pct": "Taxable Investing %",
    "roth_ira_pct": "Roth IRA %",
    "next_payday": "Next Payday (YYYY-MM-DD)",
}


def settings_field(key: str, label: str) -> rx.Component:
    return rx.el.div(
        rx.el.label(label, class_name="bb-label"),
        rx.el.input(
            value=SettingsState.settings.get(key, ""),
            on_change=lambda v: SettingsState.set_value(key, v),
            class_name="bb-input",
        ),
        class_name="bb-field",
    )


def settings_page() -> rx.Component:
    return rx.fragment(
        rx.el.div(
            rx.el.h1("Settings", class_name="bb-title"),
            rx.el.p("CONFIGURE YOUR FINANCIAL PARAMETERS", class_name="bb-subtitle"),
        ),

        rx.cond(
            SettingsState.error != "",
            rx.el.div(SettingsState.error, class_name="bb-error"),
        ),
        rx.cond(
            SettingsState.success != "",
            rx.el.div(SettingsState.success, class_name="bb-success"),
        ),

        rx.el.div(
            rx.el.div("Budget Parameters", class_name="bb-section", style={"margin_top": "0"}),
            rx.el.div(
                *[settings_field(k, v) for k, v in FIELD_LABELS.items()],
                style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(240px, 1fr))", "gap": "0 1.5rem"},
            ),
            rx.el.button(
                "Save Settings",
                class_name="bb-btn bb-btn-primary",
                on_click=SettingsState.save,
                style={"margin_top": "0.8rem"},
            ),
            class_name="bb-card",
        ),
        on_mount=SettingsState.load,
    )
