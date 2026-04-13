"""
pages/agenda.py — Upcoming financial deadlines.
"""
from __future__ import annotations

from datetime import date

import reflex as rx

from BlackBook.db import queries


class AgendaItem(rx.Base):
    label: str = ""
    date_str: str = ""
    days_str: str = ""
    type_label: str = ""
    border_color: str = ""
    days_color: str = ""


class AgendaState(rx.State):
    items: list[AgendaItem] = []
    loading: bool = False
    error: str = ""

    @rx.event
    async def load(self) -> None:
        self.loading = True
        try:
            settings = queries.get_settings()
            self.items = _build_items(settings)
        except Exception as e:
            self.error = str(e)
        finally:
            self.loading = False


def _build_items(s: dict) -> list[AgendaItem]:
    items = []
    today = date.today()

    next_pay_str = s.get("next_payday", "")
    if next_pay_str:
        try:
            next_pay = date.fromisoformat(next_pay_str)
            days = (next_pay - today).days
            items.append(_make_item("Next Payday", next_pay_str, days, "income"))
        except Exception:
            pass

    due_day = int(s.get("due_day", "27"))
    if today.day <= due_day:
        due = today.replace(day=due_day)
    else:
        due = today.replace(month=today.month % 12 + 1, day=due_day) if today.month < 12 else today.replace(year=today.year + 1, month=1, day=due_day)
    items.append(_make_item("CC Payment Due", due.isoformat(), (due - today).days, "expense"))

    stmt_day = int(s.get("statement_day", "2"))
    if today.day <= stmt_day:
        stmt = today.replace(day=stmt_day)
    else:
        stmt = today.replace(month=today.month % 12 + 1, day=stmt_day) if today.month < 12 else today.replace(year=today.year + 1, month=1, day=stmt_day)
    items.append(_make_item("CC Statement Closes", stmt.isoformat(), (stmt - today).days, "neutral"))

    return sorted(items, key=lambda x: int(x.days_str.rstrip("d")))


def _make_item(label: str, date_str: str, days: int, itype: str) -> AgendaItem:
    border = "var(--go)" if itype == "income" else ("var(--re)" if itype == "expense" else "var(--cy)")
    urgency = "var(--re)" if days <= 3 else ("var(--mg)" if days <= 7 else "var(--t1)")
    return AgendaItem(
        label=label,
        date_str=date_str,
        days_str=f"{days}d",
        type_label=itype,
        border_color=border,
        days_color=urgency,
    )


def agenda_item(item: AgendaItem) -> rx.Component:
    return rx.el.div(
        rx.el.div(
            rx.el.span(item.label, style={"color": "var(--t0)", "font_size": "0.82rem"}),
            rx.el.span(
                item.days_str,
                style={"color": item.days_color, "font_family": "'Syne', sans-serif", "font_size": "1.1rem", "font_weight": "700"},
            ),
            style={"display": "flex", "justify_content": "space-between", "align_items": "center"},
        ),
        rx.el.div(item.date_str, style={"color": "var(--t2)", "font_size": "0.6rem", "margin_top": "0.2rem"}),
        class_name="bb-card",
        style={"border_left": "2px solid " + item.border_color, "border_radius": "0 8px 8px 0"},
    )


def agenda_page() -> rx.Component:
    return rx.fragment(
        rx.el.div(
            rx.el.h1("Agenda", class_name="bb-title"),
            rx.el.p("UPCOMING FINANCIAL EVENTS", class_name="bb-subtitle"),
        ),

        rx.cond(AgendaState.error != "", rx.el.div(AgendaState.error, class_name="bb-error")),

        rx.el.div("Upcoming", class_name="bb-section"),
        rx.cond(
            AgendaState.loading,
            rx.el.div("Loading...", class_name="bb-section"),
            rx.cond(
                AgendaState.items.length() == 0,
                rx.el.div("No items.", style={"color": "var(--t2)"}),
                rx.foreach(AgendaState.items, agenda_item),
            ),
        ),
        on_mount=AgendaState.load,
    )
