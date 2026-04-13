"""
pages/journal.py — Journal entries page.
"""
from __future__ import annotations

import reflex as rx

from BlackBook.db.queries import JOURNAL_TAGS
from BlackBook.state.journal_state import JournalState, JournalEntry


def entry_card(entry: JournalEntry) -> rx.Component:
    return rx.el.div(
        rx.el.div(
            rx.el.span(entry.entry_date, class_name="bb-journal-date"),
            rx.el.span(entry.tag, class_name="bb-journal-tag"),
            rx.el.button(
                "✕",
                class_name="bb-btn bb-btn-danger",
                on_click=JournalState.delete_entry(entry.id),
                style={"font_size": "0.5rem"},
            ),
            class_name="bb-journal-header",
        ),
        rx.el.div(entry.body, class_name="bb-journal-body"),
        class_name="bb-journal-entry",
    )


def tag_filter_btn(tag: str) -> rx.Component:
    return rx.el.button(
        tag,
        class_name=rx.cond(
            JournalState.filter_tag == tag,
            "bb-btn bb-btn-primary",
            "bb-btn bb-btn-ghost",
        ),
        on_click=JournalState.set_filter(tag),
        style={"font_size": "0.56rem", "padding": "0.3rem 0.7rem"},
    )


def journal_page() -> rx.Component:
    return rx.fragment(
        rx.el.div(
            rx.el.h1("Journal", class_name="bb-title"),
            rx.el.p("THOUGHTS · REFLECTIONS · DECISIONS", class_name="bb-subtitle"),
        ),

        rx.cond(JournalState.error != "", rx.el.div(JournalState.error, class_name="bb-error")),
        rx.cond(JournalState.success != "", rx.el.div(JournalState.success, class_name="bb-success")),

        # Write entry
        rx.el.div(
            rx.el.div("New Entry", class_name="bb-section", style={"margin_top": "0"}),
            rx.el.div(
                rx.el.div(
                    rx.el.label("Date", class_name="bb-label"),
                    rx.el.input(type="date", value=JournalState.form_date, on_change=JournalState.set_form_date, class_name="bb-input"),
                    class_name="bb-field",
                ),
                rx.el.div(
                    rx.el.label("Tag", class_name="bb-label"),
                    rx.el.select(
                        *[rx.el.option(t, value=t) for t in JOURNAL_TAGS],
                        value=JournalState.form_tag, on_change=JournalState.set_form_tag, class_name="bb-select",
                    ),
                    class_name="bb-field",
                ),
                style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "0 1.5rem"},
            ),
            rx.el.div(
                rx.el.label("Entry", class_name="bb-label"),
                rx.el.textarea(
                    placeholder="Write here...",
                    value=JournalState.form_body, on_change=JournalState.set_form_body,
                    class_name="bb-input", rows="6", style={"resize": "vertical"},
                ),
                class_name="bb-field",
            ),
            rx.el.button("Save Entry", class_name="bb-btn bb-btn-primary", on_click=JournalState.submit_entry),
            class_name="bb-card",
        ),

        # Filters
        rx.el.div("Filter", class_name="bb-section"),
        rx.el.div(
            tag_filter_btn("All"),
            *[tag_filter_btn(t) for t in JOURNAL_TAGS],
            style={"display": "flex", "flex_wrap": "wrap", "gap": "0.5rem", "margin_bottom": "1rem"},
        ),

        rx.el.div("Entries", class_name="bb-section"),
        rx.cond(
            JournalState.loading,
            rx.el.div("Loading...", class_name="bb-section"),
            rx.foreach(JournalState.entries, entry_card),
        ),
        on_mount=JournalState.load,
    )
