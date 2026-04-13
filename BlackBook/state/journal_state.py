"""
state/journal_state.py — Journal entries using rx.Base typed model.
"""
from __future__ import annotations

from datetime import date

import reflex as rx

from BlackBook.db import queries


class JournalEntry(rx.Base):
    id: int = 0
    entry_date: str = ""
    tag: str = ""
    body: str = ""


class JournalState(rx.State):
    entries: list[JournalEntry] = []
    loading: bool = False
    error: str = ""
    success: str = ""

    form_date: str = ""
    form_tag: str = "General"
    form_body: str = ""
    filter_tag: str = "All"

    @rx.event
    async def load(self) -> None:
        self.loading = True
        self.error = ""
        try:
            raw = queries.load_journal_entries(limit=50, tag_filter=self.filter_tag)
            self.entries = [_to_entry(e) for e in raw]
            if not self.form_date:
                self.form_date = date.today().isoformat()
        except Exception as e:
            self.error = str(e)
        finally:
            self.loading = False

    @rx.event
    async def submit_entry(self) -> None:
        self.error = ""
        self.success = ""
        if not self.form_body.strip():
            self.error = "Entry cannot be empty."
            return
        try:
            queries.save_journal_entry(
                entry_date=date.fromisoformat(self.form_date),
                tag=self.form_tag,
                body=self.form_body,
            )
            self.success = "Entry saved."
            self.form_body = ""
            raw = queries.load_journal_entries(limit=50, tag_filter=self.filter_tag)
            self.entries = [_to_entry(e) for e in raw]
        except Exception as e:
            self.error = str(e)

    @rx.event
    async def delete_entry(self, entry_id: int) -> None:
        try:
            queries.delete_journal_entry(entry_id)
            raw = queries.load_journal_entries(limit=50, tag_filter=self.filter_tag)
            self.entries = [_to_entry(e) for e in raw]
        except Exception as e:
            self.error = str(e)

    @rx.event
    async def set_filter(self, tag: str) -> None:
        self.filter_tag = tag
        raw = queries.load_journal_entries(limit=50, tag_filter=tag)
        self.entries = [_to_entry(e) for e in raw]

    def set_form_date(self, v: str) -> None:
        self.form_date = v

    def set_form_tag(self, v: str) -> None:
        self.form_tag = v

    def set_form_body(self, v: str) -> None:
        self.form_body = v


def _to_entry(e: dict) -> JournalEntry:
    return JournalEntry(
        id=int(e.get("id") or 0),
        entry_date=str(e.get("entry_date") or ""),
        tag=str(e.get("tag") or ""),
        body=str(e.get("body") or ""),
    )
