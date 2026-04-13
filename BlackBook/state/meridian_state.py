"""
state/meridian_state.py — Meridian wiki belief graph.
"""
from __future__ import annotations

import json
import re

import reflex as rx

from BlackBook.db import queries


class MeridianState(rx.State):
    themes: list[dict] = []
    selected_theme: str = ""
    selected_body: str = ""
    loading: bool = False
    error: str = ""
    search: str = ""

    @rx.event
    async def load(self) -> None:
        self.loading = True
        self.error = ""
        try:
            self.themes = queries.load_meridian_brain()
        except Exception as e:
            self.error = str(e)
        finally:
            self.loading = False

    def select_theme(self, theme: str) -> None:
        self.selected_theme = theme
        match = next((t for t in self.themes if t["theme"] == theme), None)
        self.selected_body = match["body"] if match else ""

    def set_search(self, v: str) -> None:
        self.search = v

    def clear_selection(self) -> None:
        self.selected_theme = ""
        self.selected_body = ""

    @rx.var
    def filtered_themes(self) -> list[dict]:
        if not self.search:
            return self.themes
        q = self.search.lower()
        return [t for t in self.themes if q in t["theme"].lower() or q in (t.get("body") or "").lower()]

    @rx.var
    def theme_count(self) -> int:
        return len(self.themes)

    @rx.var
    def graph_nodes(self) -> list[dict]:
        """Build vis-network node list from themes."""
        nodes = []
        for i, t in enumerate(self.filtered_themes):
            nodes.append({
                "id": t["theme"],
                "label": t["theme"],
                "cycle": t.get("cycle", 0),
            })
        return nodes

    @rx.var
    def graph_nodes_json(self) -> str:
        return json.dumps(self.graph_nodes)

    @rx.var
    def graph_edges_json(self) -> str:
        return json.dumps(self.graph_edges)

    @rx.var
    def graph_edges(self) -> list[dict]:
        """Build vis-network edge list from [[wikilinks]] in theme bodies."""
        theme_set = {t["theme"] for t in self.themes}
        edges = []
        seen = set()
        for t in self.filtered_themes:
            body = t.get("body") or ""
            links = re.findall(r'\[\[([^\]]+)\]\]', body)
            for link in links:
                target = link.strip()
                if target in theme_set and target != t["theme"]:
                    key = tuple(sorted([t["theme"], target]))
                    if key not in seen:
                        seen.add(key)
                        edges.append({"from": t["theme"], "to": target})
        return edges
