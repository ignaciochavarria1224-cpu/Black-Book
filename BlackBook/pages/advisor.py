"""
pages/advisor.py — Streaming AI financial advisor.
"""
from __future__ import annotations

import reflex as rx

from BlackBook.state.advisor_state import AdvisorState, ChatMessage


def message_bubble(msg: ChatMessage) -> rx.Component:
    return rx.el.div(msg.content, class_name=msg.css)


def mem_row(m: dict) -> rx.Component:
    return rx.el.div(
        rx.el.span(m.get("memory_date", ""), style={"color": "var(--t2)", "font_size": "0.58rem", "margin_right": "0.8rem", "flex_shrink": "0"}),
        rx.el.span(m.get("body", ""), style={"color": "var(--t1)", "font_size": "0.75rem", "flex": "1"}),
        rx.el.button(
            "✕",
            class_name="bb-btn bb-btn-danger",
            on_click=AdvisorState.delete_memory(m["id"]),
            style={"flex_shrink": "0"},
        ),
        style={"display": "flex", "align_items": "center", "gap": "0.5rem", "padding": "0.5rem 0", "border_bottom": "1px solid var(--b1)"},
    )


def advisor_page() -> rx.Component:
    return rx.fragment(
        rx.el.div(
            rx.el.h1("Advisor", class_name="bb-title"),
            rx.el.p("AI FINANCIAL INTELLIGENCE — POWERED BY GROQ", class_name="bb-subtitle"),
        ),

        rx.cond(
            ~AdvisorState.groq_available,
            rx.el.div("GROQ_API_KEY not set — add it to your .env to activate the advisor.", class_name="bb-error"),
        ),

        # Chat window
        rx.el.div(
            rx.cond(
                AdvisorState.messages.length() == 0,
                rx.el.div(
                    "Start a conversation. Ask about your spending, investments, goals, or anything financial.",
                    style={"color": "var(--t2)", "font_size": "0.8rem", "text_align": "center", "padding": "2rem 0"},
                ),
                rx.foreach(AdvisorState.messages, message_bubble),
            ),
            rx.cond(
                AdvisorState.is_streaming,
                rx.el.span(class_name="bb-cursor"),
            ),
            class_name="bb-chat-wrap",
        ),

        # Input row
        rx.el.div(
            rx.el.textarea(
                placeholder="Ask anything about your finances...",
                value=AdvisorState.input_text,
                on_change=AdvisorState.set_input_text,
                class_name="bb-input",
                rows="2",
                style={"resize": "none", "flex": "1"},
                disabled=AdvisorState.is_streaming,
            ),
            rx.el.button(
                rx.cond(AdvisorState.is_streaming, "...", "Send"),
                class_name="bb-btn bb-btn-primary",
                on_click=AdvisorState.send_message,
                disabled=AdvisorState.is_streaming,
            ),
            rx.el.button(
                "New",
                class_name="bb-btn bb-btn-ghost",
                on_click=AdvisorState.new_session,
                style={"font_size": "0.56rem"},
            ),
            class_name="bb-chat-input-row",
        ),

        rx.cond(
            AdvisorState.error != "",
            rx.el.div(AdvisorState.error, class_name="bb-error", style={"margin_top": "0.5rem"}),
        ),

        # Memory
        rx.el.div("Advisor Memory", class_name="bb-section"),
        rx.el.div(
            rx.cond(
                AdvisorState.memory_list.length() == 0,
                rx.el.div("No memories yet.", style={"color": "var(--t2)", "font_size": "0.75rem"}),
                rx.foreach(AdvisorState.memory_list, mem_row),
            ),
            class_name="bb-card",
        ),
        on_mount=AdvisorState.load,
    )
