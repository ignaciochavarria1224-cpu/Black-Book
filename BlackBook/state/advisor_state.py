"""
state/advisor_state.py — Groq streaming AI advisor.
"""
from __future__ import annotations

import os
import uuid

import reflex as rx

from BlackBook.db import queries


class ChatMessage(rx.Base):
    role: str = ""
    content: str = ""
    css: str = ""


class AdvisorState(rx.State):
    messages: list[ChatMessage] = []
    session_id: str = ""
    is_streaming: bool = False
    input_text: str = ""
    error: str = ""
    memory_list: list[dict] = []
    sessions: list[dict] = []
    groq_available: bool = False

    @rx.event
    async def load(self) -> None:
        self.groq_available = bool(os.environ.get("GROQ_API_KEY"))
        if not self.session_id:
            self.session_id = str(uuid.uuid4())
        raw = queries.load_conversation_history(self.session_id)
        self.messages = [
            ChatMessage(
                role=m["role"],
                content=m["content"],
                css="bb-msg bb-msg-user" if m["role"] == "user" else "bb-msg bb-msg-assistant",
            )
            for m in raw
        ]
        self.memory_list = queries.load_advisor_memory_list()
        self.sessions = queries.list_conversation_sessions()

    @rx.event(background=True)
    async def send_message(self) -> None:
        user_text = self.input_text.strip()
        if not user_text:
            return

        async with self:
            self.input_text = ""
            self.is_streaming = True
            self.error = ""
            self.messages = [
                *self.messages,
                ChatMessage(role="user", content=user_text, css="bb-msg bb-msg-user"),
                ChatMessage(role="assistant", content="", css="bb-msg bb-msg-assistant"),
            ]

        try:
            queries.save_conversation_message(self.session_id, "user", user_text)
            memory = queries.load_advisor_memory()
            txns = queries.load_transactions(limit=50)
            accts = queries.load_accounts()
            settings = queries.get_settings()

            system_prompt = f"""You are Ignacio's personal financial advisor AI inside Black Book — his futuristic personal OS terminal. Be sharp, direct, insightful. No filler.

MEMORY:
{memory}

ACCOUNTS (first 5):
{accts[:5]}

RECENT TRANSACTIONS (last 10):
{txns[:10]}

SETTINGS: savings={settings.get('savings_pct','0.30')}, spending={settings.get('spending_pct','0.40')}, crypto={settings.get('crypto_pct','0.10')}
"""

            history = queries.load_conversation_history(self.session_id)
            messages_payload = [{"role": "system", "content": system_prompt}]
            messages_payload += [{"role": m["role"], "content": m["content"]} for m in history[:-1]]
            messages_payload.append({"role": "user", "content": user_text})

            from groq import Groq  # type: ignore
            client = Groq(api_key=os.environ["GROQ_API_KEY"])

            full_response = ""
            stream = client.chat.completions.create(
                model="llama3-8b-8192",
                messages=messages_payload,
                stream=True,
                max_tokens=1024,
                temperature=0.7,
            )
            for chunk in stream:
                delta = chunk.choices[0].delta.content or ""
                if delta:
                    full_response += delta
                    async with self:
                        msgs = list(self.messages)
                        msgs[-1] = ChatMessage(role="assistant", content=full_response, css="bb-msg bb-msg-assistant")
                        self.messages = msgs

            queries.save_conversation_message(self.session_id, "assistant", full_response)

        except Exception as e:
            async with self:
                msgs = list(self.messages)
                msgs[-1] = ChatMessage(role="assistant", content=f"[Error: {e}]", css="bb-msg bb-msg-assistant")
                self.messages = msgs
                self.error = str(e)
        finally:
            async with self:
                self.is_streaming = False

    def set_input_text(self, v: str) -> None:
        self.input_text = v

    @rx.event
    async def new_session(self) -> None:
        self.session_id = str(uuid.uuid4())
        self.messages = []

    @rx.event
    async def save_memory(self, body: str) -> None:
        if body.strip():
            queries.save_advisor_memory(body.strip())
            self.memory_list = queries.load_advisor_memory_list()

    @rx.event
    async def delete_memory(self, entry_id: int) -> None:
        queries.delete_advisor_memory_entry(entry_id)
        self.memory_list = queries.load_advisor_memory_list()
