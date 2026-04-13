"""
state/investment_state.py — Holdings and price data.
Uses rx.Base typed models for class_name compatibility.
"""
from __future__ import annotations

from datetime import date

import reflex as rx

from BlackBook.db import queries


class HoldingDisplay(rx.Base):
    id: int = 0
    symbol: str = ""
    display_name: str = ""
    asset_type: str = ""
    account: str = ""
    quantity: str = ""
    price_display: str = ""
    value_display: str = ""
    pnl_display: str = ""
    pnl_pct_display: str = ""
    pnl_css: str = ""


class InvestmentState(rx.State):
    holdings: list[dict] = []
    prices: dict[str, float] = {}
    accounts: list[dict] = []
    loading: bool = False
    price_loading: bool = False
    error: str = ""
    success: str = ""
    last_refresh: str = ""

    @rx.event
    async def load(self) -> None:
        self.loading = True
        self.error = ""
        try:
            self.holdings = queries.load_holdings()
            self.accounts = queries.load_accounts()
            cache = queries.load_price_cache()
            self.prices = {f"{r['symbol']}_{r['asset_type']}": float(r["price"]) for r in cache}
            self.last_refresh = next((r["fetched_at"][:16] for r in cache), "")
        except Exception as e:
            self.error = str(e)
        finally:
            self.loading = False

    @rx.event(background=True)
    async def refresh_prices(self) -> None:
        async with self:
            self.price_loading = True
            self.error = ""
        try:
            holdings = queries.load_holdings()
            today_str = date.today().isoformat()

            import httpx
            ids = list({h["coingecko_id"] for h in holdings if h.get("coingecko_id")})
            if ids:
                url = f"https://api.coingecko.com/api/v3/simple/price?ids={','.join(ids)}&vs_currencies=usd&include_24hr_change=true"
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.get(url)
                    data = resp.json()
                for h in holdings:
                    cid = h.get("coingecko_id")
                    if cid and cid in data:
                        price = float(data[cid].get("usd", 0))
                        chg = data[cid].get("usd_24h_change", 0)
                        prev = price / (1 + chg / 100) if chg else price
                        queries.upsert_price(h["symbol"], "crypto", price, prev, "coingecko", today_str)

            import yfinance as yf  # type: ignore
            tickers = list({h["symbol"] for h in holdings if h["asset_type"] == "stock"})
            if tickers:
                data = yf.download(tickers, period="2d", auto_adjust=True, progress=False)
                for sym in tickers:
                    try:
                        closes = data["Close"][sym].dropna()
                        if len(closes) >= 2:
                            price = float(closes.iloc[-1])
                            prev = float(closes.iloc[-2])
                        elif len(closes) == 1:
                            price = prev = float(closes.iloc[-1])
                        else:
                            continue
                        queries.upsert_price(sym, "stock", price, prev, "yfinance", today_str)
                    except Exception:
                        pass

            async with self:
                self.holdings = queries.load_holdings()
                cache = queries.load_price_cache()
                self.prices = {f"{r['symbol']}_{r['asset_type']}": float(r["price"]) for r in cache}
                self.last_refresh = date.today().isoformat()
        except Exception as e:
            async with self:
                self.error = str(e)
        finally:
            async with self:
                self.price_loading = False

    @rx.var
    def enriched_holdings(self) -> list[HoldingDisplay]:
        result = []
        for h in self.holdings:
            sym = str(h.get("symbol") or "")
            atype = str(h.get("asset_type") or "")
            price_key = f"{sym}_{atype}"
            price = self.prices.get(price_key, 0.0)
            qty = float(h.get("quantity") or 0)
            invested = float(h.get("amount_invested") or 0)
            current_val = price * qty
            pnl = current_val - invested
            pnl_pct = (pnl / invested * 100) if invested else 0.0
            pnl_css = "pos" if pnl >= 0 else "neg"
            result.append(HoldingDisplay(
                id=int(h.get("id") or 0),
                symbol=sym,
                display_name=str(h.get("display_name") or sym),
                asset_type=atype,
                account=str(h.get("account") or ""),
                quantity=str(qty),
                price_display=f"${price:.4f}",
                value_display=f"${current_val:.2f}",
                pnl_display=f"${pnl:.2f}",
                pnl_pct_display=f"{pnl_pct:.1f}%",
                pnl_css=pnl_css,
            ))
        return result

    @rx.var
    def portfolio_value(self) -> float:
        return round(sum(float(h.value_display.replace("$", "")) for h in self.enriched_holdings), 2)

    @rx.var
    def portfolio_pnl(self) -> float:
        return round(sum(float(h.pnl_display.replace("$", "")) for h in self.enriched_holdings), 2)

    @rx.var
    def portfolio_value_display(self) -> str:
        v = sum(
            float(h.value_display.replace("$", "")) for h in self.enriched_holdings
        )
        return f"${v:,.2f}"

    @rx.var
    def portfolio_pnl_display(self) -> str:
        p = sum(
            float(h.pnl_display.replace("$", "")) for h in self.enriched_holdings
        )
        return f"${p:,.2f}"

    @rx.var
    def portfolio_pnl_css(self) -> str:
        p = sum(
            float(h.pnl_display.replace("$", "")) for h in self.enriched_holdings
        )
        return "pos" if p >= 0 else "neg"
