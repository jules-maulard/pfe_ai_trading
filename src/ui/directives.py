from __future__ import annotations

import json
import re
from typing import Any

import pandas as pd
import streamlit as st

_CHART_BLOCK = re.compile(r"```chart\n(.*?)```", re.DOTALL)


def parse_directives(text: str) -> list[dict[str, Any]]:
    directives = []
    for raw in _CHART_BLOCK.findall(text):
        try:
            directives.append(json.loads(raw.strip()))
        except (json.JSONDecodeError, ValueError):
            pass
    return directives


def strip_directives(text: str) -> str:
    return _CHART_BLOCK.sub("", text).strip()


def render_directive(directive: dict[str, Any]) -> None:
    chart_type = directive.get("type")
    symbol = directive.get("symbol", "")
    start = directive.get("start")
    end = directive.get("end")
    title = directive.get("title") or symbol

    if chart_type == "ohlcv_chart":
        _render_ohlcv(symbol, start, end, title)
    elif chart_type == "indicator_series":
        _render_indicator(symbol, start, end, directive.get("indicator", ""), title)
    elif chart_type == "multi_series":
        _render_multi_series(directive.get("series", []), title)
    else:
        st.warning(f"Unknown chart directive type: {chart_type!r}")


def _render_ohlcv(symbol: str, start: str | None, end: str | None, title: str) -> None:
    import plotly.graph_objects as go
    from src.ui.helpers import list_symbols, load_ohlcv

    df = load_ohlcv([symbol], start, end)
    if df.empty:
        st.info(
            f"No OHLCV data for **{symbol}** ({start} → {end}). "
            f"Available symbols: {', '.join(list_symbols()[:10])}"
        )
        return

    df = _normalize_df(df)
    fig = go.Figure(
        go.Candlestick(
            x=df["date"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
        )
    )
    fig.update_layout(title=title, height=350, xaxis_rangeslider_visible=False)
    st.plotly_chart(fig, use_container_width=True)


def _render_indicator(
    symbol: str, start: str | None, end: str | None, indicator: str, title: str
) -> None:
    import plotly.graph_objects as go
    from src.ui.helpers import list_symbols, load_indicator

    df = load_indicator(indicator, [symbol], start, end)
    if df.empty:
        st.info(
            f"No {indicator.upper()} data for **{symbol}**. "
            f"Available symbols: {', '.join(list_symbols()[:10])}"
        )
        return

    df = _normalize_df(df)
    col = indicator.lower() if indicator.lower() in df.columns else df.columns[-1]
    fig = go.Figure(go.Scatter(x=df["date"], y=df[col], mode="lines", name=col.upper()))
    fig.update_layout(title=title or f"{indicator.upper()} — {symbol}", height=280)
    st.plotly_chart(fig, use_container_width=True)


def _render_multi_series(series_list: list[dict], title: str) -> None:
    import plotly.graph_objects as go
    from src.ui.helpers import list_symbols, load_indicator

    fig = go.Figure()
    for s in series_list:
        sym = s.get("symbol", "")
        indicator = s.get("indicator", "")
        df = load_indicator(indicator, [sym], s.get("start"), s.get("end"))
        if df.empty:
            continue
        df = _normalize_df(df)
        col = indicator.lower() if indicator.lower() in df.columns else df.columns[-1]
        fig.add_trace(go.Scatter(x=df["date"], y=df[col], mode="lines", name=f"{sym} {col.upper()}"))

    if not fig.data:
        st.info(
            f"No data found for multi-series chart. "
            f"Available symbols: {', '.join(list_symbols()[:10])}"
        )
        return

    fig.update_layout(title=title or "Multi-series chart", height=300)
    st.plotly_chart(fig, use_container_width=True)


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    if "date" not in df.columns and df.index.name in ("date", "Date"):
        df = df.reset_index()
    return df
