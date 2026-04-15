import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from src.ui.helpers import INDICATORS, load_ohlcv, load_indicator, list_symbols


def render():
    st.header("Market Data")

    symbols_available = list_symbols()

    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    with col1:
        symbol = st.selectbox("Symbol", symbols_available if symbols_available else ["No data"])
    with col2:
        start = st.date_input("Start", pd.Timestamp("2026-01-01"))
    with col3:
        end = st.date_input("End", pd.Timestamp("today"))
    with col4:
        selected_indicators = st.multiselect("Indicators", INDICATORS, default=["RSI", "MACD"])

    if not symbols_available:
        st.warning("No symbols found in storage.")
        return

    if st.button("Load", type="primary"):
        st.session_state.market_symbol = symbol
        st.session_state.market_start = str(start)
        st.session_state.market_end = str(end)
        st.session_state.market_indicators = selected_indicators

    sym = st.session_state.get("market_symbol")
    if not sym:
        st.info("Select a symbol and click **Load**.")
        return

    with st.spinner("Loading data…"):
        try:
            ohlcv = load_ohlcv(
                [st.session_state.market_symbol],
                st.session_state.market_start,
                st.session_state.market_end,
            )
        except Exception as e:
            st.error(f"Failed to load OHLCV: {e}")
            return

    if ohlcv.empty:
        st.warning("No OHLCV data for this symbol/range.")
        return

    ohlcv["date"] = pd.to_datetime(ohlcv["date"])
    ohlcv = ohlcv.sort_values("date")

    inds = st.session_state.get("market_indicators", [])
    n_rows = 1 + len(inds)
    row_heights = [0.5] + [0.25 for _ in inds]
    total = sum(row_heights)
    row_heights = [h / total for h in row_heights]

    subplot_titles = [f"{st.session_state.market_symbol} — OHLCV"] + inds
    fig = make_subplots(
        rows=n_rows,
        cols=1,
        shared_xaxes=True,
        row_heights=row_heights,
        subplot_titles=subplot_titles,
        vertical_spacing=0.04,
    )

    fig.add_trace(
        go.Candlestick(
            x=ohlcv["date"],
            open=ohlcv["open"],
            high=ohlcv["high"],
            low=ohlcv["low"],
            close=ohlcv["close"],
            name="OHLCV",
            increasing_line_color="#26a69a",
            decreasing_line_color="#ef5350",
        ),
        row=1, col=1,
    )

    for idx, ind in enumerate(inds, start=2):
        try:
            df_ind = load_indicator(
                ind,
                [st.session_state.market_symbol],
                st.session_state.market_start,
                st.session_state.market_end,
            )
            df_ind["date"] = pd.to_datetime(df_ind["date"])
            df_ind = df_ind.sort_values("date")
        except Exception:
            continue

        if ind == "RSI":
            _add_rsi(fig, df_ind, idx)
        elif ind == "MACD":
            _add_macd(fig, df_ind, idx)
        elif ind == "Pivot":
            _add_pivot(fig, df_ind, idx)

    fig.update_layout(
        height=200 + 200 * n_rows,
        template="plotly_dark",
        showlegend=True,
        xaxis_rangeslider_visible=False,
        margin=dict(l=0, r=0, t=30, b=0),
        hovermode="x unified",
    )
    fig.update_xaxes(
        showspikes=True,
        spikemode="across",
        spikesnap="cursor",
        spikecolor="rgba(0,0,0,0.6)",
        spikethickness=1,
        spikedash="dot",
    )
    st.plotly_chart(fig, use_container_width=True)

    # with st.expander("Raw OHLCV data"):
    #     st.dataframe(ohlcv.set_index("date"), use_container_width=True)


def _add_rsi(fig: go.Figure, df: pd.DataFrame, row: int):
    if "rsi" in df.columns:
        fig.add_trace(go.Scatter(x=df["date"], y=df["rsi"], name="RSI", line=dict(color="#ab47bc")), row=row, col=1)
        fig.add_hline(y=70, line_dash="dot", line_color="red", row=row, col=1)
        fig.add_hline(y=30, line_dash="dot", line_color="green", row=row, col=1)


def _add_macd(fig: go.Figure, df: pd.DataFrame, row: int):
    if "macd" in df.columns:
        fig.add_trace(go.Scatter(x=df["date"], y=df["macd"], name="MACD", line=dict(color="#42a5f5")), row=row, col=1)
    if "macd_signal" in df.columns:
        fig.add_trace(go.Scatter(x=df["date"], y=df["macd_signal"], name="Signal", line=dict(color="#ef5350", dash="dash")), row=row, col=1)
    if "macd_hist" in df.columns:
        colors = ["#26a69a" if v >= 0 else "#ef5350" for v in df["macd_hist"]]
        fig.add_trace(go.Bar(x=df["date"], y=df["macd_hist"], name="Histogram", marker_color=colors), row=row, col=1)


def _add_pivot(fig: go.Figure, df: pd.DataFrame, row: int):
    pivot_cols = ["pivot", "r1", "s1", "r2", "s2"]
    colors_map = {"pivot": "white", "r1": "#ef9a9a", "s1": "#a5d6a7", "r2": "#e53935", "s2": "#2e7d32"}
    for col_name in pivot_cols:
        if col_name in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df["date"], y=df[col_name], name=col_name.upper(),
                    line=dict(color=colors_map.get(col_name, "gray"), width=1, dash="dot"),
                ),
                row=row, col=1,
            )
