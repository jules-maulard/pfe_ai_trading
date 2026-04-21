import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.ui.helpers import AGENTS, ask_agent, build_agent, list_symbols, load_fundamental, run_async

STATEMENT_TYPES = ["income_statement", "balance_sheet", "cash_flow", "financial_ratios", "dividends"]
STATEMENT_LABELS = {
    "income_statement": "📋 Income Statement",
    "balance_sheet": "🏛 Balance Sheet",
    "cash_flow": "💵 Cash Flow",
    "financial_ratios": "📐 Financial Ratios",
    "dividends": "💰 Dividends",
}

# Columns to plot per statement type: (list of series, is_percentage, chart_title)
_CHART_CONFIG = {
    "income_statement": {
        "bars": ["total_revenue", "gross_profit", "ebitda", "net_income"],
        "lines": [],
        "pct": False,
        "title": "Revenue & Profitability (€)",
    },
    "balance_sheet": {
        "bars": ["total_assets", "total_liabilities", "stockholders_equity"],
        "lines": ["net_debt"],
        "pct": False,
        "title": "Balance Sheet Overview (€)",
    },
    "cash_flow": {
        "bars": ["operating_cash_flow", "free_cash_flow", "capital_expenditure"],
        "lines": [],
        "pct": False,
        "title": "Cash Flow (€)",
    },
    "financial_ratios": {
        "bars": [],
        "lines": ["gross_margin", "operating_margin", "net_margin", "return_on_equity"],
        "pct": True,
        "title": "Margins & Returns (%)",
    },
    "dividends": {
        "bars": ["amount"],
        "lines": [],
        "pct": False,
        "title": "Dividend History (€ per share)",
    },
}

_COLORS = ["#4C9BE8", "#F4845F", "#5CB85C", "#9B59B6", "#F0AD4E", "#E74C3C"]



def _render_chart(df: pd.DataFrame, stmt_type: str):
    cfg = _CHART_CONFIG.get(stmt_type)
    if cfg is None or df.empty or "date" not in df.columns:
        return

    bar_cols = [c for c in cfg["bars"] if c in df.columns]
    line_cols = [c for c in cfg["lines"] if c in df.columns]
    if not bar_cols and not line_cols:
        return

    df_sorted = df.sort_values("date")
    x = df_sorted["date"].astype(str)
    multiplier = 1 if cfg["pct"] else 1

    fig = go.Figure()
    color_idx = 0

    for col in bar_cols:
        vals = df_sorted[col]
        if not cfg["pct"]:
            vals = vals / 1e6  # show in millions
        fig.add_trace(go.Bar(
            name=col.replace("_", " ").title(),
            x=x,
            y=vals,
            marker_color=_COLORS[color_idx % len(_COLORS)],
        ))
        color_idx += 1

    for col in line_cols:
        vals = df_sorted[col]
        if not cfg["pct"]:
            vals = vals / 1e6
        fig.add_trace(go.Scatter(
            name=col.replace("_", " ").title(),
            x=x,
            y=vals,
            mode="lines+markers",
            line=dict(color=_COLORS[color_idx % len(_COLORS)], width=2),
        ))
        color_idx += 1

    y_label = "%" if cfg["pct"] else "€ millions"
    fig.update_layout(
        title=cfg["title"],
        barmode="group",
        xaxis_title="Date",
        yaxis_title=y_label,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=20, t=60, b=40),
        height=320,
        template="plotly_dark",
    )
    st.plotly_chart(fig, use_container_width=True)


def render():
    st.header("Fundamentals")

    symbols_available = list_symbols()

    tab_data, tab_chat = st.tabs(["📊 Data Explorer", "🤖 Agent Chat"])

    # ── Data Explorer ───────────────────────────────────────────────────────────
    with tab_data:
        if not symbols_available:
            st.warning("No symbols found in storage.")
        else:
            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                symbol = st.selectbox("Symbol", symbols_available, key="fund_symbol")
            with col2:
                start = st.date_input("Start", pd.Timestamp("2020-01-01"), key="fund_start")
            with col3:
                end = st.date_input("End", pd.Timestamp("today"), key="fund_end")

            if st.button("Load", type="primary", key="fund_load"):
                st.session_state.fund_loaded_symbol = symbol
                st.session_state.fund_loaded_start = str(start)
                st.session_state.fund_loaded_end = str(end)

            sym = st.session_state.get("fund_loaded_symbol")
            if not sym:
                st.info("Select a symbol and click **Load**.")
            else:
                with st.spinner("Loading fundamental data…"):
                    datasets: dict[str, pd.DataFrame] = {}
                    errors: dict[str, str] = {}
                    for stmt_type in STATEMENT_TYPES:
                        try:
                            df = load_fundamental(
                                statement_type=stmt_type,
                                symbols=[sym],
                                start=st.session_state.fund_loaded_start,
                                end=st.session_state.fund_loaded_end,
                            )
                            if not df.empty and "date" in df.columns:
                                df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
                            datasets[stmt_type] = df
                        except Exception as e:
                            errors[stmt_type] = str(e)

                for stmt_type in STATEMENT_TYPES:
                    st.subheader(STATEMENT_LABELS[stmt_type])
                    if stmt_type in errors:
                        st.error(f"Error: {errors[stmt_type]}")
                    else:
                        df = datasets[stmt_type]
                        if df.empty:
                            st.info(f"No data available for **{sym}**.")
                        else:
                            _render_chart(df, stmt_type)
                    st.divider()

    # ── Agent Chat ──────────────────────────────────────────────────────────────
    with tab_chat:
        if "fund_chat_history" not in st.session_state:
            st.session_state.fund_chat_history = []

        col_reset, _ = st.columns([1, 5])
        with col_reset:
            if st.button("🗑 Reset conversation", key="fund_reset"):
                st.session_state.fund_chat_history = []
                st.rerun()

        chat_container = st.container(height=380)
        with chat_container:
            for msg in st.session_state.fund_chat_history:
                with st.chat_message(msg["role"]):
                    st.markdown(msg["content"])

        _fund_config = AGENTS["Fundamentals"]
        if st.session_state.get("fund_agent_config") != _fund_config:
            _old = st.session_state.get("fund_agent_instance")
            if _old is not None:
                run_async(_old.disconnect())
            with st.spinner("Initializing agent…"):
                st.session_state.fund_agent_instance = run_async(build_agent(_fund_config))
            st.session_state.fund_agent_config = _fund_config
            st.session_state.fund_chat_history = []

        user_input = st.chat_input("Ask the Fundamentals agent…", key="fund_chat_input")
        if user_input:
            st.session_state.fund_chat_history.append({"role": "user", "content": user_input})
            with chat_container:
                with st.chat_message("user"):
                    st.markdown(user_input)
                with st.chat_message("assistant"):
                    with st.spinner("Thinking…"):
                        try:
                            response = run_async(
                                ask_agent(st.session_state.fund_agent_instance, user_input)
                            )
                        except Exception as e:
                            response = f"⚠️ Error: {e}"
                    st.markdown(response)
            st.session_state.fund_chat_history.append({"role": "assistant", "content": response})
