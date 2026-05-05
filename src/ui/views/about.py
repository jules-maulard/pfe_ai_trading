import streamlit as st

from src.ui.helpers import get_pipeline_last_run_summary, list_symbols


def render():
    st.header("About")
    st.markdown("""
This is a **proof-of-concept AI trading assistant** built as part of a final-year engineering project (PFE).

### Architecture
| Layer | Components |
|---|---|
| **Agents** | MACD, RSI, Pivot Points, News |
| **MCP Servers** | macd_server, rsi_server, pivot_server, news_server, screener_server |
| **Storage** | CSV (DuckDB) · Snowflake |
| **LLM** | Groq / OpenAI via litellm |

### Agents
- **MACD Agent** — Crossovers, divergences, momentum analysis
- **RSI Agent** — Overbought/oversold, failure swings, multi-timeframe
- **Pivot Points Agent** — Support/resistance, proximity interactions
- **News Agent** — Headline scraping + sentiment screening

### Data
Daily OHLCV bars from **yfinance** covering CAC 40 equities.
Indicators (RSI, MACD, Pivot) pre-computed and stored locally.
                
### UI
To run the UI, use the following command:
```
streamlit run src/ui/app.py
```
    """)

    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Indicators", "3")
    with col2:
        st.metric("MCP Servers", "5")
    with col3:
        try:
            n = len(list_symbols())
            st.metric("Symbols", n)
        except Exception:
            st.metric("Symbols", "—")
    with col4:
        last_run = get_pipeline_last_run_summary()
        st.metric("Dernière pipeline", last_run or "—")

    st.markdown("---")
    with st.expander("Symboles disponibles dans le stockage", expanded=False):
        try:
            syms = list_symbols()
            if syms:
                cols = st.columns(6)
                for i, sym in enumerate(syms):
                    cols[i % 6].write(sym)
            else:
                st.write("Aucun symbole trouvé.")
        except Exception as exc:
            st.write("Erreur :", exc)
