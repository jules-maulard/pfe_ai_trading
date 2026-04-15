import streamlit as st

from src.ui.helpers import list_symbols


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
    col1, col2, col3 = st.columns(3)
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
