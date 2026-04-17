import streamlit as st


def render():
    st.title("📈 AI Trading Assistant")
    st.markdown("### Bienvenue sur votre assistant de trading")
    st.markdown(
        "Cette application analyse les marchés financiers à l'aide d'agents spécialisés "
        "et d'indicateurs techniques. Sélectionnez une fonctionnalité depuis la barre latérale."
    )

    st.markdown("---")
    st.markdown("## Ce que vous pouvez faire")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 💬 Chat avec les agents")
        st.markdown(
            "Posez des questions en langage naturel à des agents spécialisés sur les indicateurs techniques :\n"
            "- **MACD** — Croisements, divergences, momentum\n"
            "- **RSI** — Zones de surachat/survente, failure swings\n"
            "- **Pivot Points** — Supports, résistances, interactions\n"
            "- **News** — Sentiment de marché à partir des actualités"
        )
        # st.page_link("pages/chat.py", label="Ouvrir le Chat →", icon="💬")

    with col2:
        st.markdown("### 📊 Données de marché")
        st.markdown(
            "Explorez les données historiques des actions du CAC 40 :\n"
            "- Cours OHLCV journaliers\n"
            "- Indicateurs pré-calculés (RSI, MACD, Pivot)\n"
            "- Dividendes et données financières\n"
            "- Screener multi-actifs"
        )
        # st.page_link("pages/market_data.py", label="Ouvrir les Données →", icon="📈")

    st.markdown("---")

    st.markdown("## Architecture")
    col3, col4, col5 = st.columns(3)
    with col3:
        st.info("**Agents IA**\n\nMACD · RSI · Pivot · News")
    with col4:
        st.info("**Serveurs MCP**\n\n5 serveurs spécialisés")
    with col5:
        st.info("**Stockage**\n\nCSV (DuckDB) · Snowflake")

    st.markdown("---")
    st.caption("PFE — Projet de Fin d'Études · Jules Maulard")
