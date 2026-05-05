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

    st.markdown("### 💬 Chat avec l'agent principal")
    st.markdown(
        "Posez des questions en langage naturel à l'agent principal pour obtenir des analyses de marché, "
        "des recommandations d'investissement, ou l'analyse complète d'un symbole."
    )

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 💬 Chat avec les agents spécialisés")
        st.markdown(
            "Posez des questions en langage naturel à des agents spécialisés :\n"
            "- **MACD** — Croisements, divergences, momentum\n"
            "- **RSI** — Zones de surachat/survente, failure swings\n"
            "- **Pivot Points** — Supports, résistances, interactions\n"
            "- **Fundamental** — Données financières et dividendes\n"
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
        )
        # st.page_link("pages/market_data.py", label="Ouvrir les Données →", icon="📈")

    st.markdown("---")
    st.caption("PFE — Projet de Fin d'Études · Jules Maulard")
