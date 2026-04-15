import sys
from pathlib import Path

import streamlit as st

ROOT_DIRECTORY = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIRECTORY))

from src.ui.views import home, chat, market_data, about

APPLICATION_PAGES = [
    {
        "page": home.render,
        "title": "Accueil",
        "icon": "🏠",
        "url_path": "home"
    },
    {
        "page": chat.render, 
        "title": "Chat", 
        "icon": "💬", 
        "url_path": "chat"
    },
    {
        "page": market_data.render, 
        "title": "Market Data", 
        "icon": "📈", 
        "url_path": "market-data"
    },
    {
        "page": about.render, 
        "title": "About", 
        "icon": "ℹ️", 
        "url_path": "about"
    },
]

st.set_page_config(
    page_title="AI Trading Assistant",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

navigation_pages = [st.Page(**page_configuration) for page_configuration in APPLICATION_PAGES]
application_navigation = st.navigation(navigation_pages, position="hidden")

with st.sidebar:
    st.title("📈 AI Trading")
    st.markdown("---")

    for streamlit_page in navigation_pages:
        st.page_link(streamlit_page)
    
    st.markdown("---")
    st.caption("PFE — AI Trading Assistant")
    st.caption("By Jules Maulard")


application_navigation.run()