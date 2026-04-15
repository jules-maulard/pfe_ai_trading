import streamlit as st

from src.ui.helpers import AGENTS, ask_agent, run_async


def render():
    st.header("Agent Chat")

    col1, col2 = st.columns([2, 1])
    with col1:
        agent_name = st.selectbox("Select Agent", list(AGENTS.keys()))
    with col2:
        if st.button("🗑 Reset conversation", use_container_width=True):
            st.session_state.chat_history = []
            st.rerun()

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    if "current_agent" not in st.session_state:
        st.session_state.current_agent = agent_name

    if st.session_state.current_agent != agent_name:
        st.session_state.chat_history = []
        st.session_state.current_agent = agent_name

    chat_container = st.container(height=380) # 480
    with chat_container:
        for msg in st.session_state.chat_history:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

    user_input = st.chat_input(f"Ask the {agent_name} agent…")
    if user_input:
        st.session_state.chat_history.append({"role": "user", "content": user_input})
        with chat_container:
            with st.chat_message("user"):
                st.markdown(user_input)
            with st.chat_message("assistant"):
                with st.spinner("Thinking…"):
                    history_snapshot = st.session_state.chat_history[:-1]
                    try:
                        response = run_async(
                            ask_agent(AGENTS[agent_name], history_snapshot, user_input)
                        )
                    except Exception as e:
                        response = f"⚠️ Error: {e}"
                st.markdown(response)
        st.session_state.chat_history.append({"role": "assistant", "content": response})
