import streamlit as st

from src.ui.helpers import (
    AGENTS,
    ask_agent,
    build_agent,
    conversation_store,
    run_async,
)
from src.ui.directives import parse_directives, render_directive, strip_directives
from src.agents.conversations import PersistentMemory


def _humanize(name: str) -> str:
    return name.replace("_", " ").title()


def _render_message(content: str) -> None:
    directives = parse_directives(content)
    st.markdown(strip_directives(content) if directives else content)
    for directive in directives:
        render_directive(directive)


def _run_prompt(agent_instance, prompt_name: str, arguments: dict, chat_container):
    arg_str = " ".join(f"{k}={v}" for k, v in arguments.items())
    display = f"📋 `{prompt_name}`" + (f" ({arg_str})" if arg_str else "")

    st.session_state.chat_history.append({"role": "user", "content": display})
    with chat_container:
        with st.chat_message("user"):
            st.markdown(display)
        with st.chat_message("assistant"):
            with st.spinner("Running prompt…"):
                try:
                    response = run_async(agent_instance.run_prompt(prompt_name, arguments))
                except Exception as e:
                    response = f"⚠️ Error: {e}"
            _render_message(response)
    st.session_state.chat_history.append({"role": "assistant", "content": response})
    _persist_if_needed()


def render():
    st.header("Agent Chat")

    # ── Conversation history sidebar (discret expander) ────────────
    _render_conversation_panel()

    col1, col2 = st.columns([2, 1])
    with col1:
        agent_name = st.selectbox("Select Agent", list(AGENTS.keys()))
    with col2:
        if st.button("🗑 Reset conversation", use_container_width=True):
            st.session_state.chat_history = []
            agent = st.session_state.get("agent_instance")
            if agent is not None:
                run_async(agent.reset_conversation())
            st.rerun()

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    config_path = AGENTS[agent_name]
    conv_id = st.session_state.get("conversation_id")

    # Rebuild agent when config or conversation changes
    needs_rebuild = (
        st.session_state.get("agent_config") != config_path
        or st.session_state.get("_agent_conv_id") != conv_id
    )
    if needs_rebuild:
        old = st.session_state.get("agent_instance")
        if old is not None:
            run_async(old.disconnect())
        with st.spinner("Initializing agent…"):
            st.session_state.agent_instance = run_async(build_agent(config_path, conv_id))
        st.session_state.agent_config = config_path
        st.session_state._agent_conv_id = conv_id
        # Load chat_history from persistent memory if resuming
        if conv_id:
            try:
                payload = conversation_store.load(conv_id)
                # Rebuild display history (user/assistant only)
                st.session_state.chat_history = [
                    {"role": m["role"], "content": m.get("content") or ""}
                    for m in payload.get("history", [])
                    if m["role"] in ("user", "assistant") and m.get("content")
                ]
            except FileNotFoundError:
                st.session_state.chat_history = []
        else:
            st.session_state.chat_history = []
        st.session_state.agent_prompts = agent_instance_prompts(st.session_state.agent_instance)

    agent_instance = st.session_state.agent_instance
    prompts = st.session_state.get("agent_prompts", [])

    # ── Suggested prompts ──────────────────────────────
    _prompts_expanded = not st.session_state.chat_history and "_pending_prompt" not in st.session_state
    if prompts:
        with st.expander("📋 Suggested prompts", expanded=_prompts_expanded):
            for idx, prompt in enumerate(prompts):
                args = prompt.arguments or []
                arg_names = [a.name for a in args]
                desc = prompt.description or ""

                st.markdown(f"**{_humanize(prompt.name)}**" + (f"  \n{desc}" if desc else ""))

                if not arg_names:
                    if st.button(f"▶ Run", key=f"prompt_run_{idx}", use_container_width=True):
                        st.session_state[f"_pending_prompt"] = (prompt.name, {})
                        st.rerun()
                else:
                    with st.form(key=f"prompt_form_{idx}"):
                        arg_values = {}
                        cols = st.columns(len(arg_names))
                        for col, arg_name in zip(cols, arg_names):
                            with col:
                                arg_values[arg_name] = st.text_input(
                                    arg_name.replace("_", " ").capitalize(),
                                    key=f"prompt_arg_{idx}_{arg_name}",
                                    placeholder=f"e.g. AI.PA",
                                )
                        if st.form_submit_button(f"▶ Run", use_container_width=True):
                            filled = {k: v for k, v in arg_values.items() if v.strip()}
                            if filled:
                                st.session_state[f"_pending_prompt"] = (prompt.name, filled)
                                st.rerun()
                            else:
                                st.warning("Please fill in the required argument(s).")

                if idx < len(prompts) - 1:
                    st.divider()

    # ── Chat container ─────────────────────────────────
    chat_container = st.container(height=380)
    with chat_container:
        for msg in st.session_state.chat_history:
            with st.chat_message(msg["role"]):
                if msg["role"] == "assistant":
                    _render_message(msg["content"])
                else:
                    st.markdown(msg["content"])

    # ── Handle pending prompt execution ────────────────
    pending = st.session_state.pop("_pending_prompt", None)
    if pending:
        prompt_name, prompt_args = pending
        _run_prompt(agent_instance, prompt_name, prompt_args, chat_container)

    # ── Free-text input ────────────────────────────────
    user_input = st.chat_input(f"Ask the {agent_name} agent…")
    if user_input:
        st.session_state.chat_history.append({"role": "user", "content": user_input})
        with chat_container:
            with st.chat_message("user"):
                st.markdown(user_input)
            with st.chat_message("assistant"):
                cmd = user_input.strip().lower()
                if cmd == "/memory":
                    response = _cmd_memory(agent_instance)
                elif cmd == "/tokens":
                    response = _cmd_tokens(agent_instance)
                elif cmd == "/tools":
                    response = _cmd_tools(agent_instance)
                elif cmd == "/resources":
                    response = _cmd_resources(agent_instance)
                elif cmd == "/prompts":
                    response = _cmd_prompts(agent_instance)
                else:
                    with st.spinner("Thinking…"):
                        try:
                            response = run_async(ask_agent(agent_instance, user_input))
                        except Exception as e:
                            response = f"⚠️ Error: {e}"
                _render_message(response)
        st.session_state.chat_history.append({"role": "assistant", "content": response})
        _persist_if_needed()


def agent_instance_prompts(agent) -> list:
    try:
        return agent.prompts
    except Exception:
        return []


# ── Debug command helpers ──────────────────────────────────────────────────────

def _cmd_memory(agent) -> str:
    try:
        messages = agent._memory.get_history()
    except Exception as e:
        return f"⚠️ Could not read memory: {e}"
    if not messages:
        return "Conversation memory is empty."
    lines = ["**Conversation Memory**", ""]
    for i, msg in enumerate(messages, 1):
        role = msg.get("role", "")
        content = str(msg.get("content", ""))
        preview = content[:200] + ("…" if len(content) > 200 else "")
        lines.append(f"**{i}. {role}:** {preview}")
    lines.append(f"\n*{len(messages)} message(s) in context.*")
    return "\n".join(lines)


def _cmd_tokens(agent) -> str:
    try:
        stats = agent.token_monitor.stats()
        messages = agent._memory.get_history()
    except Exception as e:
        return f"⚠️ Could not read token stats: {e}"
    lines = [
        "**Token Usage**",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Messages in context | {len(messages)} |",
        f"| LLM calls | {stats['llm_calls']} |",
        f"| Last context size | {stats['last_context_tokens']:,} tokens |",
        f"| Total prompt tokens | {stats['total_prompt_tokens']:,} |",
        f"| Total output tokens | {stats['total_completion_tokens']:,} |",
        f"| **Total tokens used** | **{stats['total_tokens']:,}** |",
    ]
    return "\n".join(lines)


def _cmd_tools(agent) -> str:
    try:
        tools = agent.tools
    except Exception as e:
        return f"⚠️ Could not list tools: {e}"
    if not tools:
        return "No tools available."
    lines = ["**Available Tools**", ""]
    for tool in tools:
        desc = (tool.description or "")[:100]
        lines.append(f"- **{tool.name}** — {desc}")
    return "\n".join(lines)


def _cmd_resources(agent) -> str:
    try:
        resources = agent.resources
    except Exception as e:
        return f"⚠️ Could not list resources: {e}"
    if not resources:
        return "No resources available."
    lines = ["**Available Resources**", ""]
    for resource in resources:
        name = getattr(resource, "name", str(resource))
        uri = getattr(resource, "uri", "")
        desc = (getattr(resource, "description", "") or "")[:80]
        lines.append(f"- **{name}** `{uri}`" + (f" — {desc}" if desc else ""))
    return "\n".join(lines)


def _cmd_prompts(agent) -> str:
    try:
        prompts = agent.prompts
    except Exception as e:
        return f"⚠️ Could not list prompts: {e}"
    if not prompts:
        return "No prompts available."
    lines = ["**Available Prompts**", ""]
    for prompt in prompts:
        arg_names = [a.name for a in (prompt.arguments or [])]
        params = f" `<{'> <'.join(arg_names)}>`" if arg_names else ""
        desc = (prompt.description or "")[:100]
        lines.append(f"- **{prompt.name}**{params}" + (f" — {desc}" if desc else ""))
    return "\n".join(lines)


# ── Conversation history panel & helpers ──────────────────────────────────────

def _persist_if_needed() -> None:
    """Persist conversation to disk if using PersistentMemory."""
    agent = st.session_state.get("agent_instance")
    if agent is None:
        return
    memory = getattr(agent, "_memory", None)
    if isinstance(memory, PersistentMemory):
        memory.persist()


def _render_conversation_panel() -> None:
    """Render a discreet conversation history panel as an expander."""
    with st.expander("📂 Historique des conversations", expanded=False):
        conversations = conversation_store.list()

        # New conversation button
        col_new, col_refresh = st.columns([1, 1])
        with col_new:
            if st.button("➕ Nouvelle conversation", use_container_width=True, key="conv_new"):
                agent_name = st.session_state.get("agent_config", "")
                conv_id = conversation_store.create(agent_name=agent_name)
                st.session_state.conversation_id = conv_id
                st.session_state.chat_history = []
                st.rerun()
        with col_refresh:
            if st.button("🔄", use_container_width=True, key="conv_refresh", help="Rafraîchir"):
                st.rerun()

        if not conversations:
            st.caption("Aucune conversation sauvegardée.")
            return

        current_id = st.session_state.get("conversation_id")

        for conv in conversations:
            conv_id = conv["id"]
            name = conv["name"] or f"Conv {conv_id[:6]}"
            msg_count = conv.get("message_count", 0)
            is_active = conv_id == current_id

            label = f"{'▶ ' if is_active else ''}{name} ({msg_count} msgs)"

            cols = st.columns([4, 1, 1])
            with cols[0]:
                if st.button(
                    label,
                    key=f"conv_load_{conv_id}",
                    use_container_width=True,
                    disabled=is_active,
                ):
                    st.session_state.conversation_id = conv_id
                    st.session_state._agent_conv_id = None  # force rebuild
                    st.rerun()
            with cols[1]:
                if st.button("✏️", key=f"conv_rename_{conv_id}", help="Renommer"):
                    st.session_state[f"_renaming_{conv_id}"] = True
                    st.rerun()
            with cols[2]:
                if st.button("🗑", key=f"conv_del_{conv_id}", help="Supprimer"):
                    conversation_store.delete(conv_id)
                    if current_id == conv_id:
                        st.session_state.conversation_id = None
                        st.session_state.chat_history = []
                        st.session_state._agent_conv_id = None
                    st.rerun()

            # Inline rename form
            if st.session_state.get(f"_renaming_{conv_id}"):
                new_name = st.text_input(
                    "Nouveau nom",
                    value=name,
                    key=f"conv_rename_input_{conv_id}",
                )
                if st.button("OK", key=f"conv_rename_ok_{conv_id}"):
                    if new_name.strip():
                        conversation_store.rename(conv_id, new_name.strip())
                    del st.session_state[f"_renaming_{conv_id}"]
                    st.rerun()
