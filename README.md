# PFE AI Trading

Plateforme d'analyse technique et fondamentale boursière basée sur des agents IA (MCP + LLM).

## Installation

```bash
pip install -r requirements.txt
```

Créer un fichier `.env` à la racine avec les variables nécessaires (clés API, config Snowflake, etc.).

---

## 1. Chargement des données (Pipeline)

```bash
# Chargement quotidien complet (OHLCV + indicateurs)
python -m src.data.market_data_pipeline --profile daily

# OHLCV uniquement
python -m src.data.market_data_pipeline --profile daily_ohlcv

# Indicateurs uniquement
python -m src.data.market_data_pipeline --profile compute_indicators

# Indicateurs individuels
python -m src.data.market_data_pipeline --profile daily_rsi
python -m src.data.market_data_pipeline --profile daily_macd
python -m src.data.market_data_pipeline --profile daily_pivot

# Fondamentaux
python -m src.data.market_data_pipeline --profile daily_fundamentals

# Ajouter de nouveaux symboles
python -m src.data.market_data_pipeline --profile add_symbols

# Rattrapage historique
python -m src.data.market_data_pipeline --profile catchup
```

Profils disponibles dans `src/data/config/profiles/`.

---

## 2. Lancer un agent IA (CLI)

```bash
# Par nom d'agent
python -m src.agents.agent_runner --agent macd
python -m src.agents.agent_runner --agent rsi
python -m src.agents.agent_runner --agent pivot
python -m src.agents.agent_runner --agent news
python -m src.agents.agent_runner --agent fundamentals
python -m src.agents.agent_runner --agent screener
python -m src.agents.agent_runner --agent orchestrator

# Par fichier de config
python -m src.agents.agent_runner --config src/agents/configs/macd.yaml
```

### Commandes dans le chat agent

| Commande     | Description                    |
|--------------|--------------------------------|
| `/tools`     | Liste les outils disponibles   |
| `/prompts`   | Liste les prompts disponibles  |
| `/resources` | Liste les ressources           |
| `/tokens`    | Statistiques de tokens         |
| `/memory`    | Historique de conversation     |
| `/reset`     | Réinitialiser la conversation  |
| `/quit`      | Quitter                        |

---

## 3. Lancer l'interface web (Streamlit)

```bash
streamlit run src/ui/app.py
```

---

## Structure du projet

```
src/
├── agents/          # Agents IA, configs YAML, CLI
├── data/            # Pipeline de données, indicateurs, stockage
├── mcp_servers/     # Serveurs MCP (MACD, RSI, Pivot, News, Screener, Fundamentals)
├── ui/              # Interface Streamlit
└── utils/           # Logger
```

## Tests

```bash
python -m pytest tests/
```