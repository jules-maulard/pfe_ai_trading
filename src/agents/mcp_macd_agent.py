from __future__ import annotations

import asyncio
import sys
from pathlib import Path

_SRC = str(Path(__file__).resolve().parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

# Ensure both project root and src directory are on sys.path so
# imports using `src.*` and bare `utils.*` both work when running
# this script directly.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
_ROOT = str(PROJECT_ROOT)
_SRC_DIR = str(SRC_DIR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from pathlib import Path
import asyncio
import sys

# Lightweight wrapper: delegate to the generic runner so we keep one implementation
# for all agents and only change per-agent config files in `src/agents/configs/`.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
_ROOT = str(PROJECT_ROOT)
_SRC_DIR = str(SRC_DIR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from src.agents.agent_runner import main as runner_main


if __name__ == "__main__":
    # Default to the MACD config file next to this module
    cfg = Path(__file__).resolve().parent / "configs" / "macd.yaml"
    # `runner_main` is synchronous and will run the event loop itself.
    runner_main(str(cfg))
