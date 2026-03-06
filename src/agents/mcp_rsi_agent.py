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

from __future__ import annotations

from pathlib import Path
import asyncio
import sys

# Thin wrapper delegating to the generic runner. Keeps one implementation for
# agent lifecycle while allowing per-agent config files under `configs/`.
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
    cfg = Path(__file__).resolve().parent / "configs" / "rsi.yaml"
    runner_main(str(cfg))
