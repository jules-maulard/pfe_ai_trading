from __future__ import annotations

from pathlib import Path

from .agent_runner import main as runner_main

if __name__ == "__main__":
    cfg = Path(__file__).resolve().parent / "configs" / "fundamentals.yaml"
    runner_main(str(cfg))
