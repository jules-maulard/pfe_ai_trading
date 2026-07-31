import pytest

from src.agents.entities import Configuration


class TestConfiguration:
    def test_from_env_loads_github_token(self, monkeypatch):
        monkeypatch.setenv("GITHUB_TOKEN", "gh-token-abc")
        cfg = Configuration.from_env()
        assert cfg.api_key == "gh-token-abc"

    def test_from_env_raises_when_token_missing(self, monkeypatch):
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        with pytest.raises(RuntimeError, match="GITHUB_TOKEN not found"):
            Configuration.from_env()

    def test_default_model(self, monkeypatch):
        monkeypatch.setenv("GITHUB_TOKEN", "gh-token")
        cfg = Configuration.from_env()
        assert cfg.model == "gpt-4o"

    def test_custom_model(self, monkeypatch):
        monkeypatch.setenv("GITHUB_TOKEN", "gh-token")
        cfg = Configuration.from_env(model="gpt-4o")
        assert cfg.model == "gpt-4o"
