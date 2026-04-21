import pytest

from src.agents.entities import Configuration


class TestLoadApiKeys:
    def test_single_key_from_groq_api_keys(self, monkeypatch):
        monkeypatch.setenv("GROQ_API_KEYS", "key-abc")
        monkeypatch.delenv("GROQ_API_KEY", raising=False)
        keys = Configuration._load_api_keys()
        assert keys == ["key-abc"]

    def test_multiple_keys_comma_separated(self, monkeypatch):
        monkeypatch.setenv("GROQ_API_KEYS", "key-1, key-2, key-3")
        monkeypatch.delenv("GROQ_API_KEY", raising=False)
        keys = Configuration._load_api_keys()
        assert keys == ["key-1", "key-2", "key-3"]

    def test_strips_whitespace_around_keys(self, monkeypatch):
        monkeypatch.setenv("GROQ_API_KEYS", "  key-a  ,  key-b  ")
        monkeypatch.delenv("GROQ_API_KEY", raising=False)
        keys = Configuration._load_api_keys()
        assert keys == ["key-a", "key-b"]

    def test_falls_back_to_groq_api_key_when_keys_absent(self, monkeypatch):
        monkeypatch.delenv("GROQ_API_KEYS", raising=False)
        monkeypatch.setenv("GROQ_API_KEY", "single-key")
        keys = Configuration._load_api_keys()
        assert keys == ["single-key"]

    def test_returns_empty_list_when_both_absent(self, monkeypatch):
        monkeypatch.delenv("GROQ_API_KEYS", raising=False)
        monkeypatch.delenv("GROQ_API_KEY", raising=False)
        keys = Configuration._load_api_keys()
        assert keys == []

    def test_ignores_empty_segments_in_comma_list(self, monkeypatch):
        monkeypatch.setenv("GROQ_API_KEYS", "key-1,,key-2,  ,key-3")
        monkeypatch.delenv("GROQ_API_KEY", raising=False)
        keys = Configuration._load_api_keys()
        assert keys == ["key-1", "key-2", "key-3"]
