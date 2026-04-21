from src.agents.token_monitor import TokenMonitor


class TestTokenMonitorInitialState:
    def test_turns_is_zero(self):
        tm = TokenMonitor()
        assert tm.turns == 0

    def test_all_token_counts_are_zero(self):
        tm = TokenMonitor()
        assert tm.total_prompt_tokens == 0
        assert tm.total_completion_tokens == 0
        assert tm.total_tokens == 0

    def test_last_context_tokens_is_zero(self):
        tm = TokenMonitor()
        assert tm.last_context_tokens == 0

    def test_stats_returns_zeros(self):
        tm = TokenMonitor()
        stats = tm.stats()
        assert stats["llm_calls"] == 0
        assert stats["total_tokens"] == 0


class TestTokenMonitorRecord:
    def test_record_increments_turns(self):
        tm = TokenMonitor()
        tm.record(100, 50)
        assert tm.turns == 1

    def test_record_accumulates_prompt_tokens(self):
        tm = TokenMonitor()
        tm.record(100, 50)
        tm.record(200, 30)
        assert tm.total_prompt_tokens == 300

    def test_record_accumulates_completion_tokens(self):
        tm = TokenMonitor()
        tm.record(100, 50)
        tm.record(200, 30)
        assert tm.total_completion_tokens == 80

    def test_total_tokens_is_sum(self):
        tm = TokenMonitor()
        tm.record(100, 50)
        assert tm.total_tokens == 150

    def test_last_context_tokens_reflects_latest_prompt(self):
        tm = TokenMonitor()
        tm.record(100, 50)
        tm.record(300, 20)
        assert tm.last_context_tokens == 300


class TestTokenMonitorReset:
    def test_reset_clears_turns(self):
        tm = TokenMonitor()
        tm.record(100, 50)
        tm.reset()
        assert tm.turns == 0

    def test_reset_clears_all_tokens(self):
        tm = TokenMonitor()
        tm.record(100, 50)
        tm.reset()
        assert tm.total_tokens == 0
        assert tm.last_context_tokens == 0


class TestTokenMonitorStats:
    def test_stats_keys(self):
        tm = TokenMonitor()
        keys = set(tm.stats().keys())
        assert keys == {
            "llm_calls",
            "last_context_tokens",
            "total_prompt_tokens",
            "total_completion_tokens",
            "total_tokens",
        }

    def test_stats_values_after_records(self):
        tm = TokenMonitor()
        tm.record(100, 40)
        tm.record(200, 60)
        stats = tm.stats()
        assert stats["llm_calls"] == 2
        assert stats["total_prompt_tokens"] == 300
        assert stats["total_completion_tokens"] == 100
        assert stats["total_tokens"] == 400
        assert stats["last_context_tokens"] == 200
