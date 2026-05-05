import pandas as pd
import pytest

from src.ui.directives import parse_directives, strip_directives


def test_parse_single_ohlcv_directive():
    text = (
        "Analysis done.\n"
        "```chart\n"
        '{"type": "ohlcv_chart", "symbol": "AAPL", "start": "2024-01-01", "end": "2024-12-31"}\n'
        "```"
    )
    directives = parse_directives(text)
    assert len(directives) == 1
    assert directives[0]["type"] == "ohlcv_chart"
    assert directives[0]["symbol"] == "AAPL"


def test_parse_multiple_directives():
    text = (
        "```chart\n"
        '{"type":"ohlcv_chart","symbol":"AAPL","start":"2024-01-01","end":"2024-12-31"}\n'
        "```\n"
        "Some text.\n"
        "```chart\n"
        '{"type":"indicator_series","symbol":"AAPL","indicator":"rsi","start":"2024-01-01","end":"2024-12-31"}\n'
        "```"
    )
    directives = parse_directives(text)
    assert len(directives) == 2
    assert directives[1]["indicator"] == "rsi"


def test_parse_invalid_json_is_skipped():
    text = "```chart\nnot_json\n```"
    assert parse_directives(text) == []


def test_parse_no_blocks():
    assert parse_directives("Just a normal response.") == []


def test_strip_removes_blocks_preserves_text():
    text = (
        "Before.\n"
        "```chart\n"
        '{"type":"ohlcv_chart","symbol":"AAPL","start":"2024-01-01","end":"2024-12-31"}\n'
        "```\n"
        "After."
    )
    stripped = strip_directives(text)
    assert "```chart" not in stripped
    assert "Before." in stripped
    assert "After." in stripped


def test_strip_no_directives_unchanged():
    text = "Clean response with no directives."
    assert strip_directives(text) == text


def test_render_directive_ohlcv(monkeypatch):
    df = pd.DataFrame({
        "date": ["2024-01-01", "2024-01-02"],
        "open": [100.0, 101.0],
        "high": [105.0, 106.0],
        "low": [99.0, 100.0],
        "close": [103.0, 104.0],
        "volume": [1000, 1200],
    })
    monkeypatch.setattr("src.ui.helpers.load_ohlcv", lambda *a, **kw: df)
    monkeypatch.setattr("streamlit.plotly_chart", lambda *a, **kw: None)

    from src.ui.directives import render_directive
    render_directive({"type": "ohlcv_chart", "symbol": "AAPL", "start": "2024-01-01", "end": "2024-12-31"})


def test_render_directive_empty_data_shows_info(monkeypatch):
    monkeypatch.setattr("src.ui.helpers.load_ohlcv", lambda *a, **kw: pd.DataFrame())
    monkeypatch.setattr("src.ui.helpers.list_symbols", lambda: ["AAPL", "TSLA"])
    info_calls = []
    monkeypatch.setattr("streamlit.info", lambda msg: info_calls.append(msg))

    from src.ui.directives import render_directive
    render_directive({"type": "ohlcv_chart", "symbol": "UNKNOWN", "start": "2024-01-01", "end": "2024-12-31"})
    assert info_calls


def test_render_directive_indicator_series(monkeypatch):
    df = pd.DataFrame({
        "date": ["2024-01-01", "2024-01-02"],
        "rsi": [45.0, 52.0],
    })
    monkeypatch.setattr("src.ui.helpers.load_indicator", lambda *a, **kw: df)
    monkeypatch.setattr("streamlit.plotly_chart", lambda *a, **kw: None)

    from src.ui.directives import render_directive
    render_directive({"type": "indicator_series", "symbol": "AAPL", "indicator": "rsi", "start": "2024-01-01", "end": "2024-12-31"})


def test_render_directive_unknown_type_shows_warning(monkeypatch):
    warnings = []
    monkeypatch.setattr("streamlit.warning", lambda msg: warnings.append(msg))

    from src.ui.directives import render_directive
    render_directive({"type": "unknown_chart"})
    assert warnings
