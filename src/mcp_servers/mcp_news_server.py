from __future__ import annotations

import time
from typing import Dict, List

from fastmcp import FastMCP

from ..data.retrievers.news_retriever import NewsRetriever

mcp = FastMCP("News Tools")
news_retriever = NewsRetriever(fetch_body=False)

_RATE_LIMIT_DELAY = 1.0


@mcp.tool(name="health_check", description="Check server health.")
def health_check() -> Dict[str, str]:
    return {"status": "ok"}


@mcp.tool(
    name="get_recent_news",
    description=(
        "Fetch recent news headlines for a list of stock symbols. "
        "Returns titles, summaries, and publication dates only. "
        "Processes symbols sequentially to respect API rate limits."
    ),
)
def get_recent_news(symbols: List[str], max_articles: int = 3) -> str:
    import json

    results: Dict[str, list] = {}

    for i, symbol in enumerate(symbols):
        articles = news_retriever.get_news(symbol.upper(), max_articles=max_articles)
        results[symbol.upper()] = [
            {
                "title": a.get("title"),
                "summary": a.get("summary"),
                "published_at": a.get("published_at"),
            }
            for a in articles
        ]
        if i < len(symbols) - 1:
            time.sleep(_RATE_LIMIT_DELAY)

    return json.dumps(results, ensure_ascii=False, default=str)


if __name__ == "__main__":
    mcp.run()
