from __future__ import annotations

from typing import List, Optional

import requests
import yfinance as yf
from bs4 import BeautifulSoup

from ...utils import get_logger

logger = get_logger(__name__)

_DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}
_REQUEST_TIMEOUT = 10


class NewsRetriever:

    def __init__(self, max_articles: int = 10, fetch_body: bool = True):
        self.max_articles = max_articles
        self.fetch_body = fetch_body

    def get_news(self, symbol: str, max_articles: Optional[int] = None) -> List[dict]:
        limit = max_articles or self.max_articles
        raw = self._fetch_raw_news(symbol)
        articles = [self._normalize(item) for item in raw[:limit]]
        if self.fetch_body:
            for article in articles:
                url = article.get("url")
                if url:
                    article["body"] = self._scrape_body(url)
        return articles

    def _fetch_raw_news(self, symbol: str) -> list:
        try:
            ticker = yf.Ticker(symbol)
            return ticker.news or []
        except Exception as exc:
            logger.error("Failed to fetch news for %s: %s", symbol, exc)
            return []

    @staticmethod
    def _normalize(item: dict) -> dict:
        content = item.get("content")
        if not isinstance(content, dict):
            return item
        canonical = content.get("canonicalUrl") or {}
        click = content.get("clickThroughUrl") or {}
        provider = content.get("provider") or {}
        return {
            "title": content.get("title") or content.get("headline"),
            "summary": content.get("summary") or content.get("description"),
            "published_at": content.get("pubDate") or content.get("displayTime"),
            "url": canonical.get("url") or click.get("url"),
            "source": provider.get("displayName") if isinstance(provider, dict) else None,
        }

    @staticmethod
    def _scrape_body(url: str) -> str:
        try:
            resp = requests.get(url, headers=_DEFAULT_HEADERS, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            container = (
                soup.find("div", {"class": "caas-body"})
                or soup.find("article")
                or soup.find("div", {"class": "article-body"})
                or soup.find("div", {"class": "body"})
                or soup.body
            )
            if not container:
                return ""
            paragraphs = container.find_all("p")
            return "\n\n".join(
                p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)
            )
        except Exception as exc:
            logger.warning("Failed to scrape %s: %s", url, exc)
            return ""
