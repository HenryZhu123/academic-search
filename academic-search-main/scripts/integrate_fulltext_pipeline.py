#!/usr/bin/env python3
"""
Integration example: wire full-text persistence into existing search flow.

Usage:
  1) Replace `retrieve_papers_from_agent` with your real retrieval entrypoint.
  2) Run this file after setting PostgreSQL env vars (optional, defaults provided).
"""

from __future__ import annotations

import logging
from typing import Any, Iterable

from fulltext_fetcher import enrich_paper_with_body_text
from fulltext_pg_store import PaperContentStore, PostgresConfig


LOGGER = logging.getLogger(__name__)


def retrieve_papers_from_agent(query: str) -> list[dict[str, Any]]:
    """
    Placeholder for the current academic-search retrieval output.
    Replace this function with your actual agent workflow result.
    """
    return [
        {
            "paper_id": "demo-001",
            "title": f"Demo paper for query: {query}",
            "abstract": "This is a demo abstract.",
            "body_text": "This is demo body text extracted from PDF or source.",
            "year": 2026,
        }
    ]


def persist_search_results(papers: Iterable[dict[str, Any]]) -> list[str]:
    cfg = PostgresConfig.from_env()
    with PaperContentStore(cfg) as store:
        store.ensure_table()
        normalized = [enrich_paper_with_body_text(p) for p in papers]
        return list(store.upsert_many(normalized))


def run_pipeline(query: str) -> list[str]:
    """
    Low-intrusion integration point:
    - keep retrieval logic unchanged
    - add one persistence step after papers are produced
    """
    papers = retrieve_papers_from_agent(query)
    written_ids = persist_search_results(papers)
    LOGGER.info("Persisted %d papers to squai_table.", len(written_ids))
    return written_ids


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run_pipeline("graph neural network")
