#!/usr/bin/env python3
"""
Retrieve multiple papers from arXiv, extract full text, and persist to PostgreSQL.
"""

from __future__ import annotations

import logging
import urllib.parse
import xml.etree.ElementTree as ET
from argparse import ArgumentParser
from typing import Any, Iterable

import requests

from fulltext_fetcher import enrich_paper_with_body_text
from fulltext_pg_store import PaperContentStore, PostgresConfig


LOGGER = logging.getLogger(__name__)
ARXIV_API_URL = "https://export.arxiv.org/api/query"


def _extract_arxiv_id(entry_id: str) -> str:
    """
    Example entry_id: http://arxiv.org/abs/1706.03762v7 -> 1706.03762
    """
    raw = entry_id.rstrip("/").split("/")[-1]
    return raw.split("v")[0]


def search_arxiv_papers(query: str, limit: int) -> list[dict[str, Any]]:
    """
    Real multi-paper retrieval via arXiv API.
    """
    encoded_query = urllib.parse.quote_plus(query)
    url = (
        f"{ARXIV_API_URL}?search_query=all:{encoded_query}"
        f"&start=0&max_results={limit}"
        "&sortBy=relevance&sortOrder=descending"
    )

    response = requests.get(
        url,
        timeout=30,
        headers={"User-Agent": "academic-search-fulltext-pipeline/1.0"},
    )
    response.raise_for_status()

    root = ET.fromstring(response.text)
    atom_ns = {"atom": "http://www.w3.org/2005/Atom"}
    papers: list[dict[str, Any]] = []

    for entry in root.findall("atom:entry", atom_ns):
        title = (entry.findtext("atom:title", default="", namespaces=atom_ns) or "").strip()
        abstract = (
            entry.findtext("atom:summary", default="", namespaces=atom_ns) or ""
        ).strip()
        published = entry.findtext("atom:published", default="", namespaces=atom_ns) or ""
        entry_id = entry.findtext("atom:id", default="", namespaces=atom_ns) or ""
        arxiv_id = _extract_arxiv_id(entry_id) if entry_id else ""
        year = int(published[:4]) if len(published) >= 4 and published[:4].isdigit() else None
        paper_id = f"arxiv:{arxiv_id}" if arxiv_id else None

        # Prefer direct PDF relation link if present; otherwise construct from arXiv ID.
        pdf_url = ""
        for link in entry.findall("atom:link", atom_ns):
            if link.attrib.get("type") == "application/pdf":
                pdf_url = link.attrib.get("href", "")
                break
        if not pdf_url and arxiv_id:
            pdf_url = f"https://arxiv.org/pdf/{arxiv_id}"

        if not title:
            continue

        papers.append(
            {
                "paper_id": paper_id,
                "title": title,
                "abstract": abstract,
                "body_text": "",
                "year": year,
                "arxiv_id": arxiv_id,
                "pdf_url": pdf_url,
                "source_platforms": ["arxiv"],
            }
        )

    return papers


def persist_search_results(papers: Iterable[dict[str, Any]]) -> list[str]:
    cfg = PostgresConfig.from_env()
    with PaperContentStore(cfg) as store:
        store.ensure_table()
        normalized = [enrich_paper_with_body_text(p) for p in papers]
        return list(store.upsert_many(normalized))


def run_pipeline(query: str, limit: int = 10) -> list[str]:
    """
    End-to-end real pipeline:
    1) retrieve multiple papers
    2) fetch full text from PDF
    3) upsert into squai_table
    """
    papers = search_arxiv_papers(query, limit=limit)
    LOGGER.info("Retrieved %d papers from arXiv for query=%r", len(papers), query)
    if not papers:
        return []
    written_ids = persist_search_results(papers)
    LOGGER.info("Persisted %d papers to squai_table.", len(written_ids))
    return written_ids


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = ArgumentParser(description="Retrieve multi papers and persist full text.")
    parser.add_argument("--query", required=True, help="Search query for papers.")
    parser.add_argument("--limit", type=int, default=10, help="Number of papers to fetch.")
    args = parser.parse_args()
    run_pipeline(args.query, limit=args.limit)
