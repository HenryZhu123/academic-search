#!/usr/bin/env python3
"""
Retrieve papers from PubMed, bioRxiv, and Semantic Scholar, return abstracts,
and persist full paper payloads into PostgreSQL.
"""

from __future__ import annotations

import json
import logging
import os
import re
import xml.etree.ElementTree as ET
from argparse import ArgumentParser
from datetime import date, timedelta
from typing import Any, Iterable

import requests

from fulltext_fetcher import enrich_paper_with_body_text
from fulltext_pg_store import PaperContentStore, PostgresConfig, resolve_paper_id


LOGGER = logging.getLogger(__name__)
PUBMED_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
PUBMED_EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
S2_SEARCH_URL = "https://api.semanticscholar.org/graph/v1/paper/search"
BIORXIV_DETAILS_URL = "https://api.biorxiv.org/details/biorxiv"
USER_AGENT = "academic-search-fulltext-pipeline/2.0"


def _to_year(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"\b(19|20)\d{2}\b", value)
    return int(match.group(0)) if match else None


def _clean_text(value: str | None) -> str:
    return (value or "").strip()


def _extract_pubmed_authors(article: ET.Element) -> list[str]:
    authors: list[str] = []
    for author in article.findall(".//AuthorList/Author"):
        collective = _clean_text(author.findtext("CollectiveName"))
        if collective:
            authors.append(collective)
            continue
        last_name = _clean_text(author.findtext("LastName"))
        fore_name = _clean_text(author.findtext("ForeName")) or _clean_text(
            author.findtext("Initials")
        )
        full_name = f"{fore_name} {last_name}".strip()
        if full_name:
            authors.append(full_name)
    return authors


def search_pubmed_papers(query: str, limit: int) -> list[dict[str, Any]]:
    search_resp = requests.get(
        PUBMED_ESEARCH_URL,
        params={
            "db": "pubmed",
            "term": query,
            "retmax": limit,
            "retmode": "json",
        },
        timeout=30,
        headers={"User-Agent": USER_AGENT},
    )
    search_resp.raise_for_status()
    id_list = search_resp.json().get("esearchresult", {}).get("idlist", [])
    if not id_list:
        return []

    fetch_resp = requests.get(
        PUBMED_EFETCH_URL,
        params={
            "db": "pubmed",
            "id": ",".join(id_list),
            "rettype": "abstract",
            "retmode": "xml",
        },
        timeout=45,
        headers={"User-Agent": USER_AGENT},
    )
    fetch_resp.raise_for_status()
    root = ET.fromstring(fetch_resp.text)

    papers: list[dict[str, Any]] = []
    for article in root.findall(".//PubmedArticle"):
        pmid = _clean_text(article.findtext(".//PMID"))
        title = _clean_text("".join(article.find(".//ArticleTitle").itertext()) if article.find(".//ArticleTitle") is not None else "")
        abstract_nodes = article.findall(".//Abstract/AbstractText")
        abstract = " ".join(_clean_text("".join(node.itertext())) for node in abstract_nodes).strip()
        venue = _clean_text(article.findtext(".//Journal/Title"))
        pubdate = (
            _clean_text(article.findtext(".//JournalIssue/PubDate/Year"))
            or _clean_text(article.findtext(".//JournalIssue/PubDate/MedlineDate"))
        )
        year = _to_year(pubdate)
        doi = ""
        for article_id in article.findall(".//ArticleId"):
            if article_id.attrib.get("IdType") == "doi":
                doi = _clean_text(article_id.text)
                break
        if not title:
            continue
        papers.append(
            {
                "paper_id": f"pubmed:{pmid}" if pmid else None,
                "title": title,
                "authors": _extract_pubmed_authors(article),
                "abstract": abstract,
                "body_text": "",
                "year": year,
                "venue": venue,
                "doi": doi or None,
                "pubmed_id": pmid or None,
                "pdf_url": "",
                "source_platforms": ["pubmed"],
                "raw_payload": {
                    "pmid": pmid,
                    "pubdate": pubdate,
                },
            }
        )
    return papers


def search_semantic_scholar_papers(query: str, limit: int) -> list[dict[str, Any]]:
    headers = {"User-Agent": USER_AGENT}
    api_key = os.getenv("S2_API_KEY", "").strip()
    if api_key:
        headers["x-api-key"] = api_key

    resp = requests.get(
        S2_SEARCH_URL,
        params={
            "query": query,
            "limit": limit,
            "fields": "title,authors,year,abstract,citationCount,externalIds,openAccessPdf,venue,url",
        },
        timeout=30,
        headers=headers,
    )
    resp.raise_for_status()
    data = resp.json().get("data", [])
    papers: list[dict[str, Any]] = []
    for item in data:
        title = _clean_text(item.get("title"))
        if not title:
            continue
        external_ids = item.get("externalIds") or {}
        arxiv_id = _clean_text(external_ids.get("ArXiv"))
        doi = _clean_text(external_ids.get("DOI"))
        pdf_url = _clean_text((item.get("openAccessPdf") or {}).get("url"))
        paper_id = _clean_text(item.get("paperId")) or None
        papers.append(
            {
                "paper_id": f"s2:{paper_id}" if paper_id else None,
                "title": title,
                "authors": [
                    _clean_text(author.get("name"))
                    for author in (item.get("authors") or [])
                    if _clean_text(author.get("name"))
                ],
                "abstract": _clean_text(item.get("abstract")),
                "body_text": "",
                "year": item.get("year"),
                "venue": _clean_text(item.get("venue")),
                "doi": doi or None,
                "arxiv_id": arxiv_id or None,
                "citation_count": item.get("citationCount"),
                "pdf_url": pdf_url,
                "source_platforms": ["semanticscholar"],
                "raw_payload": item,
            }
        )
    return papers


def search_biorxiv_papers(query: str, limit: int) -> list[dict[str, Any]]:
    # bioRxiv API is date-window based; fetch recent preprints then filter by query.
    end_day = date.today()
    start_day = end_day - timedelta(days=3650)
    from_date = start_day.isoformat()
    to_date = end_day.isoformat()
    query_lower = query.lower()
    papers: list[dict[str, Any]] = []
    cursor = 0
    max_pages = 10

    for _ in range(max_pages):
        resp = requests.get(
            f"{BIORXIV_DETAILS_URL}/{from_date}/{to_date}/{cursor}",
            timeout=30,
            headers={"User-Agent": USER_AGENT},
        )
        resp.raise_for_status()
        payload = resp.json()
        collection = payload.get("collection", [])
        if not collection:
            break

        for item in collection:
            title = _clean_text(item.get("title"))
            abstract = _clean_text(item.get("abstract"))
            haystack = f"{title} {abstract}".lower()
            if query_lower not in haystack:
                continue
            doi = _clean_text(item.get("doi"))
            version = _clean_text(str(item.get("version")))
            pdf_url = (
                f"https://www.biorxiv.org/content/{doi}v{version}.full.pdf"
                if doi and version
                else ""
            )
            papers.append(
                {
                    "paper_id": f"doi:{doi}" if doi else None,
                    "title": title,
                    "authors": [
                        _clean_text(name)
                        for name in (item.get("authors") or "").split(";")
                        if _clean_text(name)
                    ],
                    "abstract": abstract,
                    "body_text": "",
                    "year": _to_year(_clean_text(item.get("date"))),
                    "venue": "bioRxiv",
                    "doi": doi or None,
                    "pdf_url": pdf_url,
                    "source_platforms": ["biorxiv"],
                    "raw_payload": item,
                }
            )
            if len(papers) >= limit:
                return papers

        cursor += len(collection)
    return papers


def _dedup_key(paper: dict[str, Any]) -> str:
    doi = _clean_text(paper.get("doi"))
    if doi:
        return f"doi:{doi.lower()}"
    arxiv_id = _clean_text(paper.get("arxiv_id"))
    if arxiv_id:
        return f"arxiv:{arxiv_id.lower()}"
    pubmed_id = _clean_text(paper.get("pubmed_id"))
    if pubmed_id:
        return f"pubmed:{pubmed_id}"
    title = _clean_text(paper.get("title")).lower()
    year = str(paper.get("year") or "")
    return f"titleyear:{title}|{year}"


def _merge_paper(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for key, value in incoming.items():
        if key == "source_platforms":
            merged_sources = set(merged.get("source_platforms", [])) | set(value or [])
            merged["source_platforms"] = sorted(merged_sources)
            continue
        if key == "raw_payload":
            raw_list = merged.get("raw_payloads", [])
            if isinstance(raw_list, list):
                raw_list.append(value)
                merged["raw_payloads"] = raw_list
            continue
        if key not in merged or merged.get(key) in (None, "", [], {}):
            merged[key] = value
        elif key == "abstract" and isinstance(value, str) and len(value) > len(str(merged[key])):
            merged[key] = value
        elif key == "citation_count" and isinstance(value, int):
            old_value = merged.get("citation_count")
            merged[key] = max(int(old_value or 0), value)
    return merged


def collect_multi_platform_papers(query: str, limit: int) -> list[dict[str, Any]]:
    providers = [
        ("pubmed", search_pubmed_papers),
        ("biorxiv", search_biorxiv_papers),
        ("semanticscholar", search_semantic_scholar_papers),
    ]
    merged: dict[str, dict[str, Any]] = {}

    for provider_name, provider in providers:
        try:
            papers = provider(query, limit)
            LOGGER.info("Retrieved %d papers from %s.", len(papers), provider_name)
        except Exception as exc:
            LOGGER.warning("Failed to retrieve papers from %s: %s", provider_name, exc)
            continue
        for paper in papers:
            key = _dedup_key(paper)
            if key in merged:
                merged[key] = _merge_paper(merged[key], paper)
            else:
                created = dict(paper)
                created["raw_payloads"] = [paper.get("raw_payload")]
                created.pop("raw_payload", None)
                merged[key] = created

    all_papers = list(merged.values())
    all_papers.sort(
        key=lambda p: (
            int(p.get("year") or 0),
            int(p.get("citation_count") or 0),
            len(_clean_text(p.get("abstract"))),
        ),
        reverse=True,
    )
    return all_papers[:limit]


def persist_full_results(papers: Iterable[dict[str, Any]]) -> list[str]:
    cfg = PostgresConfig.from_env()
    stored_ids: list[str] = []
    with PaperContentStore(cfg) as store:
        store.ensure_table()
        for paper in papers:
            enriched = enrich_paper_with_body_text(paper)
            enriched["fetched_at"] = date.today().isoformat()
            paper_id = resolve_paper_id(enriched)
            payload_bytes = json.dumps(enriched, ensure_ascii=False).encode("utf-8")
            store.upsert_content(paper_id, payload_bytes)
            stored_ids.append(paper_id)
    return stored_ids


def run_pipeline(query: str, limit: int = 10) -> dict[str, Any]:
    """
    End-to-end pipeline:
    1) retrieve papers from PubMed/bioRxiv/Semantic Scholar
    2) collect abstracts for response
    3) persist full payloads to squai_table
    """
    papers = collect_multi_platform_papers(query, limit=limit)
    if not papers:
        return {"query": query, "count": 0, "abstracts": [], "stored_paper_ids": []}

    stored_ids = persist_full_results(papers)
    abstracts = [
        {
            "paper_id": resolve_paper_id(paper),
            "title": _clean_text(paper.get("title")),
            "abstract": _clean_text(paper.get("abstract")),
            "year": paper.get("year"),
            "source_platforms": paper.get("source_platforms", []),
        }
        for paper in papers
    ]
    return {
        "query": query,
        "count": len(abstracts),
        "abstracts": abstracts,
        "stored_paper_ids": stored_ids,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = ArgumentParser(
        description="Retrieve papers from PubMed/bioRxiv/S2 and persist full payload."
    )
    parser.add_argument("--query", required=True, help="User question or paper query.")
    parser.add_argument("--limit", type=int, default=10, help="Number of papers to return.")
    args = parser.parse_args()
    result = run_pipeline(args.query, limit=args.limit)
    print(json.dumps(result, ensure_ascii=False))
