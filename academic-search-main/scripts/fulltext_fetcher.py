#!/usr/bin/env python3
"""
Fetch and extract paper body_text from PDF URLs.
"""

from __future__ import annotations

import io
import logging
from typing import Any, Mapping

import requests
from pypdf import PdfReader


LOGGER = logging.getLogger(__name__)


def fetch_body_text_from_pdf_url(
    pdf_url: str,
    timeout_seconds: int = 30,
    max_chars: int = 300_000,
) -> str:
    """
    Download PDF and extract plain text from all pages.
    """
    response = requests.get(pdf_url, timeout=timeout_seconds)
    response.raise_for_status()

    reader = PdfReader(io.BytesIO(response.content))
    chunks: list[str] = []
    for page in reader.pages:
        chunks.append(page.extract_text() or "")
        if sum(len(c) for c in chunks) >= max_chars:
            break
    return "\n".join(chunks)[:max_chars]


def enrich_paper_with_body_text(paper: Mapping[str, Any]) -> dict[str, Any]:
    """
    Ensure one paper has `body_text`.
    If body_text is missing and pdf_url exists, try extracting from PDF.
    """
    result = dict(paper)
    if result.get("body_text"):
        return result

    pdf_url = result.get("pdf_url")
    if not pdf_url:
        result["body_text"] = ""
        return result

    try:
        result["body_text"] = fetch_body_text_from_pdf_url(str(pdf_url))
    except Exception as exc:
        LOGGER.warning("Failed to fetch body_text from %s: %s", pdf_url, exc)
        result["body_text"] = ""
    return result
