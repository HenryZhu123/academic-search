#!/usr/bin/env python3
"""
Fetch and enrich paper text from PDF URLs.
"""

from __future__ import annotations

import io
import logging
from typing import Any, Mapping

import requests
from pypdf import PdfReader

from pdf_ocr import extract_ocr_from_pdf_bytes


LOGGER = logging.getLogger(__name__)


def download_pdf_bytes(pdf_url: str, timeout_seconds: int = 30) -> bytes:
    response = requests.get(pdf_url, timeout=timeout_seconds)
    response.raise_for_status()
    return response.content


def fetch_body_text_from_pdf_bytes(pdf_bytes: bytes, max_chars: int = 300_000) -> str:
    """
    Extract plain text from all pages in PDF bytes.
    """
    reader = PdfReader(io.BytesIO(pdf_bytes))
    chunks: list[str] = []
    total_chars = 0
    for page in reader.pages:
        text = page.extract_text() or ""
        if not text:
            continue
        chunks.append(text)
        total_chars += len(text)
        if total_chars >= max_chars:
            break
    return "\n".join(chunks)[:max_chars]


def fetch_body_text_from_pdf_url(
    pdf_url: str,
    timeout_seconds: int = 30,
    max_chars: int = 300_000,
) -> str:
    """
    Download PDF and extract plain text from all pages.
    """
    pdf_bytes = download_pdf_bytes(pdf_url, timeout_seconds=timeout_seconds)
    return fetch_body_text_from_pdf_bytes(pdf_bytes, max_chars=max_chars)


def enrich_paper_with_body_text(paper: Mapping[str, Any]) -> dict[str, Any]:
    """
    Ensure one paper has `body_text`.
    If pdf_url exists, try extracting body text + OCR content.
    """
    result = dict(paper)
    result.setdefault("ocr_text", "")
    result.setdefault("ocr_images", [])
    result.setdefault("ocr_meta", {})

    pdf_url = result.get("pdf_url")
    if not pdf_url:
        result.setdefault("body_text", "")
        return result

    try:
        pdf_bytes = download_pdf_bytes(str(pdf_url))
    except Exception as exc:
        LOGGER.warning("Failed to download PDF from %s: %s", pdf_url, exc)
        result.setdefault("body_text", "")
        result["ocr_meta"] = {"error": "pdf_download_failed", "detail": str(exc)}
        return result

    if not result.get("body_text"):
        try:
            result["body_text"] = fetch_body_text_from_pdf_bytes(pdf_bytes)
        except Exception as exc:
            LOGGER.warning("Failed to parse body_text from %s: %s", pdf_url, exc)
            result["body_text"] = ""

    try:
        ocr_payload = extract_ocr_from_pdf_bytes(pdf_bytes)
        result["ocr_text"] = str(ocr_payload.get("combined_text") or "")
        result["ocr_images"] = list(ocr_payload.get("images") or [])
        result["ocr_meta"] = dict(ocr_payload.get("meta") or {})
    except Exception as exc:
        LOGGER.warning("Failed to run OCR for %s: %s", pdf_url, exc)
        result["ocr_text"] = ""
        result["ocr_images"] = []
        result["ocr_meta"] = {"error": "ocr_failed", "detail": str(exc)}

    return result
