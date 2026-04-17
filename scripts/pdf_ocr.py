#!/usr/bin/env python3
"""
Extract OCR text from images embedded in PDF files.
"""

from __future__ import annotations

import hashlib
import io
import time
from typing import Any


def _load_ocr_dependencies() -> tuple[Any, Any, Any]:
    try:
        import fitz  # type: ignore
        from PIL import Image  # type: ignore
        import pytesseract  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "OCR dependencies are missing. Install pymupdf, Pillow, and pytesseract."
        ) from exc
    return fitz, Image, pytesseract


def extract_ocr_from_pdf_bytes(
    pdf_bytes: bytes,
    *,
    lang: str = "chi_sim+eng",
    max_pages: int = 20,
    max_images_per_page: int = 8,
    min_text_length: int = 6,
) -> dict[str, Any]:
    """
    Parse embedded PDF images and extract OCR text per image.
    """
    fitz, Image, pytesseract = _load_ocr_dependencies()
    started_at = time.perf_counter()

    images: list[dict[str, Any]] = []
    text_chunks: list[str] = []
    total_images = 0
    processed_images = 0

    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        page_count = min(len(doc), max_pages)
        for page_no in range(page_count):
            page = doc[page_no]
            page_images = page.get_images(full=True)
            if not page_images:
                continue

            for image_index, image_info in enumerate(
                page_images[:max_images_per_page], start=1
            ):
                total_images += 1
                xref = image_info[0]
                extracted = doc.extract_image(xref)
                raw_image = extracted.get("image")
                if not raw_image:
                    continue

                try:
                    with Image.open(io.BytesIO(raw_image)) as image:
                        image = image.convert("RGB")
                        width, height = image.size
                        ocr_text = pytesseract.image_to_string(
                            image, lang=lang, config="--psm 6"
                        ).strip()
                except Exception:
                    continue

                processed_images += 1
                if len(ocr_text) < min_text_length:
                    continue

                text_chunks.append(ocr_text)
                images.append(
                    {
                        "page": page_no + 1,
                        "image_index": image_index,
                        "ocr_text": ocr_text,
                        "sha256": hashlib.sha256(raw_image).hexdigest(),
                        "width": width,
                        "height": height,
                        "ext": extracted.get("ext", ""),
                    }
                )

    duration_ms = int((time.perf_counter() - started_at) * 1000)
    return {
        "combined_text": "\n".join(text_chunks).strip(),
        "images": images,
        "meta": {
            "ocr_engine": "tesseract",
            "ocr_lang": lang,
            "max_pages": max_pages,
            "max_images_per_page": max_images_per_page,
            "min_text_length": min_text_length,
            "total_images": total_images,
            "processed_images": processed_images,
            "retained_images": len(images),
            "duration_ms": duration_ms,
        },
    }
