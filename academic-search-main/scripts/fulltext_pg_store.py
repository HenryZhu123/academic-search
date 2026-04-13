#!/usr/bin/env python3
"""
Persist paper full-text payloads into PostgreSQL.

This module is intentionally standalone so it can be plugged into the existing
academic-search workflow without changing current retrieval logic.
"""

from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

import psycopg2
from psycopg2.extensions import connection as PgConnection


LOGGER = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS squai_table (
    paper_id VARCHAR PRIMARY KEY,
    paper_content BYTEA NOT NULL
);
"""

UPSERT_SQL = """
INSERT INTO squai_table (paper_id, paper_content)
VALUES (%s, %s)
ON CONFLICT (paper_id)
DO UPDATE SET paper_content = EXCLUDED.paper_content;
"""


@dataclass(frozen=True)
class PostgresConfig:
    dbname: str = "squai_db"
    user: str = "squai_user"
    password: str = "squai123"
    host: str = "127.0.0.1"
    port: int = 5432

    @classmethod
    def from_env(cls) -> "PostgresConfig":
        return cls(
            dbname=os.getenv("SQUAI_DB_NAME", cls.dbname),
            user=os.getenv("SQUAI_DB_USER", cls.user),
            password=os.getenv("SQUAI_DB_PASSWORD", cls.password),
            host=os.getenv("SQUAI_DB_HOST", cls.host),
            port=int(os.getenv("SQUAI_DB_PORT", str(cls.port))),
        )


class PaperContentStore:
    """
    PostgreSQL writer with table bootstrap + idempotent upsert.
    """

    def __init__(self, config: PostgresConfig):
        self._config = config
        self._conn: PgConnection | None = None

    def connect(self) -> None:
        if self._conn is not None:
            return
        self._conn = psycopg2.connect(
            dbname=self._config.dbname,
            user=self._config.user,
            password=self._config.password,
            host=self._config.host,
            port=self._config.port,
        )
        LOGGER.info(
            "Connected to PostgreSQL %s@%s:%s/%s",
            self._config.user,
            self._config.host,
            self._config.port,
            self._config.dbname,
        )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            LOGGER.info("PostgreSQL connection closed.")

    def __enter__(self) -> "PaperContentStore":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def ensure_table(self) -> None:
        """
        Create target table if it does not exist.
        """
        self._require_connection()
        assert self._conn is not None
        try:
            with self._conn.cursor() as cur:
                cur.execute(CREATE_TABLE_SQL)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            LOGGER.exception("Failed to create table squai_table.")
            raise

    def upsert_content(self, paper_id: str, paper_content: bytes) -> None:
        """
        Upsert one paper payload in a transaction.
        """
        self._require_connection()
        assert self._conn is not None
        try:
            with self._conn.cursor() as cur:
                cur.execute(UPSERT_SQL, (paper_id, psycopg2.Binary(paper_content)))
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            LOGGER.exception("Upsert failed for paper_id=%s", paper_id)
            raise

    def upsert_paper(self, paper: Mapping[str, Any]) -> str:
        """
        Extract title/abstract/body_text, pack to bytes, then upsert.
        Returns the resolved paper_id.
        """
        paper_id = resolve_paper_id(paper)
        text_bytes = build_content_bytes(paper)
        self.upsert_content(paper_id, text_bytes)
        return paper_id

    def upsert_many(self, papers: Iterable[Mapping[str, Any]]) -> Sequence[str]:
        """
        Upsert multiple papers in one transaction.
        """
        self._require_connection()
        assert self._conn is not None
        ids: list[str] = []
        try:
            with self._conn.cursor() as cur:
                for paper in papers:
                    paper_id = resolve_paper_id(paper)
                    text_bytes = build_content_bytes(paper)
                    cur.execute(UPSERT_SQL, (paper_id, psycopg2.Binary(text_bytes)))
                    ids.append(paper_id)
            self._conn.commit()
            return ids
        except Exception:
            self._conn.rollback()
            LOGGER.exception("Batch upsert failed.")
            raise

    def _require_connection(self) -> None:
        if self._conn is None:
            raise RuntimeError("Database is not connected. Call connect() first.")


def build_content_text(paper: Mapping[str, Any]) -> str:
    """
    Build text using the required format:
    标题：{title}\n摘要：{abstract}\n正文：{body_text}
    """
    title = str(paper.get("title") or "")
    abstract = str(paper.get("abstract") or "")
    body_text = str(
        paper.get("body_text")
        or paper.get("full_text")
        or paper.get("content")
        or ""
    )
    return f"标题：{title}\n摘要：{abstract}\n正文：{body_text}"


def build_content_bytes(paper: Mapping[str, Any]) -> bytes:
    return build_content_text(paper).encode("utf-8")


def resolve_paper_id(paper: Mapping[str, Any]) -> str:
    """
    Resolve deterministic paper_id.
    Priority: explicit paper_id -> DOI -> arXiv ID -> hash(title+year).
    """
    explicit = paper.get("paper_id")
    if explicit:
        return str(explicit)

    doi = paper.get("doi")
    if doi:
        return f"doi:{doi}"

    arxiv_id = paper.get("arxiv_id")
    if arxiv_id:
        return f"arxiv:{arxiv_id}"

    title = str(paper.get("title") or "").strip()
    year = str(paper.get("year") or "").strip()
    if not title:
        raise ValueError("Cannot resolve paper_id: missing paper_id/doi/arxiv_id/title.")
    digest = hashlib.sha256(f"{title}|{year}".encode("utf-8")).hexdigest()[:24]
    return f"titlehash:{digest}"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    sample_paper = {
        "paper_id": "demo-paper-001",
        "title": "Attention Is All You Need",
        "abstract": "The dominant sequence transduction models are based on RNNs.",
        "body_text": "Full body text goes here.",
    }

    cfg = PostgresConfig.from_env()
    with PaperContentStore(cfg) as store:
        store.ensure_table()
        pid = store.upsert_paper(sample_paper)
        LOGGER.info("Upsert succeeded for %s", pid)
