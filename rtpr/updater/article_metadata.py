"""Article identity and metadata extracted from RTPR's preserved body header."""
import hashlib
import json
import re
from urllib.parse import urlsplit


def article_id(url):
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.hostname != "rtpr.io" or parsed.port not in (None, 443) or not parsed.path.startswith("/a/"):
        raise ValueError("Unexpected RTPR article URL")
    return parsed.path


def version_id(row):
    values = [row["article_id"], row.get("article_published_at"), row.get("body_format", "md"), row["body_sha256"]]
    return hashlib.sha256(json.dumps(values, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def document_metadata(text):
    # Parse only the recognizable provider header, never guess from body prose.
    lines = text.splitlines()[:16]
    id_position = next((i for i, value in enumerate(lines) if value.startswith("Article ID:")), None)
    if id_position is None:
        return {"title": None, "header_recognized": False}
    header = lines[:id_position + 1]
    nonempty = [value.strip() for value in header if value.strip()]
    if len(nonempty) < 2:
        return {"title": None, "header_recognized": False, "raw_header_lines": header}
    title = re.sub(r"^#{1,6}\s+", "", nonempty[0])
    byline = next((value for value in nonempty[1:-1] if " · " in value), None)
    fields = byline.split(" · ") if byline else []
    result = {"title": title, "header_recognized": True, "raw_header_lines": header,
              "provider_article_id": nonempty[-1].partition(":")[2].strip().strip("`")}
    if fields:
        result.update(header_fields=fields, ticker_and_exchange=fields[0])
        if len(fields) >= 3:
            result.update(wire_source=fields[1], publication_display=" · ".join(fields[2:]))
    return result


def enrich_article(row):
    result = dict(row)
    result.setdefault("body_format", "md")
    result.setdefault("body_characters", len(result["text"]))
    metadata = document_metadata(result["text"])
    reference = result.get("reference", {})
    result.setdefault("document_metadata", metadata)
    result.setdefault("title", reference.get("title") or reference.get("headline") or metadata.get("title"))
    result.setdefault("title_source", "reference" if reference.get("title") or reference.get("headline") else "document_header" if metadata.get("title") else None)
    result.setdefault("ticker", reference.get("ticker"))
    result.setdefault("article_version_id", version_id(result))
    return result


def index_entry(row, path):
    return {"article_id": row["article_id"], "article_published_at": row.get("article_published_at"),
            "body_sha256": row["body_sha256"], "body_format": row.get("body_format", "md"),
            "title": row.get("title"), "text_available_at": row.get("receipt_utc"), "path": path}
