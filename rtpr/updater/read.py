"""Offline article lookups and optional joins; no additional stored table."""
from functools import lru_cache
import gzip
import itertools
import json
from pathlib import Path


class Reader:
    def __init__(self, root):
        self.root = Path(root).expanduser().resolve()
        self.manifest = json.loads((self.root / "manifest.json").read_text())
        with gzip.open(self.root / ".state/article_index.json.gz", "rt", encoding="utf-8") as handle:
            self.index = json.load(handle)

    @lru_cache(maxsize=4)
    def _day(self, relative):
        with gzip.open(self.root / relative, "rt", encoding="utf-8") as handle:
            return {row["article_version_id"]: row for row in map(json.loads, handle)}

    def article(self, article_version_id):
        info = self.index[article_version_id]
        return self._day(info["path"])[article_version_id]

    def events(self, day, with_articles=False):
        relative = "events/" + day + ".jsonl.gz"
        path = self.root / relative
        # Later updates only append: pin this reader to the committed row count
        # observed when it opened, even if the daily file is replaced meanwhile.
        count = self.manifest["files"].get(relative, {}).get("rows", 0)
        if not count:
            return
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in itertools.islice(handle, count):
                event = json.loads(line)
                if with_articles:
                    key = event.get("article_version_id")
                    yield {"event": event, "article": self.article(key) if key else None}
                else:
                    yield event
