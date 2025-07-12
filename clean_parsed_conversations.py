"""Clean parsed_conversations.json for ChromaDB ingestion.

Usage (from the Desktop directory):
    python clean_parsed_conversations.py

The script will read `parsed_conversations.json` in the same directory and
write the cleaned messages to `parsed_conversations_cleaned.json`.

Cleaning rules
--------------
1. If `timestamp` is null, use `metadata.conversation_create_time` instead.
2. Any numeric timestamp (int or float) is converted to ISO-8601 (UTC) format.
3. Messages whose `content` is an empty string are removed.
"""
from __future__ import annotations

import json
import pathlib
from datetime import datetime, timezone
from typing import Any, List, Dict

INPUT_FILE = pathlib.Path(__file__).with_name("parsed_conversations.json")
OUTPUT_FILE = pathlib.Path(__file__).with_name("parsed_conversations_cleaned.json")


def numeric_ts_to_iso(ts: float | int) -> str:
    """Convert a Unix timestamp to an ISO-8601 string in UTC ("YYYY-MM-DDTHH:MM:SSZ")."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clean_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Apply the cleaning rules and return a new list of cleaned messages."""
    cleaned: List[Dict[str, Any]] = []

    for msg in messages:
        # 1. Skip messages with empty content.
        if msg.get("content", "") == "":
            continue

        # 2. Fix timestamp when necessary.
        ts = msg.get("timestamp")
        if ts in (None, "", "null"):
            ts = msg.get("metadata", {}).get("conversation_create_time")

        # Convert numeric timestamp to ISO-8601 (UTC).
        if isinstance(ts, (int, float)):
            ts_iso = numeric_ts_to_iso(ts)
            msg["timestamp"] = ts_iso
        # If it's already a string (maybe ISO) leave as is. If still None, drop the field.
        elif ts is None:
            msg.pop("timestamp", None)  # remove null timestamp completely for cleanliness

        cleaned.append(msg)

    return cleaned


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    with INPUT_FILE.open("r", encoding="utf-8") as f:
        messages = json.load(f)

    cleaned_messages = clean_messages(messages)

    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(cleaned_messages, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(cleaned_messages):,} cleaned messages to {OUTPUT_FILE.name}")


if __name__ == "__main__":
    main()
