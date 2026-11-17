import os
import argparse
from collections import Counter
from dotenv import load_dotenv

from hn_common import read_jsonl

load_dotenv()

REQUIRED_FIELDS = ["id", "source", "type", "author", "text", "timestamp", "url", "parent_id"]


def main():
    parser = argparse.ArgumentParser(description="Check a Hacker News JSONL file.")
    parser.add_argument("--input", default=os.getenv("HN_HISTORICAL_INPUT", "data/hn_historical.jsonl"))
    parser.add_argument("--minimum-records", type=int, default=1000)
    args = parser.parse_args()

    records = read_jsonl(args.input)

    missing_fields = 0
    empty_text = 0
    for record in records:
        for field in REQUIRED_FIELDS:
            if field not in record:
                missing_fields += 1
        if not str(record.get("text", "")).strip():
            empty_text += 1

    duplicate_ids = len(records) - len({r.get("id") for r in records})
    unique_texts = len({r.get("text") for r in records})
    types = Counter(r.get("type") for r in records)

    print(f"Total records: {len(records)}")
    print(f"Missing fields: {missing_fields}")
    print(f"Duplicate ids: {duplicate_ids}")
    print(f"Empty text: {empty_text}")
    print(f"Unique texts: {unique_texts}")

    if missing_fields or duplicate_ids or empty_text or unique_texts < args.minimum_records:
        print("FAILED")
        raise SystemExit(1)

    print("PASSED")


if __name__ == "__main__":
    main()