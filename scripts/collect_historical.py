import os
import argparse
from dotenv import load_dotenv

from hn_common import fetch_json, get_item, normalize_item, write_jsonl

load_dotenv()

FEEDS = [
    "newstories.json",
    "topstories.json",
    "beststories.json",
    "askstories.json",
    "showstories.json",
]


def main():
    parser = argparse.ArgumentParser(description="Collect HN stories and comments into a JSONL file.")
    parser.add_argument("--output", default=os.getenv("HN_HISTORICAL_OUTPUT", "data/hn_historical.jsonl"))
    parser.add_argument("--target", type=int, default=int(os.getenv("HN_TARGET_RECORDS", "1500")))
    args = parser.parse_args()

    records = {}

    for feed in FEEDS:
        story_ids = fetch_json(feed) or []

        for story_id in story_ids:
            if len(records) >= args.target:
                break

            story = get_item(story_id)
            if not story:
                continue

            record = normalize_item(story)
            if record:
                records[record["id"]] = record

            for comment_id in story.get("kids", []):
                if len(records) >= args.target:
                    break
                comment_record = normalize_item(get_item(comment_id))
                if comment_record:
                    records[comment_record["id"]] = comment_record

            print(f"Collected {len(records)} records")

        if len(records) >= args.target:
            break

    write_jsonl(list(records.values()), args.output)
    print(f"Saved {len(records)} records to {args.output}")


if __name__ == "__main__":
    main()