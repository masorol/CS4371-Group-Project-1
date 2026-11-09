import os
import argparse

from dotenv import load_dotenv

from hn_common import (
    fetch_json,
    get_item,
    normalize_item,
    write_jsonl
)

load_dotenv()

OUTPUT_FILE = "data/hn_historical.jsonl"

TARGET_RECORDS = int(
    os.getenv("HN_TARGET_RECORDS", "1000")
)

MAX_STORIES_TO_SCAN = int(
    os.getenv("HN_MAX_STORIES_TO_SCAN", "2500")
)

FEEDS = [
    "newstories.json",
    "topstories.json",
    "beststories.json",
    "askstories.json",
    "showstories.json"
]


def collect_historical(target_records):
    records = {}
    scanned_stories = 0

    print("Starting historical Hacker News collection...")
    print(f"Target: {target_records} usable records\n")

    for feed in FEEDS:
        if len(records) >= target_records:
            break
        print(f"Fetching feed: {feed}")
        
        story_ids = fetch_json(feed)
        
        if not story_ids:
            continue
        
        for story_id in story_ids:
            if len(records) >= target_records:
                break
            if scanned_stories >= MAX_STORIES_TO_SCAN:
                break

            scanned_stories += 1
            story = get_item(story_id)

            if not story:
                continue

            story_record = normalize_item(story)

            if story_record:
                records[story_record["id"]] = story_record

            comment_ids = story.get("kids", [])

            for comment_id in comment_ids:
                if len(records) >= target_records:
                    break

                if str(comment_id) in records:
                    continue

                comment = get_item(comment_id)

                if not comment:
                    continue

                comment_record = normalize_item(comment)

                if comment_record:

                    records[comment_record["id"]] = comment_record

            if scanned_stories % 25 == 0:

                print(
                    f"Scanned {scanned_stories} stories | "
                    f"Collected {len(records)} records"
                )

        if scanned_stories >= MAX_STORIES_TO_SCAN:
            break

    final_records = list(records.values())

    write_jsonl(final_records, OUTPUT_FILE)

    print("\nHistorical collection completed.")
    print(f"Stories scanned: {scanned_stories}")
    print(f"Usable unique records: {len(final_records)}")
    print(f"Saved to: {OUTPUT_FILE}")

    if len(final_records) < target_records:
        print(
            "\nWARNING: The target was not reached. "
            "Try increasing HN_MAX_STORIES_TO_SCAN."
        )

        return False

    print("\nSUCCESS: Historical collection target reached.")

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--target",
        type=int,
        default=TARGET_RECORDS
    )

    args = parser.parse_args()

    success = collect_historical(args.target)

    if not success:
        raise SystemExit(1)