import os
import time
import argparse
from dotenv import load_dotenv

from hn_common import fetch_json, get_item, normalize_item, read_jsonl, append_jsonl, make_producer

load_dotenv()


def main():
    parser = argparse.ArgumentParser(description="Poll Hacker News and send new records to Kafka.")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "topic1"))
    parser.add_argument("--poll-seconds", type=int, default=int(os.getenv("HN_POLL_SECONDS", "15")))
    parser.add_argument("--output", default=os.getenv("HN_LIVE_OUTPUT", "data/hn_live.jsonl"))
    parser.add_argument("--historical-input", default=os.getenv("HN_HISTORICAL_INPUT", "data/hn_historical.jsonl"))
    args = parser.parse_args()

    producer = make_producer(args.bootstrap_servers)

    seen_ids = set()
    for file_path in [args.historical_input, args.output]:
        for record in read_jsonl(file_path):
            seen_ids.add(record["id"])

    print("Live collector started. Press Ctrl+C to stop.")

    try:
        while True:
            updates = fetch_json("updates.json") or {}
            new_stories = fetch_json("newstories.json") or []
            candidate_ids = updates.get("items", []) + new_stories

            sent = 0
            for item_id in dict.fromkeys(candidate_ids):
                item_id = str(item_id)
                if item_id in seen_ids:
                    continue
                seen_ids.add(item_id)

                record = normalize_item(get_item(item_id))
                if record is None:
                    continue

                producer.send(args.topic, key=record["id"], value=record)
                append_jsonl(record, args.output)
                sent += 1

            producer.flush()
            print(f"Sent {sent} new records")
            time.sleep(args.poll_seconds)

    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()