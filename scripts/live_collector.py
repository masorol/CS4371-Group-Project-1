import os
import json
import time

from dotenv import load_dotenv
from kafka import KafkaProducer, future, future

from hn_common import (
    fetch_json,
    get_item,
    normalize_item,
    read_jsonl,
    append_jsonl
)

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

TOPIC = os.getenv("KAFKA_TOPIC", "topic1")

POLL_SECONDS = int(
    os.getenv("HN_POLL_SECONDS", "15")
)

LIVE_OUTPUT = "data/hn_live.jsonl"

STATE_FILE = "data/live_seen_ids.json"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,

        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False
        ).encode("utf-8"),

        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=10,
        linger_ms=10,
        max_block_ms=30000
    )


def load_seen_ids():
    seen = set()
    historical = read_jsonl("data/hn_historical.jsonl")

    for record in historical:
        seen.add(str(record["id"]))

    if os.path.exists(STATE_FILE):
        with open(
            STATE_FILE,
            "r",
            encoding="utf-8"
        ) as file:
            saved_ids = json.load(file)
            seen.update(str(item_id) for item_id in saved_ids)
    return seen


def save_seen_ids(seen):
    with open(
        STATE_FILE,
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(sorted(seen), file)


def publish_record(producer, record):
    future = producer.send(
        TOPIC,
        key=record["id"],
        value=record
    )
    future.get(timeout=30)


def run_live_collector():
    print("Starting live Hacker News collector...")
    print(f"Kafka broker: {BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {TOPIC}")
    print(f"Polling interval: {POLL_SECONDS} seconds")

    producer = create_producer()
    seen_ids = load_seen_ids()

    print(f"Previously processed IDs: {len(seen_ids)}")
    print("Press CTRL+C to stop.\n")

    try:
        while True:
            published_this_cycle = 0

            try:
                updates = fetch_json("updates.json") or {}
                updated_ids = updates.get("items", [])

                new_story_ids = fetch_json(
                    "newstories.json"
                ) or []

                candidate_ids = list(
                    dict.fromkeys(updated_ids + new_story_ids)
                )

                for item_id in candidate_ids:
                    item_id = str(item_id)

                    if item_id in seen_ids:
                        continue

                    try:
                        item = get_item(item_id)
                        record = normalize_item(item)

                        if not record:
                            seen_ids.add(item_id)
                            continue

                        publish_record(producer, record)
                        append_jsonl(record, LIVE_OUTPUT)
                        seen_ids.add(item_id)
                        published_this_cycle += 1

                    except Exception as error:
                        print(
                            f"Could not process item "
                            f"{item_id}: {error}"
                        )

                save_seen_ids(seen_ids)

                print(
                    f"Cycle complete | "
                    f"Published: {published_this_cycle} | "
                    f"Tracked IDs: {len(seen_ids)}"
                )

            except Exception as error:
                print(f"Live collection error: {error}")

            time.sleep(POLL_SECONDS)

    except KeyboardInterrupt:
        print("\nStopping live collector...")

    finally:
        producer.flush()
        producer.close()
        save_seen_ids(seen_ids)
        print("Live collector stopped.")


if __name__ == "__main__":
    run_live_collector()