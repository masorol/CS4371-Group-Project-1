import os
import json

from kafka import KafkaProducer
from dotenv import load_dotenv
from hn_common import read_jsonl

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

TOPIC = os.getenv("KAFKA_TOPIC", "hn_raw")

INPUT_FILE = "data/hn_historical.jsonl"


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


def publish_historical():
    records = read_jsonl(INPUT_FILE)

    if not records:
        print("ERROR: No historical records found.")
        return

    print(f"Publishing {len(records)} records to Kafka...")
    print(f"Topic: {TOPIC}")

    producer = create_producer()
    successful = 0
    failed = 0

    try:
        for record in records:
            try:
                future = producer.send(
                    TOPIC,
                    key=record["id"],
                    value=record
                )

                future.get(timeout=30)
                successful += 1

                if successful % 100 == 0:
                    print(
                        f"Published {successful} records..."
                    )

            except Exception as error:
                failed += 1

                print(
                    f"Failed to publish "
                    f"{record.get('id')}: {error}"
                )

        producer.flush()

    finally:
        producer.close()

    print("\nPUBLISHING RESULTS")
    print("=" * 40)

    print(f"Successful: {successful}")
    print(f"Failed: {failed}")

    if failed == 0:
        print("SUCCESS: All records published.")
    else:
        print("WARNING: Some records failed to publish.")


if __name__ == "__main__":
    publish_historical()