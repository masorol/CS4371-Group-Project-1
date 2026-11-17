import os
import argparse
from dotenv import load_dotenv

from hn_common import read_jsonl, make_producer

load_dotenv()


def main():
    parser = argparse.ArgumentParser(description="Send a JSONL file to a Kafka topic.")
    parser.add_argument("--input", default=os.getenv("HN_HISTORICAL_INPUT", "data/hn_historical.jsonl"))
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "topic1"))
    args = parser.parse_args()

    records = read_jsonl(args.input)
    if not records:
        print(f"No records found in {args.input}")
        return

    producer = make_producer(args.bootstrap_servers)
    for record in records:
        producer.send(args.topic, key=record["id"], value=record)
    producer.flush()
    producer.close()

    print(f"Sent {len(records)} records to {args.topic}")


if __name__ == "__main__":
    main()