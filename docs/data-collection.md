## Part 1 Data Collection Pipeline: Hacker News -> Kafka

### 1. Start Kafka and create the topics

```
docker compose up -d
docker exec hn-kafka /opt/kafka/bin/kafka-topics.sh --create --topic topic1 --bootstrap-server localhost:9092
```

### 2. Collect and check the dataset (about 1,500 records)

```
python scripts/collect_historical.py
python scripts/validate_dataset.py
```

Use `--output`, `--target`, or `--input` to change the file or size.

### 3. Send the dataset to topic1 (Note: be sure to run this only once or topic1 gets duplicates)

```
python scripts/publish_historical.py
```

### 4. Run the live collector (needs the historical file from step 2)

```
python scripts/live_collector.py
```

Press Ctrl+C to stop. New records are also saved to `data/hn_live.jsonl`.

### Tests

```
python -m pytest
```

### Message format (topic1)

JSON with keys `id, source, type, author, text, timestamp, url, parent_id`.
The key of each Kafka message is the HN item id.
