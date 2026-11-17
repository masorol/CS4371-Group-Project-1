##  Part 1 Real-Time Named Entity Recognition (NER) Pipeline: Kafka (topic1) -> PySpark -> SpaCy NER -> Entity Counting -> Kafka (topic2)

## Preliminary Steps (for running Spark outside Docker)

### Create and activate virtual environment

```bash
python -m venv .venv
```

#### Windows

```bash
.venv\Scripts\activate
```

#### Mac/Linux

```bash
source .venv/bin/activate
```

### Install dependencies

```bash
pip install -r requirements.txt
```

### Download spaCy English language model

```bash
python -m spacy download en_core_web_sm
```

## Kafka Bootstrap Server

In `spark_ner_streaming.py`, use the address appropriate to where Spark runs:

- **Inside Docker:** `KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")`
- **Outside Docker (on your Mac/PC):** `KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")`

The addresses must match the Kafka listeners configured in `docker-compose.yml`. `kafka` is the Compose service name; `hn-kafka` is the container name used in the commands below.

Run these commands from the project directory containing `docker-compose.yml`. Use **Terminal 1** for setup, publishing, and the live collector. Open **Terminal 2** and **Terminal 3** in step 6 to monitor the Kafka topics.
Steps 2, 6, 7, and 8 are from data-collection.md.

### 1. Stop and remove any running containers for a clean run

```bash
docker compose down -v --remove-orphans
```

### 2. Start Kafka and create topic1

```bash
docker compose up -d
docker exec hn-kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic topic1 \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 3. Create topic2

```bash
docker exec hn-kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic topic2 \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 4. Build and start the Spark container

```bash
docker compose up --build -d spark
```

### 5. Check whether Kafka and Spark are running

```bash
docker compose ps
```

### 6. Collect and check the dataset (about 1,500 records)

```bash
python scripts/collect_historical.py
python scripts/validate_dataset.py
```

### 7. Test Kafka (by reading messages in 2 terminals)

Open the 2 consumer terminals **before step 8**, so you can see the data arrive when you publish it. These commands continue listening until you press Ctrl+C.

**Terminal 2 — Consume raw messages from topic1:**

```bash
docker exec -it hn-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic topic1 \
  --from-beginning \
  --bootstrap-server localhost:9092
```

**Terminal 3 - Read and display messages from topic2:**

```bash
docker exec -it hn-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic topic2 \
  --from-beginning \
  --bootstrap-server localhost:9092
```

### 8. Send the dataset to topic1 (Note: be sure to run this only once or topic1 gets duplicates)

Run this in **Terminal 1**:

```bash
python scripts/publish_historical.py
```

You should see incoming JSON messages in **Terminal 2** and, after Spark processes them, entity-count JSON in **Terminal 3**.

### 9. Run the live collector (needs the historical file from step 6)

Run this in **Terminal 1**:

```bash
python scripts/live_collector.py
```

If the Spark application is working correctly, you should see named entity counts in **Terminal 3** similar to:

```json
{"entity":"Illiac IV","count":1}
{"entity":"SYSOP","count":1}
{"entity":"Sweden","count":1}
{"entity":"IPO","count":4}
```

This confirms that the Spark application is consuming messages from topic1, processing named entities, and publishing the results to topic2.

## Other Helpful Docker Commands

### Check Docker and Docker Compose versions

```bash
docker --version
docker compose version
```

### View startup logs

```bash
docker compose logs -f kafka
docker compose logs -f spark
docker compose logs -f spark | grep "hn-ner"
```

### Check if Kafka/Spark are running

```bash
docker compose ps
```
