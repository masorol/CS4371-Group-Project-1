# CS4371-Group-Project-1

## Setup Instructions
### 1. Clone repository

### 2. Navigate to project
cd CS4371-Group-Project-1

### 3. Create virtual environment
python -m venv .venv

### 4. Activate
#### Windows
.venv\Scripts\activate

#### Mac/Linux
source .venv/bin/activate

### 5. Install dependencies
pip install -r requirements.txt

### 6. Download spaCy English language model
python -m spacy download en_core_web_sm

### Note: change the Kafka bootstrap server depending on where the Spark application runs
#### If running Spark inside a Docker container - use the Kafka service name and internal Docker port in spark_ner_streaming.py:
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

#### If running Spark outside Docker (directly on your local computer) - change the Kafka bootstrap server in spark_ner_streaming.py to:
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

## Steps to Run the Spark Application Inside a Docker Container (Recommended)

### Docker Commands
docker --version
docker compose version

### Delete the container and remove everything for a Clean Run
docker compose down
docker compose down -v --remove-orphans

### Build the Spark image
docker compose build --no-cache spark

### Start Kafka
docker compose up -d

### Check Kafka is running
docker compose ps

### To view its startup logs
docker compose logs -f kafka

### Create topic 1
docker exec hn-kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic topic1 \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

### Create topic 2
docker exec hn-kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic topic2 \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

### Verify the topic exists
docker exec hn-kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092

### Start Spark
docker compose up -d spark

### If we need to Build and Start the Spark container
### docker compose up --build -d spark

### Open 2 more Terminals: one to Publish and another to Read the message

### Test Kafka (by sending a message)
#### Terminal 1 — Publish a message
docker exec -it hn-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --topic topic1 \
  --bootstrap-server localhost:9092

### Example message
{"id":"50428106","source":"hackernews","type":"story","author":"boninferizi","text":"An SEO page checker that shows its scoring and admits its limits.Microsoft, Tesla, Google, Apple","timestamp":"2026-11-09T16:49:19Z","url":"https://news.ycombinator.com/item?id=50428106","parent_id":null}

### Read the message - open another Terminal window and run:
#### Terminal 2 — Consume messages from topic1
docker exec -it hn-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic topic1 \
  --from-beginning \
  --bootstrap-server localhost:9092

### Check if Spark is running
docker compose ps

### To view its startup logs:
docker compose logs -f spark
docker compose logs -f spark | grep "hn-ner"

### Read the message - open another Terminal window and look at Topic 2:
docker exec -it hn-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic topic2 \
  --from-beginning \
  --bootstrap-server localhost:9092

### If the Spark application is working correctly, you should see named entity counts similar to:
{"entity":"Microsoft","count":1}
{"entity":"Tesla","count":1}
{"entity":"Google","count":1}
{"entity":"Apple","count":1}

#### The actual entities and counts depend on spaCy's recognition and previously processed messages.
#### This confirms that the Spark application is consuming messages from topic1, processing named entities, and publishing results to topic2.

### Delete the container
docker compose down

### Delete the container and remove everything for a Clean Run
docker compose down
docker compose down -v --remove-orphans