# CS4371-Group-Project-1

## Setup Instructions
#### 1. Clone repository

#### 2. Navigate to project
cd CS4371-Group-Project-1

#### 3. Create virtual environment
python -m venv .venv

#### 4. Activate (Windows)
.venv\Scripts\activate

### Mac/Linux
source .venv/bin/activate

#### 5. Install dependencies
pip install -r requirements.txt

### 6. Download spaCy English language model
python -m spacy download en_core_web_sm


## Steps to Run the Spark application inside a Docker Container

### Docker Commands
docker --version
docker compose version

### Delete the container and remove everything for a Clean Run
docker compose down
docker compose down -v --remove-orphans

## Build spark image
docker compose build --no-cache spark

### Start Kafka
docker compose up -d

### Check Kafka is running
docker compose ps

### To view its startup logs
docker compose logs -f kafka

### Create Kafka topic1
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

### Start spark
docker compose up -d spark

### If we need to Build and Start the Spark container
### docker compose up --build -d spark

## Open a new Terminal to Publish and and another Terminal to Read the message to Test
### Test Kafka (by sending a message)
docker exec -it hn-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --topic topic1 \
  --bootstrap-server localhost:9092

### Example message
{"id":"50428106","source":"hackernews","type":"story","author":"boninferizi","text":"An SEO page checker that shows its scoring and admits its limits.Microsoft, Tesla, Google, Apple","timestamp":"2026-11-09T16:49:19Z","url":"https://news.ycombinator.com/item?id=50428106","parent_id":null}

### Read the message - Open a second Terminal window and run:
docker exec -it hn-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic topic1 \
  --from-beginning \
  --bootstrap-server localhost:9092

### Check if Spark is running
docker compose ps

### To view its startup logs
docker compose logs -f spark
docker compose logs -f spark | grep "hn-ner"

### Read the message - Open a another Terminal window and look at the Topic 2:
docker exec -it hn-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic topic2 \
  --from-beginning \
  --bootstrap-server localhost:9092

### Delete the container
docker compose down

### Delete the container and remove everything for a Clean Run
docker compose down
docker compose down -v --remove-orphans