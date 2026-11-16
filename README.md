
# CS4371-Group-Project-1

## Setup Instructions
#### 1. Clone repository

#### 2. Navigate to project
cd CS4371-Group-Project-1

#### 3. Create virtual environment
python -m venv .venv

#### 4. Activate (Windows)
.venv\Scripts\activate

#### 5. Install dependencies
pip install -r requirements.txt

## Version Requirements

| Component | Version | Notes |
|-----------|---------|-------|
| Python | 3.11 | Recommended for Spark components |
| Java | 17 | Required for Person 2 and Person 4; not needed for the Kafka collector |
| PySpark | 3.5.9 | |
| Spark Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9` | Loaded via `spark.jars.packages` or `--packages` |
| Kafka | `apache/kafka:4.1.2` | Docker image from `docker-compose.yml` |
| Docker Desktop | Not pinned | Required to run Kafka |
| Elasticsearch, Logstash, Kibana | TBD | Person 3 |

### Spark Setup

Spark components (Person 2 and Person 4) also need the Spark dependencies:

pip install -r requirements-spark.txt

**Windows note:** Spark on Windows needs `winutils.exe` and `hadoop.dll`.

1. Download the Hadoop 3.3.6 build of both files from the [cdarlint/winutils](https://github.com/cdarlint/winutils) GitHub repo.
2. Place them in `C:\hadoop\bin`.
3. Set the `HADOOP_HOME` environment variable to `C:\hadoop`.
4. Add `C:\hadoop\bin` to `PATH`.

## Running Part 2: Text Classification

### Train the model

Open JupyterLab from the repo root:

```
jupyter lab
```

Then run `notebooks/01_agnews_training.ipynb` top to bottom. It will download AG News, train the classifier, and save the model to `models/agnews_lr_pipeline`. This takes about 10–15 minutes due to the two-stage hyperparameter search.

### Run batch predictions on Hacker News data

Run `notebooks/02_hn_predictions.ipynb` top to bottom. It loads the saved model, reads `data/hn_historical.jsonl`, cleans the messages, predicts a class for each one, and saves results to `data/hn_predictions.parquet`. It also evaluates predictions against 50 manually labeled messages and prints accuracy and F1.

This notebook requires `data/hn_historical.jsonl` from Person 1's collector.

### Live classification (streaming)

`spark/classify_stream.py` is designed for integration into Person 2's `spark_ner_streaming.py`. Configure it with environment variables:

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker address |
| `INPUT_TOPIC` | `topic1` | Topic to read messages from |
| `OUTPUT_TOPIC` | `topic2` | Topic to publish class counts to |
| `MODEL_PATH` | *(required)* | Path to saved PipelineModel |
| `CHECKPOINT_LOCATION` | *(required)* | Spark checkpoint directory |
| `TRIGGER_INTERVAL` | `10 seconds` | Processing interval |

## Project Overview

This project is split into two parts.

### Part 1: Real-Time Named Entity Recognition

A streaming pipeline that extracts and counts named entities from live text:

- **Kafka** ingests real-time messages from the collector.
- **PySpark Structured Streaming** consumes the messages and uses **spaCy** to extract named entities (people, organizations, locations), maintaining cumulative counts.
- Updated entity counts are published back to Kafka, read by **Logstash**, indexed in **Elasticsearch**, and visualized in **Kibana**.

### Part 2: Text Classification

A **Spark MLlib** text classification pipeline trained on the **AG News** dataset, then applied to the messages collected in Part 1, both in batch and as a live streaming classification.

## Team Responsibilities

### Person 1: Mia - Data Collection and Kafka

Owns the input pipeline that both project parts depend on.

- Set up Kafka and create `topic1` and `topic2`.
- Select a real-time source, preferably Hacker News or Bluesky because neither requires an API key.
- Write a Python collector that continuously retrieves text and publishes it to `topic1`.
- Save messages to a local file as a backup for Part 2.
- Verify at least 1,000 messages are collected.
- Provide instructions for starting the producer and Kafka.

**Deliverable:** Working producer script, Kafka configuration and a dataset of at least 1,000 messages.

### Person 2: Anika - Spark Streaming and NER

Owns the core streaming analytics component.

- Create a PySpark Structured Streaming application that consumes `topic1`.
- Use spaCy to extract named entities such as people, organizations, and locations.
- Clean entity names and maintain cumulative occurrence counts.
- Configure triggers, output mode and checkpointing.
- Publish entity-count updates to `topic2`.
- Work with Person 4 to integrate the second streaming query into the application.

**Deliverable:** Functional PySpark streaming script producing accurate cumulative entity counts.

### Person 3: Calantha - Elasticsearch, Logstash and Kibana

Owns Part 1 visualization and leads final evidence collection.

- Install and configure Elasticsearch, Logstash and Kibana.
- Configure Logstash to consume messages from `topic2`.
- Ensure updated cumulative counts replace previous counts instead of being summed.
- Build a Kibana bar chart displaying the 10 most frequent named entities.
- Capture dashboard screenshots at multiple intervals (ideally 15, 30, 45 and 60 minutes).
- Write the Part 1 results interpretation and coordinate the report.

**Deliverable:** Live dashboard, screenshots, configuration files and Part 1 results summary.

### Person 4: Salamot - Machine Learning and Text Classification

Owns Part 2, which can begin independently using AG News.

- Load AG News and report the number of articles in each class.
- Build the required MLlib pipeline: RegexTokenizer -> StopWordsRemover -> HashingTF/CountVectorizer -> IDF -> LogisticRegression.
- Train and save the PipelineModel.
- Load Person 1's collected messages and remove empty/duplicate records.
- Predict labels, save probabilities and provide five examples per class.
- Coordinate manual labeling of 50 randomly selected messages and calculate accuracy.
- Implement live classification and running counts per topic.

**Deliverable:** Trained and saved model, prediction files, evaluation results and live classification query.

## Data Contract

Kafka details set by Person 1:

| Setting | Value |
|---------|-------|
| Topic | `hn_raw` |
| Broker | `localhost:9092` |
| Message format | JSON |
| Message key | Hacker News item ID |
| Record types | `story`, `comment` |

**Note:** `hn_raw` is currently serving as `topic1` from the assignment. The team should confirm whether to rename it to `topic1` to match the assignment requirements.

### Message Fields

| Field | Description |
|-------|-------------|
| `id` | Hacker News item ID |
| `source` | Source of the message |
| `type` | Record type: `story` or `comment` |
| `author` | Author username |
| `text` | Message text |
| `timestamp` | Creation time, UTC ISO 8601 |
| `url` | URL of the item |
| `parent_id` | ID of the parent item |
