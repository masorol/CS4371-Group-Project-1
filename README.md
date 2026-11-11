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
