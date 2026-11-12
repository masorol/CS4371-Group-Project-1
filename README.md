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
| Elasticsearch, Logstash, Kibana | 8.15.3 | Docker images from `docker-compose.yml` (Person 3) |

### Spark Setup

Spark components (Person 2 and Person 4) also need the Spark dependencies:

pip install -r requirements-spark.txt

**Windows note:** Spark on Windows needs `winutils.exe` and `hadoop.dll`.

1. Download the Hadoop 3.3.6 build of both files from the [cdarlint/winutils](https://github.com/cdarlint/winutils) GitHub repo.
2. Place them in `C:\hadoop\bin`.
3. Set the `HADOOP_HOME` environment variable to `C:\hadoop`.
4. Add `C:\hadoop\bin` to `PATH`.

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

| Setting        | Value               |
| -------------- | ------------------- |
| Topic          | `topic1`            |
| Broker         | `localhost:9092`    |
| Message format | JSON                |
| Message key    | Hacker News item ID |
| Record types   | `story`, `comment`  |

### Message Fields

| Field       | Description                       |
| ----------- | --------------------------------- |
| `id`        | Hacker News item ID               |
| `source`    | Source of the message             |
| `type`      | Record type: `story` or `comment` |
| `author`    | Author username                   |
| `text`      | Message text                      |
| `timestamp` | Creation time, UTC ISO 8601       |
| `url`       | URL of the item                   |
| `parent_id` | ID of the parent item             |

### Entity Counts (`topic2`)

Spark (Person 2) sends one message to `topic2` every time an entity's total changes. Logstash (Person 3) reads these.

| Field    | Description                                      | Example    |
| -------- | ------------------------------------------------ | ---------- |
| `entity` | The name that was found (person, company, place) | `"OpenAI"` |
| `count`  | Total times it has been seen **so far**          | `42`       |

Example message: `{"entity": "OpenAI", "count": 42}`

The count is a running total, not "new since last time". That's why Elasticsearch replaces the old number instead of adding to it.

## Part 1 Dashboard: Elasticsearch, Logstash and Kibana (Person 3)

This is the last step of Part 1. It takes the entity counts from Spark and shows them as a live bar chart of the 10 most common names.

```
collector → topic1 → Spark + spaCy → topic2 → Logstash → Elasticsearch → Kibana dashboard
```

- **Logstash** listens to `topic2` and passes each message to Elasticsearch.
- **Elasticsearch** stores one record per entity. When a new count arrives for the same entity, it **overwrites** the old one, so numbers are never double-counted.
- **Kibana** shows the "Top 10 Named Entities" bar chart and refreshes every 10 seconds.

### Files

| File | What it does |
| ---- | ------------ |
| `docker-compose.yml` | Starts Kafka, Spark, Elasticsearch, Logstash and Kibana |
| `config/logstash.conf` | Reads `topic2`, skips broken messages, saves each entity using its name as the ID (this is what makes counts replace instead of add up) |
| `scripts/setup_kibana.sh` | One-time setup: creates the `entity-counts` index, the Kibana data view, and loads the dashboard |
| `config/kibana_dashboard.ndjson` | The saved "Top 10 Named Entities" dashboard |

### How to run it

**1. Start everything.** Docker Desktop must be open and say "Engine running".

On a machine with 16 GB of RAM or more (or a GitHub Codespace), run the full pipeline:

```
docker compose up -d --build
```

On a laptop with 8 GB of RAM, leave Spark out or Docker may crash:

```
docker compose up -d kafka elasticsearch logstash kibana
```

**2. Wait 1–2 minutes.** Kibana is always the last one to start. If the page says "Kibana server is not ready yet" or "connection was reset", just wait and refresh.

**3. Run the setup script (only once per machine).** On Windows use Git Bash; on Mac, Linux or Codespaces use the normal terminal:

```
bash scripts/setup_kibana.sh
```

**4. Open the dashboard.** Go to http://localhost:5601, then ☰ menu → **Dashboards** → **Top 10 Named Entities**. Bars appear as soon as Spark starts sending counts. Before that, it says "No results found", which is normal.

**5. Stop everything when you're done** (this frees up memory):

```
docker compose down
```

### Checking that data is arriving

- See how many entities are stored: open http://localhost:9200/entity-counts/_count
- See the raw records: open http://localhost:9200/entity-counts/_search
- Watch Logstash receive messages: `docker logs -f logstash`

### Running in GitHub Codespaces

Use this if your laptop doesn't have enough memory.

1. On the GitHub repo page, click **Code → Codespaces → … → New with options**.
2. Choose the **4-core, 16 GB RAM** machine. The 2-core one is too small.
3. In the Codespace terminal, run `docker compose up -d --build`, then `bash scripts/setup_kibana.sh`.
4. Open the **Ports** tab, find port **5601**, and click the globe icon to open Kibana.
5. Before a long run, go to **GitHub → Settings → Codespaces** and set **Default idle timeout** to 240 minutes. Otherwise the Codespace shuts down after 30 minutes of no typing.
6. **Stop the Codespace when you're done** so you don't use up your free hours.

### Common problems

| Problem | Fix |
| ------- | --- |
| `failed to connect to the docker API` | Docker Desktop isn't open. Open it and wait for "Engine running". |
| Docker stuck on "Starting the Docker Engine…" | Open Task Manager, end every process with "Docker" in the name, run `wsl --shutdown`, then open Docker Desktop again. |
| Docker crashes after a few minutes | Not enough memory. Close other apps, leave Spark out (see above), or use a Codespace. |
| Kibana page won't load | Wait 1–2 minutes after starting. Kibana is slow to start. |
| Dashboard says "No results found" | No counts have arrived yet. Check that Spark is running (`docker logs spark-ner`) and the collector is sending to `topic1`. |
