"""Hacker News Kafka -> Spark Structured Streaming -> spaCy NER -> Kafka counts.

Logs each configuration/plan stage and actual micro-batch processing. Spark
DataFrame transformations are lazy, so setup messages do not imply that records
have been processed. Processing metrics are emitted when a batch finishes.
"""

import logging
import os
import re
import sys

import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json, struct, to_json, trim, udf, upper
from pyspark.sql.types import ArrayType, StringType, StructField, StructType


# 1. Logging and configuration -------------------------------------------------
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)-5s [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
log = logging.getLogger("hn-ner")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "topic1")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "topic2")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/checkpoints/hn-ner-topic1")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")
SPACY_MODEL = os.getenv("SPACY_MODEL", "en_core_web_sm")
SAMPLE_SIZE = max(0, int(os.getenv("LOG_SAMPLE_SIZE", "5")))

ALLOWED_LABELS = {"PERSON", "ORG", "GPE", "NORP", "EVENT", "PRODUCT"}

log.info("STEP 1/13: Configuration loaded")
log.info("Kafka broker: %s", KAFKA_BOOTSTRAP_SERVERS)
log.info("Input topic: %s | Output topic: %s", INPUT_TOPIC, OUTPUT_TOPIC)
log.info("Checkpoint: %s | Trigger: %s", CHECKPOINT_LOCATION, TRIGGER_INTERVAL)
log.info("spaCy model: %s | Entity labels: %s", SPACY_MODEL, sorted(ALLOWED_LABELS))
log.info("Per-batch example records: %d | Logging: %s", SAMPLE_SIZE, logging.getLevelName(log.level))


# 2. Input schema --------------------------------------------------------------
hn_schema = StructType([
    StructField("id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("type", StringType(), True),
    StructField("author", StringType(), True),
    StructField("text", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("url", StringType(), True),
    StructField("parent_id", StringType(), True),
])
log.info("STEP 2/13: Input JSON schema defined: %s", hn_schema.fieldNames())


# 3–4. spaCy model and extraction ----------------------------------------------
_nlp = None


def get_nlp():
    """Load the model once per Python worker, not once per input message."""
    global _nlp
    if _nlp is None:
        worker_log = logging.getLogger("hn-ner.worker")
        worker_log.info("STEP 3/13: Loading spaCy model '%s' in Python worker", SPACY_MODEL)
        _nlp = spacy.load(SPACY_MODEL)
        worker_log.info("STEP 3/13: spaCy model loaded successfully")
    return _nlp


def extract_entities(text):
    """Return one normalized entity name for each recognized mention."""
    if text is None or not text.strip():
        return []

    doc = get_nlp()(text)
    entities = []
    for ent in doc.ents:
        if ent.label_ not in ALLOWED_LABELS:
            continue
        entity = re.sub(r"\s+", " ", ent.text).strip()
        entity = re.sub(r"(?i)['’]s$", "", entity).strip()
        entity = entity.strip(" \t\n\r.,;:!?\"'()[]{}")
        if entity:
            entities.append(entity)

    # DEBUG logs are per document and can be very verbose.
    logging.getLogger("hn-ner.worker").debug(
        "STEP 4/13: Processed text (length=%d), entity mentions=%d",
        len(text), len(entities)
    )
    return entities


ner_udf = udf(extract_entities, ArrayType(StringType()))
log.info("STEP 4/13: NER UDF registered (will execute on Spark workers)")


# 5. Start Spark ---------------------------------------------------------------
log.info("STEP 5/13: Creating SparkSession")
spark = SparkSession.builder.appName("HackerNewsNamedEntityRecognition").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
log.info("STEP 5/13: SparkSession ready | version=%s | master=%s", spark.version, spark.sparkContext.master)


# 6. Kafka streaming source ----------------------------------------------------
log.info("STEP 6/13: Configuring Kafka streaming source: %s/%s", KAFKA_BOOTSTRAP_SERVERS, INPUT_TOPIC)
raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)
log.info("STEP 6/13: Kafka streaming source initialized (no records processed yet)")


# 7. Parse JSON ----------------------------------------------------------------
parsed_stream = (
    raw_stream.selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS json_string",
    )
    .select(col("kafka_key"), from_json(col("json_string"), hn_schema).alias("data"))
    .filter(col("data").isNotNull())
    .select(col("kafka_key"), "data.*")
)
log.info("STEP 7/13: JSON parsing transformation configured")


# 8. Filter messages -----------------------------------------------------------
messages = parsed_stream.filter(
    col("text").isNotNull()
    & (trim(col("text")) != "")
    & (upper(trim(col("text"))) != "TK")
    & col("type").isin("story", "comment")
)
log.info("STEP 8/13: Filtering configured (nonempty text, excluding TK, story/comment only)")


# 9. Apply spaCy NER -----------------------------------------------------------
entity_messages = messages.withColumn("entities", ner_udf(col("text")))
log.info("STEP 9/13: spaCy NER UDF added to streaming transformation plan")


# 10. Explode entity arrays ----------------------------------------------------
entity_rows = entity_messages.select(explode(col("entities")).alias("entity"))
log.info("STEP 10/13: Entity-array explosion configured")


# 11. Stateful cumulative aggregation -----------------------------------------
entity_counts = entity_rows.groupBy("entity").count()
log.info("STEP 11/13: Stateful entity counts configured (cumulative across batches)")


# 12. Write updated counts to Kafka --------------------------------------------
def write_to_kafka(batch_df, batch_id):
    """Publish updated cumulative counts; retry failures through Spark."""
    log.info("STEP 12/13: Micro-batch %s started", batch_id)
    batch_df.persist()
    try:
        updated_entities = batch_df.count()
        log.info("Batch %s: updated entity rows=%d", batch_id, updated_entities)
        if updated_entities == 0:
            log.info("Batch %s: no updated entity counts; skipping Kafka write", batch_id)
            return

        if SAMPLE_SIZE:
            for record in batch_df.orderBy(col("count").desc()).limit(SAMPLE_SIZE).collect():
                log.info(
                    "Batch %s sample: entity=%r cumulative_count=%d",
                    batch_id, record["entity"], record["count"]
                )

        kafka_output = batch_df.select(
            col("entity").cast("string").alias("key"),
            to_json(struct(col("entity"), col("count"))).alias("value"),
        )
        log.info("Batch %s: publishing %d cumulative updates to topic '%s'", batch_id, updated_entities, OUTPUT_TOPIC)
        kafka_output.write.format("kafka").option(
            "kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS
        ).option("topic", OUTPUT_TOPIC).save()
        log.info("Batch %s: Kafka write finished successfully", batch_id)
    except Exception:
        log.exception("Batch %s: FAILED to publish entity counts", batch_id)
        raise
    finally:
        batch_df.unpersist()


# 13. Start and monitor the streaming query ------------------------------------
query = None
try:
    log.info("STEP 13/13: Starting streaming query | outputMode=update | interval=%s", TRIGGER_INTERVAL)
    query = (
        entity_counts.writeStream
        .outputMode("update")
        .foreachBatch(write_to_kafka)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    log.info("STEP 13/13: Streaming query started | id=%s", query.id)
    log.info("Waiting for records on '%s' (Spark runs indefinitely until stopped)", INPUT_TOPIC)

    last_logged_batch = None
    while query.isActive:
        query.awaitTermination(10)
        progress = query.lastProgress
        if progress is not None and progress.get("batchId") != last_logged_batch:
            last_logged_batch = progress["batchId"]
            log.info(
                "Streaming progress | batch=%s | input_rows=%s | input_rows_per_sec=%s | processed_rows_per_sec=%s | duration_ms=%s",
                progress.get("batchId"),
                progress.get("numInputRows"),
                progress.get("inputRowsPerSecond"),
                progress.get("processedRowsPerSecond"),
                progress.get("durationMs"),
            )
            for source in progress.get("sources", []):
                log.info("Source offsets | start=%s | end=%s", source.get("startOffset"), source.get("endOffset"))
            for state in progress.get("stateOperators", []):
                log.info("Aggregation state | total_keys=%s | updated_keys=%s", state.get("numRowsTotal"), state.get("numRowsUpdated"))
except KeyboardInterrupt:
    log.info("Shutdown requested by keyboard interrupt")
except Exception:
    log.exception("Streaming application failed")
    raise
finally:
    if query is not None and query.isActive:
        log.info("Stopping streaming query")
        query.stop()
    log.info("Stopping SparkSession")
    spark.stop()
    log.info("Spark application stopped")
