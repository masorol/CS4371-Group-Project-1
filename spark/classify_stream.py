# ----------------- Classify Streams

from pyspark.ml import PipelineModel
from pyspark.sql.functions import create_map, lit

# Person 2's code: 
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "topic1")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "topic2")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/checkpoints/hn-classify-topic1")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")
MODEL_PATH = os.getenv("MODEL_PATH", "/models/agnews_lr_pipeline")

# load the model with model path
model = PipelineModel.load(MODEL_PATH)

# create the label map
label_names = create_map([lit(x) for kv in {0:"World",1:"Sports",2:"Business",3:"Sci/Tech"}.items() for x in kv])

# predict messages from Person 2
predictions = model.transform(messages)
classified = predictions.withColumn(
    "predicted_class",
    label_names[col("prediction").cast("int")])
topic_counts = classified.groupBy("predicted_class").count()


# spark calls this function automatically 
def write_classifications_to_kafka(batch_df, batch_id):
    """Publish cumulative topic-class counts for a completed micro-batch to Kafka.

    Called by Spark's foreachBatch sink after each micro-batch completes.
    Converts the aggregated (predicted_class, count) DataFrame into Kafka
    messages keyed by class name, with a JSON payload carrying both fields,
    and writes them to OUTPUT_TOPIC so Logstash can update Elasticsearch.

    With only four possible classes, all four rows are published every batch
    regardless of whether a class appeared in the current batch. This keeps
    Elasticsearch counts consistent and avoids stale reads on the dashboard.

    Args:
        batch_df: A non-streaming DataFrame with columns predicted_class (str)
                  and count (long), representing cumulative counts across all
                  batches seen so far.
        batch_id: Integer identifier for the current micro-batch, used for
                  logging and idempotency.
    """
    log.info("Classification batch %s started", batch_id)
    batch_df.persist()
    try:
        updated_classes = batch_df.count()
        log.info("Batch %s: updated class rows=%d", batch_id, updated_classes)
        if updated_classes == 0:
            log.info("Batch %s: no updated class counts; skipping Kafka write", batch_id)
            return

        for record in batch_df.collect():
            log.info(
                "Batch %s: class=%r cumulative_count=%d",
                batch_id, record["predicted_class"], record["count"]
            )

        kafka_output = batch_df.select(
            col("predicted_class").cast("string").alias("key"),
            to_json(struct(
                col("predicted_class"),
                col("count")
            )).alias("value"),
        )
        log.info("Batch %s: publishing %d class counts to topic '%s'",
                 batch_id, updated_classes, OUTPUT_TOPIC)
        kafka_output.write.format("kafka").option(
            "kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS
        ).option("topic", OUTPUT_TOPIC).save()
        log.info("Batch %s: Kafka write finished successfully", batch_id)
    except Exception:
        log.exception("Batch %s: FAILED to publish class counts", batch_id)
        raise
    finally:
        batch_df.unpersist()

query2 = (
    topic_counts.writeStream
    .outputMode("complete")
    .foreachBatch(write_classifications_to_kafka)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)
log.info("Classification streaming query started | id=%s", query2.id)

# query2 runs alongside query1 (NER). Shutdown is handled by
# spark_ner_streaming.py which calls spark.streams.awaitAnyTermination()
# or query1.awaitTermination().

