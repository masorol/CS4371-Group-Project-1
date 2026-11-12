
# Python 3.11 with Debian 12 (Bookworm)
FROM python:3.11-slim-bookworm

# Install Java 17 for PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Determine Java location automatically
# Works with both Apple Silicon and Intel Macs
RUN JAVA_PATH=$(readlink -f $(which java)) && \
    JAVA_HOME_PATH=$(dirname $(dirname "$JAVA_PATH")) && \
    ln -s "$JAVA_HOME_PATH" /opt/java

ENV JAVA_HOME=/opt/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Download spaCy English NER model
RUN python -m spacy download en_core_web_sm

# Copy application code
COPY . .

# Start Spark with Kafka connector
CMD ["spark-submit", \
     "--packages", \
     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9", \
     "/app/spark_ner_streaming.py"]
