#!/usr/bin/env bash
# Person 3: one-time Elasticsearch/Kibana setup.
# Run AFTER `docker compose up -d`:   bash scripts/setup_kibana.sh
#
# 1. Waits for Elasticsearch and Kibana to be ready
# 2. Creates the "entity-counts" index (entity = keyword, count = number)
# 3. Creates the "Entity Counts" data view in Kibana
# 4. Imports the saved dashboard from config/kibana_dashboard.ndjson (if it exists)

ES="${ES_URL:-http://localhost:9200}"
KIBANA="${KIBANA_URL:-http://localhost:5601}"
DASHBOARD_FILE="$(dirname "$0")/../config/kibana_dashboard.ndjson"

echo "Waiting for Elasticsearch at $ES ..."
until curl -s "$ES" > /dev/null; do sleep 5; done
echo "  Elasticsearch is up."

echo "Waiting for Kibana at $KIBANA (can take 1-2 minutes) ..."
until curl -s "$KIBANA/api/status" | grep -q '"level":"available"'; do sleep 5; done
echo "  Kibana is up."

echo "Creating index 'entity-counts' ..."
curl -s -X PUT "$ES/entity-counts" -H "Content-Type: application/json" -d '{
  "mappings": {
    "properties": {
      "entity":     { "type": "keyword" },
      "count":      { "type": "long" },
      "@timestamp": { "type": "date" }
    }
  }
}'
echo

echo "Creating Kibana data view 'Entity Counts' ..."
curl -s -X POST "$KIBANA/api/data_views/data_view" \
  -H "kbn-xsrf: true" -H "Content-Type: application/json" -d '{
  "data_view": { "id": "entity-counts", "title": "entity-counts", "name": "Entity Counts" }
}'
echo

if [ -f "$DASHBOARD_FILE" ]; then
  echo "Importing dashboard from $DASHBOARD_FILE ..."
  curl -s -X POST "$KIBANA/api/saved_objects/_import?overwrite=true" \
    -H "kbn-xsrf: true" --form file=@"$DASHBOARD_FILE"
  echo
else
  echo "No saved dashboard found at config/kibana_dashboard.ndjson (build it in Kibana, then export it there)."
fi

echo "Done. Open Kibana: $KIBANA"
