import os
import json
import time
import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup
 
BASE_URL = "https://hacker-news.firebaseio.com/v0"

def fetch_json(endpoint):
    url = f"{BASE_URL}/{endpoint}"
    
    for _ in range(3):
        try:
            time.sleep(0.05)
            return requests.get(url, timeout=20).json()
        except Exception:
            time.sleep(2)
    return None


def get_item(item_id):
    return fetch_json(f"item/{item_id}.json")


def clean_text(raw_text):
    if not raw_text:
        return ""
    
    soup = BeautifulSoup(str(raw_text), "html.parser")
    return " ".join(soup.get_text(" ", strip=True).split())


def normalize_item(item):
    if not item or item.get("deleted") or item.get("dead"):
        return None

    item_type = item.get("type")

    if item_type == "story":
        raw_text = item.get("title", "")
    elif item_type == "comment":
        raw_text = item.get("text", "")
    else:
        return None

    text = clean_text(raw_text)
    if not text or item.get("time") is None:
        return None

    timestamp = datetime.fromtimestamp(item["time"], tz=timezone.utc)
    timestamp = timestamp.isoformat(timespec="seconds").replace("+00:00", "Z")

    item_id = str(item["id"])
    parent_id = item.get("parent")

    return {
        "id": item_id,
        "source": "hackernews",
        "type": item_type,
        "author": item.get("by"),
        "text": text,
        "timestamp": timestamp,
        "url": f"https://news.ycombinator.com/item?id={item_id}",
        "parent_id": str(parent_id) if parent_id is not None else None,
    }


def read_jsonl(file_path):
    if not os.path.exists(file_path):
        return []
    with open(file_path, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]

def write_jsonl(records, file_path):
    folder = os.path.dirname(file_path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

def append_jsonl(record, file_path):
    folder = os.path.dirname(file_path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
 
 
def make_producer(bootstrap_servers):
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
    )