import os
import time
import json
import html
import requests

from datetime import datetime, timezone
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://hacker-news.firebaseio.com/v0"

REQUEST_DELAY_SECONDS = float(
    os.getenv("HN_REQUEST_DELAY_SECONDS", "0.05")
)


def fetch_json(endpoint):
    url = f"{BASE_URL}/{endpoint}"

    for attempt in range(5):
        time.sleep(REQUEST_DELAY_SECONDS)

        try:
            response = requests.get(url, timeout=20)

            if response.status_code == 404:
                return None

            response.raise_for_status()
            return response.json()

        except requests.RequestException as error:
            if attempt == 4:
                print(f"API request failed: {url}")
                raise error

            wait_seconds = 2 ** attempt

            print(
                f"Request failed. Retrying in "
                f"{wait_seconds} seconds..."
            )

            time.sleep(wait_seconds)

    return None


def get_item(item_id):
    return fetch_json(f"item/{item_id}.json")


def clean_text(raw_text):
    if not raw_text:
        return ""

    decoded = html.unescape(str(raw_text))
    soup = BeautifulSoup(decoded, "html.parser")
    cleaned = soup.get_text(" ", strip=True)
    cleaned = " ".join(cleaned.split())
    return cleaned.strip()


def normalize_item(item):
    if not item:
        return None

    if item.get("deleted") or item.get("dead"):
        return None

    item_type = item.get("type")

    if item_type == "story":
        title = item.get("title", "")
        body = item.get("text", "")
        raw_text = " ".join(
            part for part in [title, body] if part
        )

    elif item_type == "comment":
        raw_text = item.get("text", "")

    else:
        return None

    cleaned_text = clean_text(raw_text)

    if not cleaned_text:
        return None

    item_timestamp = item.get("time")

    if not item_timestamp:
        return None

    timestamp = datetime.fromtimestamp(
        item_timestamp,
        tz=timezone.utc
    ).isoformat(timespec="seconds")

    timestamp = timestamp.replace("+00:00", "Z")
    item_id = str(item["id"])
    parent_id = item.get("parent")

    if parent_id is not None:
        parent_id = str(parent_id)

    normalized = {
        "id": item_id,
        "source": "hackernews",
        "type": item_type,
        "author": item.get("by"),
        "text": cleaned_text,
        "timestamp": timestamp,
        "url": f"https://news.ycombinator.com/item?id={item_id}",
        "parent_id": parent_id
    }

    return normalized


def read_jsonl(file_path):
    records = []

    if not os.path.exists(file_path):
        return records

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records

def write_jsonl(records, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as file:
        for record in records:
            file.write(
                json.dumps(record, ensure_ascii=False) + "\n"
            )

def append_jsonl(record, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "a", encoding="utf-8") as file:
        file.write(
            json.dumps(record, ensure_ascii=False) + "\n"
        )