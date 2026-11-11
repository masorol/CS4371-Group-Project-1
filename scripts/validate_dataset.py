from collections import Counter

from hn_common import read_jsonl

FILE_PATH = "data/hn_historical.jsonl"

REQUIRED_FIELDS = [
    "id",
    "source",
    "type",
    "author",
    "text",
    "timestamp",
    "url",
    "parent_id"
]


def validate_dataset():
    records = read_jsonl(FILE_PATH)

    print("\nDATASET VALIDATION")
    print("=" * 50)

    print(f"Total records: {len(records)}")

    if not records:
        print("ERROR: Dataset is empty.")
        return False

    ids = [record["id"] for record in records]
    duplicate_count = len(ids) - len(set(ids))
    print(f"Duplicate IDs: {duplicate_count}")

    type_counts = Counter(
        record["type"] for record in records
    )

    print("\nRecord types:")
    for record_type, count in type_counts.items():
        print(f"{record_type}: {count}")

    missing_fields = 0
    empty_text = 0

    for record in records:
        for field in REQUIRED_FIELDS:

            if field not in record:
                missing_fields += 1

        if not record.get("text", "").strip():
            empty_text += 1

    print(f"\nMissing required fields: {missing_fields}")
    print(f"Empty text records: {empty_text}")

    valid = (
        duplicate_count == 0
        and missing_fields == 0
        and empty_text == 0
    )

    print("\nRESULT:")

    if valid:
        print("PASS: Dataset structure is valid.")
    else:
        print("FAIL: Dataset needs attention.")

    if len(records) >= 1000:
        print("PASS: At least 1,000 records collected.")
    else:
        print("WARNING: Fewer than 1,000 records.")

    return valid


if __name__ == "__main__":
    validate_dataset()