from scripts.hn_common import (
    clean_text,
    normalize_item
)

def test_html_is_cleaned():
    raw = "Hello&#x27;s <p>world</p>"
    result = clean_text(raw)
    assert result == "Hello's world"

def test_story_normalization():
    item = {
        "id": 123,
        "type": "story",
        "by": "testuser",
        "title": "Example title",
        "time": 1794177346,
        "url": "https://example.com"
    }

    result = normalize_item(item)

    assert result["id"] == "123"
    assert result["source"] == "hackernews"
    assert result["type"] == "story"
    assert result["author"] == "testuser"
    assert result["text"] == "Example title"
    assert result["parent_id"] is None


def test_comment_normalization():
    item = {
        "id": 456,
        "type": "comment",
        "by": "commenter",
        "text": "This is a <p>comment</p>",
        "time": 1794177346,
        "parent": 123
    }

    result = normalize_item(item)

    assert result["id"] == "456"
    assert result["type"] == "comment"
    assert result["text"] == "This is a comment"
    assert result["parent_id"] == "123"


def test_deleted_item_is_ignored():
    item = {
        "id": 789,
        "type": "comment",
        "deleted": True
    }

    assert normalize_item(item) is None


def test_empty_comment_is_ignored():
    item = {
        "id": 790,
        "type": "comment",
        "text": "",
        "time": 1794177346
    }

    assert normalize_item(item) is None


def test_unsupported_item_is_ignored():
    item = {
        "id": 791,
        "type": "job",
        "text": "A job posting",
        "time": 1794177346
    }

    assert normalize_item(item) is None