from scripts.hn_common import clean_text, normalize_item


def test_html_is_cleaned():
    assert clean_text("Hello&#x27;s <p>world</p>") == "Hello's world"


def test_story_normalization():
    item = {"id": 123, "type": "story", "by": "testuser", "title": "Example title", "time": 1794177346}
    result = normalize_item(item)
    assert result["id"] == "123"
    assert result["source"] == "hackernews"
    assert result["type"] == "story"
    assert result["author"] == "testuser"
    assert result["text"] == "Example title"
    assert result["parent_id"] is None


def test_comment_normalization():
    item = {"id": 456, "type": "comment", "by": "commenter", "text": "This is a <p>comment</p>",
            "time": 1794177346, "parent": 123}
    result = normalize_item(item)
    assert result["type"] == "comment"
    assert result["text"] == "This is a comment"
    assert result["parent_id"] == "123"


def test_deleted_item_is_ignored():
    assert normalize_item({"id": 789, "type": "comment", "deleted": True}) is None


def test_empty_comment_is_ignored():
    assert normalize_item({"id": 790, "type": "comment", "text": "", "time": 1794177346}) is None


def test_unsupported_item_is_ignored():
    assert normalize_item({"id": 791, "type": "job", "text": "A job", "time": 1794177346}) is None


def test_story_uses_title_only():
    item = {"id": 999, "type": "story", "title": "Story title", "text": "Body", "time": 1794177346}
    assert normalize_item(item)["text"] == "Story title"


def test_story_without_title_is_ignored():
    item = {"id": 1000, "type": "story", "title": "", "text": "Only a body", "time": 1794177346}
    assert normalize_item(item) is None