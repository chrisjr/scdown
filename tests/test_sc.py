from scdown.sc import RequestDB


def test_request_store():
    db = RequestDB(db_name="test")
    assert db.get("/test") is None
    a = {"a": "test"}
    db.set("/test", a)
    assert db.get("/test") == a
    db.client.drop_database("test")
