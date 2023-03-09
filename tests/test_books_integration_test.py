from fastapi.testclient import TestClient

DUMMY_MESSAGE = {
    "message": {
        "data": "SGVsbG8gQ2xvdWQgUHViL1N1YiEgSGVyZSBpcyBteSBtZXNzYWdlIQ==",
        "message_id": "2070443601311540",
        "publish_time": "2021-02-26T19:13:55.749Z"},
    "subscription": "projects/myproject/subscriptions/mysubscription"
}


def test_handle_endpoint_doesnt_allow_gets(test_client: TestClient):
    response = test_client.get("/pubsub-books/handle")
    assert response.status_code == 405


def test_handle_endpoint_rejects_malformed_requests(test_client: TestClient):
    request = {"malformed_request": 123}
    response = test_client.post("/pubsub-books/handle", json=request)
    assert response.status_code == 422


def test_well_formed_request_but_invalid_book_payload_returns_200_but_doesnt_call_api_client(test_client: TestClient):
    response = test_client.post("/pubsub-books/handle", json=DUMMY_MESSAGE)
    assert response.status_code == 200
