from etd.worker import Worker
import requests
import unittest


class MockResponse:
    text = "REST api is running."


class TestWorkerClass():

    def test_version(self):
        expected_version = "0.0.1"
        worker = Worker()
        version = worker.get_version()
        assert version == expected_version

    def test_api(self, monkeypatch):

        def mock_get(*args, **kwargs):
            return MockResponse()

        # apply the monkeypatch for requests.get to mock_get
        monkeypatch.setattr(requests, "get", mock_get)
        expected_msg = "REST api is running."
        worker = Worker()
        msg = worker.call_api()
        assert msg == expected_msg

    def test_api_fail(self, monkeypatch):

        def mock_get(*args, **kwargs):
            return MockResponse()

        # apply the monkeypatch for requests.get to mock_get
        monkeypatch.setattr(requests, "get", mock_get)
        expected_msg = "REST api is NOT running."
        worker = Worker()
        msg = worker.call_api()
        assert msg != expected_msg

    @unittest.skip("Need to get sftp key in place")
    def test_send_to_dash(self):
        expected_resp = "success"
        worker = Worker()
        resp = worker.send_to_dash({"hello": "world"})
        assert resp == expected_resp
