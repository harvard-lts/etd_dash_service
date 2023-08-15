from etd.worker import Worker
import requests
import shutil
import lxml.etree as ET
import os


class MockResponse:
    text = "REST api is running."


class MockDuplicateResponse:
    text = '{"has": "content"}'


class MockNoDuplicateResponse:
    text = '[]'


class TestWorkerClass():

    def test_version(self):
        worker = Worker()
        expected_version = "0.0.1"
        version = worker.get_version()
        assert version == expected_version

    def test_api(self, monkeypatch):
        worker = Worker()

        def mock_get(*args, **kwargs):
            return MockResponse()

        # apply the monkeypatch for requests.get to mock_get
        monkeypatch.setattr(requests, "get", mock_get)
        expected_msg = "REST api is running."
        msg = worker.call_api()
        assert msg == expected_msg

    def test_api_fail(self, monkeypatch):
        worker = Worker()

        def mock_get(*args, **kwargs):
            return MockResponse()

        # apply the monkeypatch for requests.get to mock_get
        monkeypatch.setattr(requests, "get", mock_get)
        expected_msg = "REST api is NOT running."
        msg = worker.call_api()
        assert msg != expected_msg

    ''' # consider an integration test instead
    def test_send_to_dash(self, monkeypatch):
        worker = Worker()
        schoolFile = "submission_993578.zip"
        submission_incoming_path = "/home/etdadm/tests/data/incoming/gsd"
        in_path = "/home/etdadm/tests/data/in/proquest2023071720-993578-gsd"

        # def mock_aipFiles(self):
        #    aipFiles = []
        #    aipFiles.append(["gsd", "proquest2023071720-993578-gsd",
        #                    f'{in_path}/{schoolFile}'])
        #    return aipFiles

        # monkeypatch.setattr(worker, "send_to_dash", mock_aipFiles)

        def mock_get_files(self):
            shutil.copy(submission_incoming_path + "/" +
                        schoolFile, in_path)
            aipFiles = []
            aipFiles.append(["gsd", "proquest2023071720-993578-gsd",
                            f'{in_path}/{schoolFile}'])
            return aipFiles

        monkeypatch.setattr(worker, "send_to_dash", mock_get_files)

        def mock_filesDir(self):
            return '/home/etdadm/tests/files'

        monkeypatch.setattr(worker, "send_to_dash", mock_filesDir)

        expected_resp = True
        resp = worker.send_to_dash({"hello": "world"})
        assert resp == expected_resp'''

    def test_rewrite_mets(self):
        schoolCode = "gsd"
        worker = Worker()
        namespace_mapping = {"dim": "http://www.dspace.org/xmlns/dspace/dim"}
        aipDir = "./tests/data/in/proquest2023071720-993578-gsd"
        batch = "proquest2023071720-993578-gsd"
        metsBeforeFile = os.path.join(aipDir, "mets_before.xml")
        shutil.copy(metsBeforeFile, os.path.join(aipDir, "mets.xml"))
        metsFile = os.path.join(aipDir, "mets.xml")
        json_message = {}
        worker.rewrite_mets(aipDir, batch, schoolCode, json_message)
        # doc_before = ET.parse(os.path.join(aipDir, "mets.xml"))
        doc = ET.parse(metsFile)

        # this checks all differences between original and rewritten mets
        assert doc.xpath("//dim:field[text()='Design']",
                         namespaces=namespace_mapping)[0].\
            get('qualifier') is None

        assert doc.xpath("//dim:field[text()='2023' and @mdschema='dc']",
                         namespaces=namespace_mapping)[0].\
            get('element') == "date"
        assert doc.xpath("//dim:field[text()='2023' and @mdschema='dc']",
                         namespaces=namespace_mapping)[0]\
            .get('qualifier') == "created"

        assert doc.xpath("//dim:field[text()='2023-05']",
                         namespaces=namespace_mapping)[0].\
            get('element') == "date"
        assert doc.xpath("//dim:field[text()='2023-05']",
                         namespaces=namespace_mapping)[0].\
            get('mdschema') == "dc"
        assert doc.xpath("//dim:field[text()='2023-05']",
                         namespaces=namespace_mapping)[0].\
            get('qualifier') == "submitted"

        assert doc.xpath("//dim:field[@qualifier='level']",
                         namespaces=namespace_mapping)[0].\
            text == "Masters"

        assert int(json_message["identifier"]) == 30522803

        # test the exceptions with a bad mets.xml
        '''metsEmptyFile = os.path.join(aipDir, "mets_bad.xml")
        shutil.copy(metsEmptyFile, os.path.join(aipDir, "mets.xml"))
        metsFile = os.path.join(aipDir, "mets.xml")
        try:
            worker.rewrite_mets(aipDir, batch, schoolCode)
            treeMets = doc.parse(metsFile)
            rootMets = treeMets.getroot()
        except Exception as e:
            assert "Start tag expected" in str(e)

        os.remove(metsFile)
        try:
            resp = worker.rewrite_mets(aipDir, batch, schoolCode)
            assert resp == False
        except Exception as e:
            assert e is None'''

    def test_check_for_duplicates(self, monkeypatch):
        worker = Worker()
        # text = '{"has": "content"}'

        def mock_post(*args, **kwargs):
            return MockDuplicateResponse()

        # apply the monkeypatch for requests.get to mock_get
        monkeypatch.setattr(requests, "post", mock_post)
        msg = worker.check_for_duplicates(123)
        # since we are mocking a post response with content other than []
        # we expect that duplicate is true (otherwise would be empty [])
        assert msg is True

        def mock_post(*args, **kwargs):
            return MockNoDuplicateResponse()

        # apply the monkeypatch for requests.get to mock_get
        monkeypatch.setattr(requests, "post", mock_post)
        msg = worker.check_for_duplicates(123)
        # since we are mocking a post response with []
        # we expect that duplicate is false (b/c expected resp is empty [])
        assert msg is False
