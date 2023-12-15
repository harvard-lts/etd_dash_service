from etd.worker import Worker
import pytest
import requests
import shutil
import lxml.etree as ET
import os
import glob


class MockResponse:
    text = "REST api is running."


class MockDuplicateResponse:
    text = '{"has": "content"}'


class MockNoDuplicateResponse:
    text = '[]'


# create a directory if it does not exist
def create_directory(dir):
    if os.path.exists(dir) is False:
        os.makedirs(dir)


# delete directory if it exists
def delete_directory(dir):
    if os.path.exists(dir) is True:
        os.rmdir(dir)


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

    # test Worker Class rename_directory function
    def test_rename_directory_success(self):
        srcDir = "./tests/data/in/testDir"
        destDir = "./tests/data/out/testDir"
        # make sure outputDir does not exist
        delete_directory(destDir)
        create_directory(srcDir)

        worker = Worker()
        worker.rename_directory(srcDir, destDir)

        assert os.path.isdir(srcDir) is False
        assert os.path.isdir(destDir) is True
        # cleanup input and output dirs
        delete_directory(srcDir)
        delete_directory(destDir)

    def test_rename_directory_with_timestamp_success(self):
        srcDir = "./tests/data/in/testDir"
        destDir = "./tests/data/out/testDir"
        worker = Worker()
        timestamp = worker.get_timestamp()
        destDir = destDir + "_" + timestamp
        # make sure outputDir does not exist
        delete_directory(destDir)
        create_directory(srcDir)

        worker.rename_directory(srcDir, destDir)
        assert os.path.isdir(srcDir) is False
        assert os.path.isdir(destDir) is True
        # cleanup input and output dirs
        delete_directory(srcDir)
        delete_directory(destDir)

    # test rename_directory function failure.
    # failure occurs when src_dir does not exist
    def test_rename_directory_failure_src_not_exists(self):
        srcDir = "./tests/data/in/random_dir_name"
        destDir = "./tests/data/out/testDir"
        delete_directory(srcDir)
        delete_directory(destDir)
        worker = Worker()

        with pytest.raises(FileNotFoundError):
            worker.rename_directory(srcDir, destDir)

    # test rename_directory function failure.
    # failure occurs when dest_dir already exists
    def test_rename_directory_failure_dest_exists(self):
        srcDir = "./tests/data/in/testDir"
        destDir = "./tests/data/out/testDir"
        create_directory(srcDir)
        create_directory(destDir)
        worker = Worker()

        with pytest.raises(FileExistsError):
            worker.rename_directory(srcDir, destDir)
        # cleanup input and output dirs
        delete_directory(srcDir)
        delete_directory(destDir)

    def test_unzip(self):
        aipDir = "./tests/data/ziptest"
        outDir = "./tests/data/ziptest/out"
        create_directory(outDir)
        aipFile = "test.zip"
        aipPath = os.path.join(aipDir, aipFile)
        outPath = os.path.join(outDir, aipFile)
        shutil.copy(aipPath, outPath)
        os.chdir(outDir)

        # unzip the zip and test the title of the pdf
        worker = Worker()
        worker.sh(['unzip', aipFile])

        metsTitle = ("Bridging the Divide — Policy Prospects for Addressing "
                     "Regional Disparities in Affordable Housing Funding "
                     "Under the Massachusetts Community Preservation Act "
                     "(May 2023).pdf")

        titleStartswith = "Bridging"
        file_list = os.listdir(".")
        matching_files = [filename for filename in file_list if
                          filename.startswith(titleStartswith)]

        assert matching_files[0] == metsTitle

        os.remove(aipFile)

        # zip again to confirm zipping works
        zipWithArgs = ['zip', aipFile]
        for file in os.listdir('.'):
            zipWithArgs.append(file)
        worker.sh(zipWithArgs)

        assert os.path.exists(aipFile) is True

        [os.remove(f) for f in glob.glob("*") if
         os.path.isfile(f) and not f.endswith(".zip")]

        # unzip a second time to confirm title is still correct
        worker.sh(['unzip', aipFile])
        titleStartswith = "Bridging"
        file_list = os.listdir(".")
        matching_files = [filename for filename in file_list if
                          filename.startswith(titleStartswith)]

        assert matching_files[0] == metsTitle

        homeDir = os.path.expanduser("~")
        os.chdir(homeDir)
        # cleanup outDir and files
        if os.path.exists(outDir):  # pragma no-cover
            shutil.rmtree(outDir)  
