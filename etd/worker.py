import requests
import logging
import os

# import lxml.etree as xmlTree
# import os, re, sys, shutil, xml.sax
import re
from datetime import datetime
# from glob import glob
# from shlex import quote

"""
This is a basic worker class.

Since: 2023-05-23
Author: cgoines
"""


class Worker():
    version = None
    logger = logging.getLogger('etd_dash')

    def __init__(self):
        self.version = os.getenv("APP_VERSION", "0.0.1")

    def get_version(self):
        return self.version

    def send_to_dash(self, message):
        # global dspace_instance, notifyJM
        # now = datetime.now()
        # dateTimeStamp = now.strftime('%Y%m%d%H')
        # dateTimeStamp = now.strftime('%Y%m%d%H')
        # logFile  = os.path.join(logDir, f"{jobCode}_{dateTimeStamp}.log")
        # notifyJM = notify('monitor+log', jobCode, logFile)
        # Let the Job Monitor know that the job has started
        # notifyJM.log('pass', f'Start Proquest to ETDs processing', verbose)
        # notifyJM.report('start')
        api_files = self.get_files()

        self.logger.debug(message)
        return "success"

    # this is call to the DASH healthcheck for integration testing
    def call_api(self):
        url = "https://dash.harvard.edu/rest/test"
        r = requests.get(url)
        self.logger.debug("In call api")
        return r.text

    def get_files(self):
        from xfer_files import xfer_files
        # global notifyJM
        dropboxServer = os.getenv("dropboxServer")
        dropboxUser = os.getenv("dropboxUser")
        homeDir = os.getenv("homeDir")
        privateKey = f'{homeDir}/.ssh/kant_id_rsa'
        aipPattern = '.+_(\\d+).zip'
        reAipPackage = re.compile(aipPattern)
        now = datetime.now()
        datestamp = now.strftime('%Y%m%d%H')
        dataDir = 'data'
        dataInDir = os.path.join(dataDir, "in")
        aipFiles = []

        # Connect to our Proquest dropbox
        try:
            xfer = xfer_files(dropboxServer, dropboxUser,
                              privateKey=privateKey)
            if xfer.error:
                xfer.close()
                # notifyJM.log('fail', xfer.error, verbose)
                self.logger.error(xfer.error)
                # notifyJM.report('stopped')
                self.logger.error('stopped')
                return False
        except Exception as e:
            # notifyJM.log('fail', f'Fail to connect to
            # {dropboxUser}@{dropboxServer}', verbose)
            self.logger.error(f'Fail to connect to \
                              {dropboxUser}@{dropboxServer}: {e}')

        # Get a list of incoming school directories
        schoolDirs = xfer.listdir('incoming')
        if xfer.error:
            xfer.close()
            # notifyJM.log('fail', xfer.error, verbose)
            self.logger.error(xfer.error)
            # notifyJM.report('stopped')
            self.logger.error('stopped')
            return False

        # Loop on directory list looking for incoming AIP files
        for schoolCode in schoolDirs:

            # Skip backlog
            if schoolCode == 'proquest_backlog':
                continue

            if xfer.isdir(f'incoming/{schoolCode}'):

                schoolFiles = xfer.listdir(f'incoming/{schoolCode}')
                if xfer.error:
                    # notifyJM.log('fail', xfer.error, verbose)
                    self.logger.error(xfer.error)
                    continue

                # Loop on any files found in school incoming directories
                for schoolFile in schoolFiles:

                    # Get the subm ID to use for local AIP dir names
                    match = reAipPackage.match(schoolFile)
                    if match:
                        submissionId = match.group(1)
                    else:
                        # notifyJM.log('fail', f'File name {schoolFile}
                        #  is not supported', verbose)
                        self.logger.error(f'File name {schoolFile} \
                                          is not supported')
                        continue

                    batch = f'proquest{datestamp}-{submissionId}-{schoolCode}'
                    proquestInDir = f'{dataInDir}/{batch}'

                    if not os.path.isdir(proquestInDir):
                        try:
                            os.makedirs(proquestInDir)
                        except Exception as e:
                            xfer.close()
                            # notifyJM.log('fail', f'Failed to create
                            # {proquestInDir}', verbose)
                            self.logger.error(f'Failed to create \
                                              {proquestInDir}: {e}')
                            # notifyJM.report('stopped')
                            self.logger.error('stopped')
                            return False

                    # Get file and then move it to it's archive
                    try:
                        xfer.get_file(f'incoming/{schoolCode}/{schoolFile}',
                                      f'{proquestInDir}/{schoolFile}')
                        if xfer.error:
                            # notifyJM.log('fail', xfer.error, verbose)
                            self.logger.error(xfer.error)
                            continue
                    except Exception as e:
                        # notifyJM.log('fail', f'Fail to sftp {dropboxServer}:
                        # incoming/{schoolCode}/{schoolFile}', verbose)
                        self.logger.error(f'Fail to sftp {dropboxServer}: \
                                          incoming/{schoolCode}/{schoolFile}: \
                                            {e}')
                        continue

                    try:
                        xfer.rename(f'incoming/{schoolCode}/{schoolFile}',
                                    f'archives/{schoolCode}/{schoolFile}')
                        if xfer.error:
                            # notifyJM.log('fail', xfer.error, verbose)
                            self.logger.error(xfer.error)
                    except Exception as e:
                        # notifyJM.log('fail', f'Fail to archive
                        # {dropboxServer}:incoming/{schoolCode}/{schoolFile}',
                        # verbose)
                        self.logger.error(f'Fail to archive {dropboxServer}: \
                                          incoming/{schoolCode}/{schoolFile}: \
                                          {e}')
                        continue

                    # notifyJM.log('pass', f'Received {schoolCode}:
                    # {schoolFile}', verbose)
                    aipFiles.append([schoolCode, batch,
                                     f'{proquestInDir}/{schoolFile}'])

        xfer.close()
        return aipFiles
