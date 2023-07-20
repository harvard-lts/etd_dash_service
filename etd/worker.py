import requests
import logging
import os
from opentelemetry import trace
from opentelemetry.trace import Status
from opentelemetry.trace import StatusCode
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter)
from opentelemetry.sdk.resources import SERVICE_NAME


import lxml.etree as xmlTree
# import os, re, sys, shutil, xml.sax
import re
from datetime import datetime
from subprocess import run, PIPE
import zipfile
# from glob import glob
from shlex import quote
from .xfer_files import xfer_files
from .constants import instance_data

"""
This is a basic worker class.

Since: 2023-05-23
Author: cgoines
"""

# tracing setup
JAEGER_NAME = os.getenv('JAEGER_NAME')
JAEGER_SERVICE_NAME = os.getenv('JAEGER_SERVICE_NAME')

resource = Resource(attributes={SERVICE_NAME: JAEGER_SERVICE_NAME})
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)

otlp_exporter = OTLPSpanExporter(endpoint=JAEGER_NAME, insecure=True)

span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# any "pragma: no cover" comments should be reviewed 2023--7-20
# see jira: https://jira.huit.harvard.edu/browse/ETD-205


class Worker():
    version = None
    logger = logging.getLogger('etd_dash')
    aipPattern = '.+_(\\d+).zip'
    reAipPackage = re.compile(aipPattern)
    datePattern = '\\d{5}(-\\d\\d-\\d\\d)'
    re5digitDate = re.compile(datePattern)
    dspaceHome = "/home/dspace"
    DSPACE_COMMAND = f'{dspaceHome}/dspace/bin/dspace'
    dspaceImportDir = f'{dspaceHome}/import'
    dspaceHost = os.getenv("dspaceHost")
    importUserName = 'dspace'
    DSPACE_COMMAND = f'{dspaceHome}/dspace/bin/dspace'
    handler2nrs = '/home/osc/proj/dashdump/data/tsv/handle2nrs.tsv'
    dimNamespace = '{http://www.dspace.org/xmlns/dspace/dim}'
    rightsNamespace = '{http://cosimo.stanford.edu/sdr/metsrights/}'
    dropboxUser = 'proquest'

    def __init__(self):
        self.version = os.getenv("APP_VERSION", "0.0.1")

    def get_version(self):
        return self.version

    @tracer.start_as_current_span("send_to_dash_worker")
    def send_to_dash(self, message):  # pragma: no cover
        # global dspace_instance, notifyJM
        # now = datetime.now()
        # dateTimeStamp = now.strftime('%Y%m%d%H')
        # dateTimeStamp = now.strftime('%Y%m%d%H')
        # logFile = os.path.join(logDir, f"{jobCode}_{dateTimeStamp}.log")
        # notifyJM = notify('monitor+log', jobCode, logFile)
        # Let the Job Monitor know that the job has started
        # notifyJM.log('pass', f'Start Proquest to ETDs processing', verbose)
        # notifyJM.report('start')

        aipFiles = self.get_files()

        self.logger.debug(message)
        filesDir = 'files'
        csvEtds2Alma = os.path.join(filesDir, 'etds2alma.csv')
        etds2AlmaOut = open(csvEtds2Alma, 'a+')

        current_span = trace.get_current_span()
        current_span.add_event("sending to dash")

        # Process AIP files found
        for schoolCode, batch, aipFile in aipFiles:
            # notifyJM.log('info', f'Processing {aipFile}', verbose)
            self.logger.info(f'Processing {aipFile}')

            aipDir = os.path.dirname(os.path.abspath(aipFile))
            aipFile = os.path.basename(aipFile)
            os.chdir(aipDir)

            # Get the submission ID for the AIP file name
            match = self.reAipPackage.match(aipFile)
            if match:
                self.submissionId = match.group(1)
            else:
                # notifyJM.log('fail', f'File name {aipFile}
                # is not supported', verbose)
                self.logger.error(f'File name {aipFile} is not supported')
                current_span.add_event(f'File name {aipFile} is not supported')
                continue

            # Unpack AIP package
            # proc = self.sh(['unzip', aipFile])
            # if proc.returncode > 0:
                # notifyJM.log('fail', f"Failed to
                # unzip {aipDir}/{aipFile}", verbose)
            #     self.logger.error(f"Failed to unzip {aipDir}/{aipFile}")
            #    continue

            with zipfile.ZipFile(aipFile, 'r') as zip_ref:
                zip_ref.extractall(".")

            os.remove(aipFile)

            # Rewrite mets file remapping a few elements
            if not self.rewrite_mets(aipDir, batch, schoolCode):
                continue
            # Zip package back up with updated mets file
            # zipWithArgs = ['zip', aipFile]
            # for file in os.listdir('.'):
            #     zipWithArgs.append(file)
            # proc = self.sh(zipWithArgs)
            try:
                # proc = run(['zip', '-r', aipFile, aipDir])
                current_directory = os.getcwd()
                with zipfile.ZipFile(aipFile, 'w',
                                     zipfile.ZIP_DEFLATED) as zipf:
                    for root, dirs, files in os.walk(current_directory):
                        for file in files:
                            if file != aipFile:
                                file_path = os.path.join(root, file)
                                zipf.write(file_path, arcname=os.path.
                                           relpath(file_path,
                                                   current_directory))
            except Exception as e:
                self.logger.error(e)
                self.logger.error(f"The zip command \
                                  failed in {aipDir}")
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event(f"The zip command \
                                  failed in {aipDir}")
                current_span.record_exception(e)
            # if proc.returncode > 0:
                # notifyJM.log('fail', f"The command {zipWithArgs}
                # failed in {aipDir}", verbose)
            #     self.logger.error(f"The zip command \
            #                       failed in {aipDir}")
            #    continue

            proquestOutDir = aipDir.replace('/in/', '/out/')
            if not os.path.isdir(proquestOutDir):
                try:
                    os.makedirs(proquestOutDir)
                except Exception as e:
                    # notifyJM.log('fail', f'Failed to
                    # create {proquestOutDir}', verbose)
                    self.logger.error(f'Failed to create \
                                      {proquestOutDir}: {e}')
                    current_span.set_status(Status(StatusCode.ERROR))
                    current_span.add_event(f'Failed to create \
                                      {proquestOutDir}: {e}')
                    current_span.record_exception(e)
                    continue

            collection_handle = instance_data[schoolCode]['handle']
            dashImportFile = f'{self.dspaceImportDir}/proquest/' + aipFile

            dest = f'{self.importUserName}@{self.dspaceHost}:{dashImportFile}'
            proc = self.sh(['scp', '-r', aipFile, dest])
            if proc.returncode == 0:
                # notifyJM.log('pass', f"Copied AIP package to
                # {importUserName}@{dspaceHost}:{dashImportFile}", verbose)
                self.logger.info(f"Copied AIP package to \
                                 {self.importUserName} \
                                 @{self.dspaceHost}:{dashImportFile}")
                current_span.add_event(f"Copied AIP package to \
                                 {self.importUserName} \
                                 @{self.dspaceHost}:{dashImportFile}")
            else:
                # notifyJM.log('fail', f"Failed to send {aipDir}/{aipFile} to \
                # {importUserName}@{dspaceHost}:{dashImportFile}", verbose)
                self.logger.error(f"Failed to send {aipDir}/{aipFile} to \
                                  {self.importUserName}@{self.dspaceHost}: \
                                  {dashImportFile}")
                current_span.add_event(f"Failed to send {aipDir}/{aipFile} to \
                                  {self.importUserName}@{self.dspaceHost}: \
                                  {dashImportFile}")
                continue

            # Import to DASH
            sub2handle = {}  # we keep mapfile as an on-disk record
            with open(os.path.join(proquestOutDir, "mapfile"), 'w') as mapfile:

                sub_id = aipFile.rstrip('.zip')
                result = self.ssh([self.DSPACE_COMMAND, 'packager', '-s', '-w',
                                   '-t', 'AIP',
                                   '-e', 'hl_dash_admin@harvard.edu',
                                   '-p', collection_handle,
                                   dashImportFile])
                if result.returncode == 0:
                    # notifyJM.log('pass', f"Imported
                    # {aipDir}/{aipFile} to DSpace", verbose)
                    self.logger.info(f"Imported {aipDir}/ \
                                     {aipFile} to DSpace")
                    handle = self.get_handle(str(result.stdout, 'utf-8'))
                    sub2handle[sub_id] = handle
                    self.logger.info(f'{sub_id} {handle}', file=mapfile)
                else:
                    message = f"DSpace import failed \
                        for {aipDir}/{aipFile}.\n"
                    message += f"Command run was: \
                        {' '.join(result.args)}\n"
                    message += f"Process return code was: \
                        {result.returncode}\n"
                    message += f"Process output was:\
                        \n\n{str(result.stdout, 'utf-8')}\n"
                    message += f"Process error was:\
                        \n\n{str(result.stderr, 'utf-8')}\n"
                    # notifyJM.log('fail', message, verbose)
                    self.logger.error(message)
                    current_span.add_event(message)
                    continue

            # handles = set(sub2handle.values())

            # run citation-update curation task
            result = self.ssh([self.DSPACE_COMMAND, 'curate', '-t',
                               'citation-update', '-i', collection_handle])
            if result.returncode == 0:
                # notifyJM.log('pass',
                # "Ran citation-update curation task", verbose)
                self.logger.info("Ran citation-update curation task")
            else:
                message = "Couldn't run citation-update curation task"
                message += f'Command run was: {" ".join(result.args)}'
                message += f"Process return code was: {result.returncode}"
                message += f"Process output was:\
                    \n\n{str(result.stdout, 'utf-8')}"
                message += f"Process error was:\
                    \n\n{str(result.stderr, 'utf-8')}"
                # notifyJM.log('warn', message, verbose)
                self.logger.warn(message)
                current_span.add_event(message)

            etds2AlmaOut.write(f'{schoolCode},{batch}\n')

        etds2AlmaOut.close()
        # notifyJM.report('complete')
        self.logger.info('complete')
        current_span.add_event("completed")
        return True

    @tracer.start_as_current_span("get_files")
    def get_files(self):  # pragma: no cover
        # global notifyJM
        dropboxServer = os.getenv("dropboxServer")
        dropboxUser = os.getenv("dropboxUser")
        # homeDir = os.getenv("homeDir")
        privateKey = os.getenv("PRIVATE_KEY_PATH")
        now = datetime.now()
        datestamp = now.strftime('%Y%m%d%H')
        dataDir = 'data'
        dataInDir = os.path.join(dataDir, "in")
        aipFiles = []

        current_span = trace.get_current_span()
        current_span.add_event("get files started")

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
            current_span.set_status(Status(StatusCode.ERROR))
            current_span.add_event(f'Fail to connect to \
                              {dropboxUser}@{dropboxServer}: {e}')
            current_span.record_exception(e)

        # Get a list of incoming school directories
        schoolDirs = xfer.listdir('incoming')
        if xfer.error:
            xfer.close()
            # notifyJM.log('fail', xfer.error, verbose)
            self.logger.error(xfer.error)
            # notifyJM.report('stopped')
            self.logger.error('stopped')
            current_span.add_event('stopped')
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
                    current_span.add_event(xfer.error)
                    continue

                # Loop on any files found in school incoming directories
                for schoolFile in schoolFiles:
                    self.logger.info("schoolFile")
                    self.logger.info(schoolFile)
                    current_span.add_event("schoolFile")
                    current_span.add_event(schoolFile)
                    # Get the subm ID to use for local AIP dir names
                    match = self.reAipPackage.match(schoolFile)
                    if match:
                        submissionId = match.group(1)
                    else:
                        # notifyJM.log('fail', f'File name {schoolFile}
                        #  is not supported', verbose)
                        self.logger.error(f'File name {schoolFile} \
                                          is not supported')
                        current_span.add_event('File name {schoolFile} \
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
                            current_span.add_event(f'Failed to create \
                                              {proquestInDir}: {e}')
                            current_span.add_event('stopped')
                            return False

                    # Get file and then move it to it's archive
                    try:
                        xfer.get_file(f'incoming/{schoolCode}/{schoolFile}',
                                      f'{proquestInDir}/{schoolFile}')
                        if xfer.error:
                            # notifyJM.log('fail', xfer.error, verbose)
                            self.logger.error(xfer.error)
                            current_span.add_event(xfer.error)
                            continue
                    except Exception as e:
                        # notifyJM.log('fail', f'Fail to sftp {dropboxServer}:
                        # incoming/{schoolCode}/{schoolFile}', verbose)
                        self.logger.error(f'Fail to sftp {dropboxServer}: \
                                          incoming/{schoolCode}/{schoolFile}: \
                                            {e}')
                        current_span.set_status(Status(StatusCode.ERROR))
                        current_span.add_event(f'Fail to sftp \
                                          {dropboxServer}: \
                                          incoming/{schoolCode}/{schoolFile}: \
                                            {e}')
                        current_span.record_exception(e)
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
                        current_span.set_status(Status(StatusCode.ERROR))
                        current_span.add_event(f'Fail to archive \
                                          {dropboxServer}: \
                                          incoming/{schoolCode}/{schoolFile}: \
                                          {e}')
                        current_span.record_exception(e)
                        continue

                    # notifyJM.log('pass', f'Received {schoolCode}:
                    # {schoolFile}', verbose)
                    aipFiles.append([schoolCode, batch,
                                     f'{proquestInDir}/{schoolFile}'])

        xfer.close()
        current_span.add_event("get files completed")
        return aipFiles

    # A few fields need to be remapped in the xml
    @tracer.start_as_current_span("rewrite_mets")
    def rewrite_mets(self, aipDir, batch, schoolCode):
        global notifyJM
        underGrad = False
        masters = False
        addMasters = False
        sendToDash = True

        current_span = trace.get_current_span()
        current_span.add_event("rewrite mets started")

        # Load mets file into an xml tree object
        metsFile = f'{aipDir}/mets.xml'
        if os.path.isfile(metsFile):
            try:
                treeMets = xmlTree.parse(metsFile)
                rootMets = treeMets.getroot()
            except Exception as e:  # pragma: no covers
                # notifyJM.log('fail', f'Failed to
                # load xml from {metsFile}', verbose)
                self.logger.error(f'Failed to load xml from {metsFile}: {e}')
                return False
        else:  # pragma: no cover
            # notifyJM.log('fail', f"{metsFile} not found", verbose)
            self.logger.error(f"{metsFile} not found")
            # notifyJM.report('stopped')
            self.logger.error("stppped")
            current_span.add_event("stopped")
            return False

        # Find and remapped fields
        for dimField in rootMets.iter(f'{self.dimNamespace}field'):
            if dimField.attrib['mdschema'] == 'dc':
                if dimField.attrib['element'] == 'date':
                    if dimField.attrib['qualifier'] == 'created':
                        dimField.attrib['mdschema'] = 'thesis'
                        dimField.attrib['element'] = 'degree'
                        dimField.attrib['qualifier'] = 'date'

                    elif dimField.attrib['qualifier'] == 'submitted':
                        dimField.attrib['qualifier'] = 'created'

                elif dimField.attrib['element'] == 'subject':
                    try:
                        if dimField.attrib['qualifier'] == 'PQ':
                            dimField.attrib.pop('qualifier')
                    except Exception as e:
                        self.logger.info(e)
                        current_span.record_exception(e)
                        continue

                elif dimField.attrib['element'] == 'dc':
                    try:   # pragma: no cover
                        if dimField.attrib['qualifier'] == 'subject':
                            dimField.attrib['element'] = 'subject'
                            dimField.attrib.pop('qualifier')
                    except Exception as e:  # pragma: no cover
                        self.logger.info(e)
                        current_span.record_exception(e)
                        continue

            elif dimField.attrib['mdschema'] == 'thesis':
                if dimField.attrib['element'] == 'degree':
                    if dimField.attrib['qualifier'] == 'date':
                        dimField.attrib['mdschema'] = 'dc'
                        dimField.attrib['element'] = 'date'
                        dimField.attrib['qualifier'] = 'submitted'

                    elif dimField.attrib['qualifier'] == 'name':

                        # College Undergraduate	and DCE Masters
                        if (dimField.text == 'A.B.' or dimField.text == 'S.B.'
                                or dimField.text == 'A.L.M.'):
                            dimField.text = dimField.text.replace('.', '')

                            # College Undergraduate
                            if dimField.text == 'AB' or dimField.text == 'SB':
                                underGrad = True
                                parentNode = dimField.getparent()
                                dimFieldAdd = xmlTree.SubElement(
                                    parentNode, f'{self.dimNamespace}field')
                                dimFieldAdd.attrib['mdschema'] = "thesis"
                                dimFieldAdd.attrib['element'] = "degree"
                                dimFieldAdd.attrib['qualifier'] = "level"
                                dimFieldAdd.text = 'Undergraduate'

                            #  DCE Masters
                            elif dimField.text == 'ALM':
                                masters = True

                    elif dimField.attrib['qualifier'] == 'level':

                        # Doctoral
                        if (dimField.text == 'Doctoral Dissertation'):
                            dimField.text = 'Doctoral'

                        elif dimField.text == "Master's":
                            dimField.text = dimField.text.replace("'", '')
                            addMasters = False

                    elif dimField.attrib['qualifier'] == 'level':

                        # Doctoral
                        if dimField.text == 'Doctoral Dissertation':
                            dimField.text = 'Doctoral'

            # Check for an embargo. Replace 5 digit year dates.
            elif dimField.attrib['mdschema'] == 'dash':
                if (dimField.attrib['element'] == 'embargo'):
                    # notifyJM.log('info', f"{aipDir}/mets.xml: Embargo
                    # information found", verbose)
                    self.logger.info(f"{aipDir}/mets.xml: \
                                     Embargo information found")
                    current_span.add_event(f"{aipDir}/mets.xml: \
                                     Embargo information found")

                    try:
                        if (dimField.attrib['qualifier'] == 'terms' or
                                dimField.attrib['qualifier'] == 'until'):
                            match = self.re5digitDate.match(dimField.text)
                            if match:

                                # These do not go to Dash
                                if (schoolCode == 'college' and
                                        dimField.attrib['qualifier'] ==
                                        'terms'):
                                    if dimField.text == '10000-01-01':
                                        # notifyJM.log('warn', f"Batch {batch}
                                        # will not be sent to Dash due to
                                        # permanentEmbargo information found",
                                        # verbose)
                                        sendToDash = False
                                else:
                                    dimField.text = f'9999{match.group(1)}'
                    except Exception as e:
                        self.logger.info(e)
                        current_span.record_exception(e)
                        continue

        # Do another pass to remove any html tags
        # and to set College Undergraduate and masters
        for dimField in rootMets.iter(f'{self.dimNamespace}field'):

            # Remove any html tags
            if dimField.text:
                textNoHtmlPattern = '</*\\w*>?'
                textNoHtml = re.sub(textNoHtmlPattern, '', dimField.text)
                dimField.text = textNoHtml

            if dimField.attrib['mdschema'] == 'thesis':
                if dimField.attrib['element'] == 'degree':
                    try:
                        if dimField.attrib['qualifier'] == 'grantor':

                            # Remove if found
                            dimFieldPattern = '\\s*-\\s*Pre\\s*20\\d\\d'
                            dimField.text = re.sub(dimFieldPattern, '',
                                                   dimField.text)

                            if underGrad:  # pragma: no cover
                                engText = 'Harvard University Engineering \
                                    and Applied Sciences'
                                if dimField.text == engText:
                                    dimField.text = 'Harvard College'

                            if masters:   # pragma: no cover
                                almText = ': Master of Liberal Arts in \
                                    Ext. Studies (ALM)'
                                if almText in dimField.text:
                                    dimField.text = (
                                        dimField.text.replace(almText, '')
                                    )

                            if addMasters:  # pragma: no cover
                                parentNode = dimField.getparent()
                                dimFieldAdd = xmlTree.\
                                    SubElement(parentNode,
                                               f'{self.dimNamespace}field')
                                dimFieldAdd.attrib['mdschema'] = "thesis"
                                dimFieldAdd.attrib['element'] = "degree"
                                dimFieldAdd.attrib['qualifier'] = "level"
                                dimFieldAdd.text = 'Masters'

                    except Exception as e:  # pragma: no cover
                        self.logger.info(e)
                        current_span.record_exception(e)
                        continue

        # Replace any 5 digit year dates in right context
        for rightsContext in \
                rootMets.iter(f'{self.rightsNamespace}Context'):
            try:
                match = \
                    self.re5digitDate.match(rightsContext.attrib['start-date'])
                if match:
                    rightsContext.attrib['start-date'] = \
                        f'9999{match.group(1)}'
            except Exception as e:
                self.logger.info(e)
                current_span.record_exception(e)
                continue

        # Write out updated mets file and move into place
        os.remove(metsFile)
        treeMets.write(metsFile, encoding='utf-8',
                       xml_declaration=True, pretty_print=True)

        current_span.add_event("rewrite mets completed")
        return sendToDash

    def sh(self, *args, **kwargs):  # pragma: no cover
        kwargs['stdout'] = kwargs['stderr'] = PIPE
        return run(*args, **kwargs)

    @tracer.start_as_current_span("ssh")
    def ssh(self, command, *arguments, **kwargs):  # pragma: no cover
        current_span = trace.get_current_span()
        current_span.add_event("connecting via ssh")

        if self.importUserName != 'dspace':
            command = ["sudo", "-u", "dspace", *command]
        return self.sh(['ssh', f'{self.importUserName}@{self.dspaceHost}',
                        " ".join(map(quote, command))], *arguments, **kwargs)

    def get_handle(self, output):  # pragma: no cover
        for line in output.split("\n"):
            if line.startswith('CREATED'):
                match = re.search(r'hdl=(\d+/\d+)', line)
                if match:
                    return match.group(1)
                else:
                    # notifyJM.log('warn', f"No handle in output:
                    # {output}", verbose)
                    self.logger.warn(f"No handle in output: {output}")

    # this is call to the DASH healthcheck for integration testing
    @tracer.start_as_current_span("call_api")
    def call_api(self):
        # url = "https://dash.harvard.edu/rest/test"
        url = os.getenv("DASH_TESTING_URL")
        # need verify false b/c using selfsigned certs
        r = requests.get(url, verify=False)
        self.logger.debug("In call api")
        self.logger.debug(r.text)
        # current_span = trace.get_current_span()
        # current_span.add_event("in call api")
        return r.text
