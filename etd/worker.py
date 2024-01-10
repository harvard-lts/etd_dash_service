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
# from opentelemetry.trace.propagation.tracecontext \
#    import TraceContextTextMapPropagator
import lxml.etree as xmlTree
# import os, re, sys, shutil, xml.sax
import re
from datetime import datetime
from subprocess import run, PIPE
# import zipfile
# from glob import glob
from shlex import quote
from .xfer_files import xfer_files
from .constants import instance_data
from lib.notify import notify

"""
This is a basic worker class.

Since: 2023-05-23
Author: cgoines
"""

# tracing setup
JAEGER_NAME = os.getenv('JAEGER_NAME')
JAEGER_SERVICE_NAME = os.getenv('JAEGER_SERVICE_NAME')

resource = Resource(attributes={SERVICE_NAME: JAEGER_SERVICE_NAME})
provider = TracerProvider(resource=resource)
otlp_exporter = OTLPSpanExporter(endpoint=JAEGER_NAME, insecure=True)
span_processor = BatchSpanProcessor(otlp_exporter)
provider.add_span_processor(span_processor)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

# any "pragma: no cover" comments should be reviewed 2023--7-20
# see jira: https://jira.huit.harvard.edu/browse/ETD-205


class Worker():
    version = None
    logger = logging.getLogger('etd_dash')
    aipPattern = '.+_(\\d+).zip'
    reAipPackage = re.compile(aipPattern)
    datePattern = '\\d{4}(-\\d\\d-\\d\\d)'
    re5digitDate = re.compile(datePattern)
    dspaceHome = "/home/dspace"
    DSPACE_COMMAND = f'{dspaceHome}/dspace/bin/dspace'
    dspaceImportDir = f'{dspaceHome}/import'
    dspaceHost = os.getenv("dspaceHost")
    importUserName = 'dspace'
    handler2nrs = '/home/osc/proj/dashdump/data/tsv/handle2nrs.tsv'
    dimNamespace = '{http://www.dspace.org/xmlns/dspace/dim}'
    rightsNamespace = '{http://cosimo.stanford.edu/sdr/metsrights/}'
    dropboxUser = 'proquest'
    jobCode = 'proquest2dash'

    def __init__(self):
        self.version = os.getenv("APP_VERSION", "0.0.1")

    def get_version(self):
        return self.version

    @tracer.start_as_current_span("DASH SERVICE - send_to_dash_worker")
    def send_to_dash(self, message):  # pragma: no cover
        global notifyJM
        # now = datetime.now()
        # dateTimeStamp = now.strftime('%Y%m%d%H')
        # dateTimeStamp = now.strftime('%Y%m%d%H')
        # logFile = os.path.join(logDir, f"{jobCode}_{dateTimeStamp}.log")
        notifyJM = notify('monitor', self.jobCode, None)
        # Let the Job Monitor know that the job has started
        notifyJM.log('pass', 'Start Proquest to ETDs processing')
        notifyJM.report('start')

        if "job_ticket_id" in message:
            job_ticket_id = message['job_ticket_id']
            notifyJM.log('pass', f'job_ticket_id {job_ticket_id}')
            self.logger.info(f'job_ticket_id {job_ticket_id}')

        aipFiles = self.get_files()
        if not aipFiles:
            notifyJM.log('pass', 'No files found in dropbox')
            notifyJM.report('complete')
            return True

        filesDir = '/home/etdadm/files'
        csvEtds2Alma = os.path.join(filesDir, 'etds2alma.csv')
        etds2AlmaOut = open(csvEtds2Alma, 'a+')

        current_span = trace.get_current_span()
        current_span.add_event("sending to dash")

        # Process AIP files found
        for schoolCode, batch, aipFile in aipFiles:
            notifyJM.log('info', f'Processing {aipFile}')
            self.logger.info(f'Processing {aipFile}')

            aipDir = os.path.dirname(os.path.abspath(aipFile))
            aipFile = os.path.basename(aipFile)
            os.chdir(aipDir)

            # Get the submission ID for the AIP file name
            match = self.reAipPackage.match(aipFile)
            if match:
                self.submissionId = match.group(1)
            else:
                notifyJM.log('fail', f'File name {aipFile} is not supported')
                self.logger.error(f'File name {aipFile} is not supported')
                current_span.add_event(f'File name {aipFile} is not supported')
                continue

            # Unpack AIP package
            proc = self.sh(['unzip', aipFile])
            if proc.returncode > 0:
                notifyJM.log('fail', f"Failed to unzip {aipDir}/{aipFile}")
                self.logger.error(f"Failed to unzip {aipDir}/{aipFile}")
                continue

            os.remove(aipFile)

            # Rewrite mets file remapping a few elements
            if not self.rewrite_mets(aipDir, batch, schoolCode, message):
                continue

            # get proquest identifier from json message
            identifier = None
            if "identifier" in message:
                identifier = message["identifier"]
                current_span.set_attribute("identifier",
                                           message["identifier"])

            # Zip package back up with updated mets file
            zipWithArgs = ['zip', aipFile]
            for file in os.listdir('.'):
                zipWithArgs.append(file)
            proc = self.sh(zipWithArgs)
            if proc.returncode > 0:
                notifyJM.log('fail', f"The command {zipWithArgs} \
                            failed in {aipDir}")
                self.logger.info('fail', f"The command {zipWithArgs} \
                            failed in {aipDir}")
                continue

            proquestOutDir = aipDir.replace('/in/', '/out/')
            if not os.path.isdir(proquestOutDir):
                try:
                    os.makedirs(proquestOutDir)
                except Exception as e:
                    notifyJM.log('fail', f'Failed to create {proquestOutDir}')
                    self.logger.error(f'Failed to create \
                                      {proquestOutDir}: {e}')
                    current_span.set_status(Status(StatusCode.ERROR))
                    current_span.add_event(f'Failed to create \
                                      {proquestOutDir}: {e}')
                    current_span.record_exception(e)
                    continue
            self.logger.info(f'IDENTIFIER: {identifier}')
            # use api to check for duplicate and end if so
            if identifier is None:
                self.logger.error(f'No proquest for {aipFile}')
                notifyJM.log('fail', f'No proquest for {aipFile}')
                continue
            if self.check_for_duplicates(identifier):
                self.logger.error(f'{identifier} is a duplicate')
                notifyJM.log('fail', f'{identifier} is a duplicate')
                # form the dupe directory for the aip
                dupe_dir = aipDir.replace('/in/', '/dupe/')
                # append timestamp to the dupe_dir
                dupe_dir = dupe_dir + "_" + self.get_timestamp()
                # move the aip to the dupe_dir, catch exception
                try:
                    self.rename_directory(aipDir, dupe_dir)
                except Exception as e:
                    self.logger.error(f'Failed to move \
                                        {aipDir} to {dupe_dir}: {e}')
                # delete empty output directory, catch exception
                try:
                    os.rmdir(proquestOutDir)
                except Exception as e:
                    self.logger.error(f'Failed to remove \
                                        {proquestOutDir}: {e}')
                continue

            collection_handle = instance_data[schoolCode]['handle']
            dashImportFile = f'{self.dspaceImportDir}/proquest/' + aipFile

            dest = f'{self.importUserName}@{self.dspaceHost}:\
                {dashImportFile}'
            proc = self.sh(['scp', '-r', aipFile, dest])
            if proc.returncode == 0:
                notifyJM.log('pass', f"Copied AIP package to {self.importUserName}@{self.dspaceHost}:{dashImportFile}")  # noqa: E501
                self.logger.info(f"Copied AIP package to \
                                {self.importUserName} \
                                @{self.dspaceHost}:{dashImportFile}")
                current_span.add_event(f"Copied AIP package to \
                                {self.importUserName} \
                                @{self.dspaceHost}:{dashImportFile}")
            else:
                notifyJM.log('fail', f"Failed to send \
                                {aipDir}/{aipFile} to \
                {self.importUserName}@{self.dspaceHost}:{dashImportFile}")
                self.logger.error(f"Failed to send {aipDir}/{aipFile} to \
                                {self.importUserName}@{self.dspaceHost}: \
                                {dashImportFile}")
                current_span.add_event(f"Failed to send \
                                        {aipDir}/{aipFile} to \
                                {self.importUserName}@{self.dspaceHost}: \
                                {dashImportFile}")
                continue

            # Import to DASH
            sub2handle = {}  # we keep mapfile as an on-disk record
            # check that proquestOutDir exists
            if not os.path.isdir(proquestOutDir):
                notifyJM.log('fail', f'{proquestOutDir} does not exist')
                self.logger.error(f'{proquestOutDir} does not exist')
                current_span.add_event(f'{proquestOutDir} does not exist')
                continue

            with open(os.path.join(proquestOutDir,
                                   "mapfile"), 'w') as mapfile:

                sub_id = aipFile.rstrip('.zip')
                result = self.ssh([self.DSPACE_COMMAND, 'packager', '-s',
                                   '-w',
                                   '-t', 'AIP',
                                   '-e', 'hl_dash_admin@harvard.edu',
                                   '-p', collection_handle,
                                   dashImportFile])
                # convert result to string and log it
                resultStr = str(result.stdout, 'utf-8')
                self.logger.debug(f"DSpace result: {resultStr}")

                if result.returncode == 0:
                    notifyJM.log('pass', f"Imported {aipDir}/\
                                {aipFile} to DSpace")
                    self.logger.info(f"Imported {aipDir}/ \
                                    {aipFile} to DSpace")
                    handle = self.get_handle(str(result.stdout, 'utf-8'))
                    sub2handle[sub_id] = handle
                    print(f'{sub_id} {handle}', file=mapfile)
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
                    notifyJM.log('fail', message)
                    self.logger.error(message)
                    current_span.add_event(message)
                    continue

            # handles = set(sub2handle.values())

            # run citation-update curation task

            result = self.ssh([self.DSPACE_COMMAND, 'curate', '-t',
                              'citation-update', '-i', collection_handle])
            if result.returncode == 0:
                notifyJM.log('pass', "Ran citation-update curation task")
                self.logger.info("Ran citation-update curation task")
            else:
                message = "Couldn't run citation-update curation task"
                message += f'Command run was: {" ".join(result.args)}'
                message += f"Process return code was: {result.returncode}"
                message += f"Process output was:\
                    \n\n{str(result.stdout, 'utf-8')}"
                message += f"Process error was:\
                    \n\n{str(result.stderr, 'utf-8')}"
                notifyJM.log('warn', message)
                self.logger.warn(message)
                current_span.add_event(message)

            etds2AlmaOut.write(f'{schoolCode},{batch}\n')

        etds2AlmaOut.close()
        notifyJM.report('complete')
        self.logger.info('complete')
        current_span.add_event("completed")
        return True

    @tracer.start_as_current_span("get_files")
    def get_files(self):  # pragma: no cover
        global notifyJM
        dropboxServer = os.getenv("dropboxServer")
        dropboxUser = os.getenv("dropboxUser")
        # homeDir = os.getenv("homeDir")
        privateKey = os.getenv("PRIVATE_KEY_PATH")
        now = datetime.now()
        datestamp = now.strftime('%Y%m%d%H')
        dataDir = '/home/etdadm/data'
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
                notifyJM.log('fail', xfer.error)
                self.logger.error(xfer.error)
                notifyJM.report('stopped')
                self.logger.error('stopped')
                return False
        except Exception as e:
            notifyJM.log('fail', f'Fail to connect to \
                         {dropboxUser}@{dropboxServer}')
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
            notifyJM.log('fail', xfer.error)
            self.logger.error(xfer.error)
            notifyJM.report('stopped')
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
                    notifyJM.log('fail', xfer.error)
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
                        notifyJM.log('fail', f'File name {schoolFile}\
                         is not supported')
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
                            notifyJM.log('fail', f'Failed to create \
                                         {proquestInDir}')
                            self.logger.error(f'Failed to create \
                                              {proquestInDir}: {e}')
                            notifyJM.report('stopped')
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
                            notifyJM.log('fail', xfer.error)
                            self.logger.error(xfer.error)
                            current_span.add_event(xfer.error)
                            continue
                    except Exception as e:
                        notifyJM.log('fail', f'Fail to sftp {dropboxServer}:\
                         incoming/{schoolCode}/{schoolFile}')
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
                        self.move_to_archives_dir(xfer, schoolCode, schoolFile,
                                                  dropboxServer, notifyJM)
                    except Exception as e:
                        log_msg = (
                            f'Failed to archive {dropboxServer}:'
                            f'incoming/{schoolCode}/{schoolFile}: {e}'
                        )
                        notifyJM.log('fail', log_msg)
                        self.logger.error(log_msg)
                        current_span.set_status(Status(StatusCode.ERROR))
                        current_span.add_event(f'Fail to archive \
                                          {dropboxServer}: \
                                          incoming/{schoolCode}/{schoolFile}: \
                                          {e}')
                        current_span.record_exception(e)
                        continue

                    notifyJM.log('pass', f'Received {schoolCode}: \
                                 {schoolFile}')
                    aipFiles.append([schoolCode, batch,
                                     f'{proquestInDir}/{schoolFile}'])

        xfer.close()
        current_span.add_event("get files completed")
        return aipFiles

    # A few fields need to be remapped in the xml
    @tracer.start_as_current_span("rewrite_mets")
    def rewrite_mets(self, aipDir, batch, schoolCode, json_message):
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
                notifyJM.log('fail', f'Failed to load xml from {metsFile}')
                self.logger.error(f'Failed to load xml from {metsFile}: {e}')
                return False
        else:  # pragma: no cover
            notifyJM.log('fail', f"{metsFile} not found")
            self.logger.error(f"{metsFile} not found")
            notifyJM.report('stopped')
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
                        # if dimField.attrib['qualifier'] exists, log it
                        if 'qualifier' in dimField.attrib and \
                                dimField.attrib['qualifier'] == 'PQ':
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

                elif dimField.attrib['element'] == 'identifier':
                    try:   # pragma: no cover
                        proquest_identifier = dimField.text
                        json_message["identifier"] = proquest_identifier
                        current_span.set_attribute("identifier",
                                                   proquest_identifier)
                        self.logger.info("proquest id: " +
                                         str(proquest_identifier))
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
                                or dimField.text == 'A.L.M.'):  # pragma: no cover # noqa: E501
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
                        if (dimField.text == 'Doctoral Dissertation'):  # pragma: no cover # noqa: E501
                            dimField.text = 'Doctoral'

                        elif dimField.text == "Master's":
                            dimField.text = dimField.text.replace("'", '')
                            addMasters = False

                    elif dimField.attrib['qualifier'] == 'level':

                        # Doctoral
                        if dimField.text == 'Doctoral Dissertation':  # pragma: no cover # noqa: E501
                            dimField.text = 'Doctoral'

            # Check for an embargo. Replace 5 digit year dates.
            elif dimField.attrib['mdschema'] == 'dash':  # pragma: no cover # noqa: E501
                if (dimField.attrib['element'] == 'embargo'):
                    notifyJM.log('info', f"{aipDir}/mets.xml: \
                                 Embargo information found")
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
                                        notifyJM.log('warn', f"Batch {batch} \
                                        will not be sent to Dash due to \
                                        permanentEmbargo information found")
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
                if match:  # pragma: no cover # noqa: E501
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

    def sh(self, *args, **kwargs):
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
                    notifyJM.log('warn', f"No handle in output: {output}")
                    self.logger.warn(f"No handle in output: {output}")

    # this is call to the DASH healthcheck for integration testing
    @tracer.start_as_current_span("call_api")
    def call_api(self):
        # url = "https://dash.harvard.edu/rest/test"
        rest_url = os.getenv("DASH_REST_URL",
                             "https://dash.harvard.edu/rest")
        url = rest_url + "/test"
        # need verify false b/c using selfsigned certs
        r = requests.get(url, verify=False)
        self.logger.debug("In call api")
        self.logger.debug(r.text)
        current_span = trace.get_current_span()
        current_span.add_event("call api url: " + url)
        return r.text

    def check_for_duplicates(self, identifier):
        rest_url = os.getenv("DASH_REST_URL",
                             "https://dash.harvard.edu/rest")
        query_url = f"{rest_url}/items/find-by-metadata-field"
        self.logger.debug(f'URL: {query_url}')
        json_query = {"key": "dc.identifier.other", "value": identifier}
        resp = requests.post(query_url, json=json_query, verify=False)
        # self.logger.debug(f'RESPONSE: {resp.text}')
        if resp.text == "[]":
            return False
        else:
            return True

    # rename a directory and throw exception if it fails.
    # this will create directories as needed.
    def rename_directory(self, src_dir, dest_dir):
        if os.path.exists(dest_dir):
            raise FileExistsError(f"Destination directory {dest_dir} exists")
        try:
            os.renames(src_dir, dest_dir)
        except Exception as e:
            raise e

    # return a timestamp as a string
    def get_timestamp(self):
        now = datetime.now()
        return now.strftime('%Y%m%d%H%M%S')

    # move deposit to archives directory
    def move_to_archives_dir(self, xfer, schoolCode, schoolFile,
                             dropboxServer, notifyJM):
        incoming_path = f'incoming/{schoolCode}/{schoolFile}'
        archives_path = f'archives/{schoolCode}/{schoolFile}'

        log_msg = f'Moving {schoolFile} to {archives_path}'
        notifyJM.log('info', log_msg)
        self.logger.info(log_msg)

        if not xfer.isfile(archives_path):
            xfer.rename(incoming_path, archives_path)
        else:
            log_msg = f'File {schoolFile} already exists in {archives_path}.'
            notifyJM.log('warn', log_msg)
            self.logger.warn(log_msg)

            # create dupe directory if needed
            if not xfer.isdir(f'dupe/{schoolCode}'):
                xfer.makedirs(f'dupe/{schoolCode}')

            # move the file to the dupe directory
            dupe_file = schoolFile.replace(".",
                                           "_" + self.get_timestamp() + ".")
            dupe_path = f'dupe/{schoolCode}/{dupe_file}'

            log_msg = f'Moving {schoolFile} to {dupe_path}'
            notifyJM.log('info', log_msg)
            self.logger.info(log_msg)
            xfer.rename(incoming_path, dupe_path)
