import requests
import logging
import os

import lxml.etree as xmlTree
# import os, re, sys, shutil, xml.sax
import re
from datetime import datetime
from subprocess import run, PIPE
import zipfile
# from glob import glob
from shlex import quote
from .xfer_files import xfer_files
# import paramiko

"""
This is a basic worker class.

Since: 2023-05-23
Author: cgoines
"""


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

    def send_to_dash(self, message):
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
                    continue

            instance_data = self.getInstanceData()
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
            else:
                # notifyJM.log('fail', f"Failed to send {aipDir}/{aipFile} to \
                # {importUserName}@{dspaceHost}:{dashImportFile}", verbose)
                self.logger.error(f"Failed to send {aipDir}/{aipFile} to \
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

            etds2AlmaOut.write(f'{schoolCode},{batch}\n')

        etds2AlmaOut.close()
        # notifyJM.report('complete')
        self.logger.info('complete')
        return True

    # this is call to the DASH healthcheck for integration testing
    def call_api(self):
        url = "https://dash.harvard.edu/rest/test"
        r = requests.get(url)
        self.logger.debug("In call api")
        return r.text

    def get_files(self):
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
                    self.logger.info("schoolFile")
                    self.logger.info(schoolFile)
                    # Get the subm ID to use for local AIP dir names
                    match = self.reAipPackage.match(schoolFile)
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

    # A few fields need to be remapped in the xml
    def rewrite_mets(self, aipDir, batch, schoolCode):
        global notifyJM
        underGrad = False
        masters = False
        addMasters = False
        sendToDash = True

        # Load mets file into an xml tree object
        metsFile = f'{aipDir}/mets.xml'
        if os.path.isfile(metsFile):
            try:
                treeMets = xmlTree.parse(metsFile)
                rootMets = treeMets.getroot()
            except Exception as e:
                # notifyJM.log('fail', f'Failed to
                # load xml from {metsFile}', verbose)
                self.logger.error(f'Failed to load xml from {metsFile}: {e}')
                return False
        else:
            # notifyJM.log('fail', f"{metsFile} not found", verbose)
            self.logger.error(f"{metsFile} not found")
            # notifyJM.report('stopped')
            self.logger.error("stppped")
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
                        continue

                elif dimField.attrib['element'] == 'dc':
                    try:
                        if dimField.attrib['qualifier'] == 'subject':
                            dimField.attrib['element'] = 'subject'
                            dimField.attrib.pop('qualifier')
                    except Exception as e:
                        self.logger.info(e)
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
                        if dimField.text == 'Doctoral Dissertation':
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
                if dimField.attrib['element'] == 'embargo':
                    # notifyJM.log('info', f"{aipDir}/mets.xml: Embargo
                    # information found", verbose)
                    self.logger.info(f"{aipDir}/mets.xml: \
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

                            if underGrad:
                                engText = 'Harvard University Engineering \
                                    and Applied Sciences'
                                if dimField.text == engText:
                                    dimField.text = 'Harvard College'

                            if masters:
                                almText = ': Master of Liberal Arts in \
                                    Ext. Studies (ALM)'
                                if almText in dimField.text:
                                    dimField.text = (
                                        dimField.text.replace(almText, '')
                                    )

                            if addMasters:
                                parentNode = dimField.getparent()
                                dimFieldAdd = xmlTree.\
                                    SubElement(parentNode,
                                               f'{self.dimNamespace}field')
                                dimFieldAdd.attrib['mdschema'] = "thesis"
                                dimFieldAdd.attrib['element'] = "degree"
                                dimFieldAdd.attrib['qualifier'] = "level"
                                dimFieldAdd.text = 'Masters'

                    except Exception as e:
                        self.logger.info(e)
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
                continue

        # Write out updated mets file and move into place
        os.remove(metsFile)
        treeMets.write(metsFile, encoding='utf-8',
                       xml_declaration=True, pretty_print=True)

        return sendToDash

    def sh(self, *args, **kwargs):
        kwargs['stdout'] = kwargs['stderr'] = PIPE
        return run(*args, **kwargs)

    def ssh(self, command, *arguments, **kwargs):

        if self.importUserName != 'dspace':
            command = ["sudo", "-u", "dspace", *command]
        return self.sh(['ssh', f'{self.importUserName}@{self.dspaceHost}',
                        " ".join(map(quote, command))], *arguments, **kwargs)

    def get_handle(self, output):
        for line in output.split("\n"):
            if line.startswith('CREATED'):
                match = re.search(r'hdl=(\d+/\d+)', line)
                if match:
                    return match.group(1)
                else:
                    # notifyJM.log('warn', f"No handle in output:
                    # {output}", verbose)
                    self.logger.warn(f"No handle in output: {output}")

    def getInstanceData(self):
        instance_data = {
            'gsas': {
                'handle': '1/4927603',
                'bill_code': '1235',
                'bill_address': "Office of the Registrar, Faculty of \
                Arts and Sciences, Att’n Kathy Hanley, Richard A. and \
                Susan F. Smith Campus Center, 1350 Massachusetts Avenue, \
                Suite 450, Cambridge, MA 02138"
                },
            'gsd': {
                'handle': '1/13398958',
                'bill_code': '',
                'bill_address': ''
                },
            'gse': {
                'handle': '1/13056148',
                'bill_code': '1616',
                'bill_address': 'Harvard Graduate School of Education c/o \
                Jennifer Schroeder, 13 Appian Way, Cambridge, MA 02138'
                },
            'hbs': {
                'handle': '1/13398959',
                'bill_code': '622',
                'bill_address': 'Jen Mucciarone, Wyss House, Harvard Business \
                School, Soldiers Field Road, Boston, MA 02163'
                },
            'hds': {
                'handle': '1/13398960',
                'bill_code': '4130',
                'bill_address': "Harvard Divinity School Registrar's Office, \
                Andover Hall, 45 Francis Ave., Cambridge, MA 02138"
                },
            'hls': {
                'handle': '',
                'bill_code': '',
                'bill_address': ''
                },
            'hms': {
                'handle': '1/11407446',
                'bill_code': '',
                'bill_address': ''
                },
            'hsdm': {
                'handle': '1/11407445',
                'bill_code': '2553',
                'bill_address': 'Dawn DeCosta, Harvard School of Dental \
                Medicine, 188 Longwood Ave. REB 404, Boston, MA 02115'
                },
            'hsph': {
                'handle': '1/13398961',
                'bill_code': '2020',
                'bill_address': 'Karen Brown, 677 Huntington Ave, Kresge G10, \
                Boston, MA 02115'
                },
            'osc': {
                'handle': '1/37156562',
                'bill_code': '',
                'bill_address': ''
                },
            'edld': {  # use GSE billing code for now, need new code
                'handle': '1/13056148',
                'bill_code': '1616',
                'bill_address': 'Doctoral Programs Office, \
                Harvard Graduate School of Education, 13 Appian Way, \
                Longfellow Hall G039, Cambridge, MA 02138'
                },
            'college': {
                'handle': '1/4927603',  # true? same as GSAS?
                'bill_code': '',
                'bill_address': ''
                },
            'qp': {
                'handle': '1/11512821',  # per CL, go in GSE student papers
                'bill_code': '',
                'bill_address': ''
                },
            'dce': {
                'handle': '1/14557739',
                'bill_code': '',
                'bill_address': ''
                }
        }
        return instance_data
