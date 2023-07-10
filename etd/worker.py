import requests
import logging
import os

# import lxml.etree as xmlTree
# import os, re, sys, shutil, xml.sax
import re
from datetime import datetime
from subprocess import run, PIPE
from vireo import instance_data
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
    aipPattern = '.+_(\\d+).zip'
    reAipPackage = re.compile(aipPattern)

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
        dspaceHome = "/home/dspace"
        dspaceImportDir = f'{dspaceHome}/import'
        importUserName = 'dspace'

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
            proc = self.sh(['unzip', aipFile])
            if proc.returncode > 0:
                # notifyJM.log('fail', f"Failed to 
                # unzip {aipDir}/{aipFile}", verbose)
                self.logger.error(f"Failed to unzip {aipDir}/{aipFile}")
                continue

            os.remove(aipFile)
            
            # Rewrite mets file remapping a few elements
            if not self.rewrite_mets(aipDir, batch, schoolCode): 
                continue

            # Zip package back up with updated mets file
            zipWithArgs = ['zip', aipFile]
            for file in os.listdir('.'):
                zipWithArgs.append(file)
            proc = self.sh(zipWithArgs)
            if proc.returncode > 0:
                # notifyJM.log('fail', f"The command {zipWithArgs} 
                # failed in {aipDir}", verbose)
                self.logger.error(f"The command {zipWithArgs} \
                                  failed in {aipDir}")
                continue
            
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

            collection_handle = instance_data[schoolCode]['handle']
            dashImportFile = f'{dspaceImportDir}/proquest/' + aipFile		
            proc = self.sh(['scp', '-r', aipFile, f'{importUserName} \
                            @{dspaceHost}:{dashImportFile}'])
            if proc.returncode == 0:
                # notifyJM.log('pass', f"Copied AIP package to 
                # {importUserName}@{dspaceHost}:{dashImportFile}", verbose)
                self.logger.info(f"Copied AIP package to {importUserName} \
                                 @{dspaceHost}:{dashImportFile}")
            else:
                # notifyJM.log('fail', f"Failed to send {aipDir}/{aipFile} to \
                # {importUserName}@{dspaceHost}:{dashImportFile}", verbose)
                self.logger.error("Failed to send {aipDir}/{aipFile} to \
                                  {importUserName}@{dspaceHost}:{dashImportFile}")
                continue
            
            # Import to DASH
            sub2handle = {} # this is source of truth, we keep mapfile as an on-disk record
            with open(os.path.join(proquestOutDir, "mapfile"), 'w') as mapfile:

                sub_id = aipFile.rstrip('.zip')			
                result = ssh([DSPACE_COMMAND, 'packager', '-s', '-w',
                            '-t', 'AIP',
                            '-e', 'hl_dash_admin@harvard.edu',
                            '-p', collection_handle,
                            dashImportFile])
                if result.returncode == 0:
                    # notifyJM.log('pass', f"Imported {aipDir}/{aipFile} to DSpace", verbose)
                    self.logger.info(f"Imported {aipDir}/{aipFile} to DSpace")
                    handle = get_handle(str(result.stdout, 'utf-8'))
                    sub2handle[sub_id] = handle
                    self.logger.info(f'{sub_id} {handle}', file=mapfile)
                else:
                    message = f"DSpace import failed for {aipDir}/{aipFile}.\n"
                    message += f"Command run was: {' '.join(result.args)}\n"
                    message += f"Process return code was: {result.returncode}\n"
                    message += f"Process output was:\n\n{str(result.stdout, 'utf-8')}\n"
                    message += f"Process error was:\n\n{str(result.stderr, 'utf-8')}\n"
                    notifyJM.log('fail', message, verbose)
                    continue
                
            handles = set(sub2handle.values())

            # run citation-update curation task
            result = ssh([DSPACE_COMMAND, 'curate', '-t', 'citation-update', '-i', collection_handle])
            if result.returncode == 0:
                # notifyJM.log('pass', "Ran citation-update curation task", verbose)
                self.logger.info("Ran citation-update curation task")
            else:
                message = "Couldn't run citation-update curation task"
                message += f'Command run was: {" ".join(result.args)}'
                message += f"Process return code was: {result.returncode}"
                message += f"Process output was:\n\n{str(result.stdout, 'utf-8')}"
                message += f"Process error was:\n\n{str(result.stderr, 'utf-8')}"
                notifyJM.log('warn', message, verbose)
            
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
        from xfer_files import xfer_files
        # global notifyJM
        dropboxServer = os.getenv("dropboxServer")
        dropboxUser = os.getenv("dropboxUser")
        homeDir = os.getenv("homeDir")
        privateKey = f'{homeDir}/.ssh/kant_id_rsa'
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
    def rewrite_mets(aipDir, batch, schoolCode):
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
            except:
                notifyJM.log('fail', f'Failed to load xml from {metsFile}', verbose)
                return False
        else:
            notifyJM.log('fail', f"{metsFile} not found", verbose)
            notifyJM.report('stopped')
            return False
            
        # Find and remapped fields	
        for dimField in rootMets.iter(f'{dimNamespace}field'):
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
                    except:
                        continue
                                    
                elif dimField.attrib['element'] == 'dc': 
                    try:
                        if dimField.attrib['qualifier'] == 'subject':
                            dimField.attrib['element'] = 'subject'
                            dimField.attrib.pop('qualifier')
                    except:
                        continue

            elif dimField.attrib['mdschema'] == 'thesis':
                if dimField.attrib['element'] == 'degree':
                    if dimField.attrib['qualifier'] == 'date':
                        dimField.attrib['mdschema'] = 'dc'
                        dimField.attrib['element'] = 'date'
                        dimField.attrib['qualifier'] = 'submitted'
                    
                    elif dimField.attrib['qualifier'] == 'name':

                        # College Undergraduate	and DCE Masters
                        if dimField.text == 'A.B.' or dimField.text == 'S.B.' or dimField.text == 'A.L.M.':
                            dimField.text = dimField.text.replace('.', '')						

                            # College Undergraduate	
                            if dimField.text == 'AB' or dimField.text == 'SB':
                                underGrad = True
                                parentNode = dimField.getparent()
                                dimFieldAdd = xmlTree.SubElement(parentNode, f'{dimNamespace}field')
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
                    notifyJM.log('info', f"{aipDir}/mets.xml: Embargo information found", verbose)
                    
                    try:
                        if dimField.attrib['qualifier'] == 'terms' or dimField.attrib['qualifier'] == 'until':
                            match = re5digitDate.match(dimField.text)
                            if match:
                            
                                # These do not go to Dash
                                if schoolCode == 'college' and dimField.attrib['qualifier'] == 'terms':
                                    if dimField.text == '10000-01-01':
                                        notifyJM.log('warn', f"Batch {batch} will not be sent to Dash due to permanent Embargo information found", verbose)
                                        sendToDash = False
                                else:
                                    dimField.text = f'9999{match.group(1)}'
                    except:
                        continue

        # Do another pass to remove any html tags and to set College Undergraduate and masters	
        for dimField in rootMets.iter(f'{dimNamespace}field'):
                            
            # Remove any html tags
            if dimField.text:
                textNoHtml = re.sub('</*\w*>?', '', dimField.text)
                dimField.text = textNoHtml

            if dimField.attrib['mdschema'] == 'thesis':			
                if dimField.attrib['element'] == 'degree':
                    try:
                        if dimField.attrib['qualifier'] == 'grantor':

                            # Remove if found
                            dimField.text = re.sub('\s*-\s*Pre\s*20\d\d', '', dimField.text)

                            if underGrad:
                                if dimField.text == 'Harvard University Engineering and Applied Sciences':
                                    dimField.text = 'Harvard College'
                    
                            if masters:
                                if ': Master of Liberal Arts in Ext. Studies (ALM)' in dimField.text:
                                    dimField.text = dimField.text.replace(': Master of Liberal Arts in Ext. Studies (ALM)', '')

                            if addMasters:
                                parentNode = dimField.getparent()
                                dimFieldAdd = xmlTree.SubElement(parentNode, f'{dimNamespace}field')
                                dimFieldAdd.attrib['mdschema'] = "thesis"
                                dimFieldAdd.attrib['element'] = "degree"
                                dimFieldAdd.attrib['qualifier'] = "level"
                                dimFieldAdd.text = 'Masters'

                    except:
                        continue

        # Replace any 5 digit year dates in right context
        for rightsContext in rootMets.iter(f'{rightsNamespace}Context'):
            try:
                match = re5digitDate.match(rightsContext.attrib['start-date'])
                if match:
                    rightsContext.attrib['start-date'] = f'9999{match.group(1)}'
            except:
                continue
            
        # Write out updated mets file and move into place
        os.remove(metsFile)
        treeMets.write(metsFile, encoding='utf-8', xml_declaration=True, pretty_print=True)
        
        return sendToDash

    def sh(*args, **kwargs):
        kwargs['stdout'] = kwargs['stderr'] = PIPE
        return run(*args, **kwargs)

    def ssh(command, *arguments, **kwargs):

        if importUserName != 'dspace':
            command = ["sudo", "-u", "dspace", *command]
        return sh(['ssh', f'{importUserName}@{dspaceHost}', " ".join(map(quote, command))], *arguments, **kwargs)

    def get_handle(output):
        for line in output.split("\n"):
            if line.startswith('CREATED'):
                match = re.search(r'hdl=(\d+/\d+)', line)
                if match: return match.group(1)
                else: notifyJM.log('warn', f"No handle in output: {output}", verbose)

