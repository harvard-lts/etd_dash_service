#!/bin/env python3

#
# TME  09/03/19  Using etds config file. Set to use prod for QA (for now).
#                Added read timeout for requests.Session().
# TME  11/26/19  get_package() will now print failures when they occur
# TME  10/14/20  Use "returnStatus" to hold http return status rather
#                than "r" to help with degugging

import os

username = os.getenv("vireoUsername")
pw = os.getenv("vireoPassword")

tiers = ['qa', 'prod']

hosts = {
    'qa': 'etds.lib.harvard.edu',  # Fixme
    'prod': 'etds.lib.harvard.edu',
    }

exports = {
    'qa': 'etdusr@etds-cloud.lib.harvard.edu:/home/etdusr',  # Fixme
    'prod': 'etdusr@etds-cloud.lib.harvard.edu:/home/etdusr',
    }

collections = ["hua", "med", "bus"]

# get these from Vireo instead?
# currently from Harvard-ETD-shippingbillingtheses.03172015.xls,
# from EK's email of that date
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
        'bill_address': 'Dawn DeCosta, Harvard School of Dental Medicine, \
        188 Longwood Ave. REB 404, Boston, MA 02115'
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
    'edld': {  # use GSE billing code for now, need new code later-brs 20150629
        'handle': '1/13056148',
        'bill_code': '1616',
        'bill_address': 'Doctoral Programs Office, Harvard Graduate School of \
        Education, 13 Appian Way, Longfellow Hall G039, Cambridge, MA 02138'
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

instances = instance_data.keys()
