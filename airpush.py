"""
This script calls the optimizer APIs and enerates the CSVs files.

req.json() ->

{u'2006570': [{u'SOV%': u'0.16',
               u'app': 321589,
               u'click': 1,
               u'conversion': 0,
               u'conversionrate': 0,
               u'cpa': 0,
               u'cpc': 0.2,
               u'ctr': 5,
               u'hour': 5,
               u'publisher': 172287,
               u'push': 20},
              {u'SOV%': u'0.42',
               u'app': 282414,
               u'click': 0,
               u'conversion': 0,
               u'conversionrate': 0,
               u'cpa': 0,
               u'cpc': 0,
               u'ctr': 0,
               u'hour': 5,
               u'publisher': 230391,
               u'push': 19},
            ]
}

subitem ->

{u'SOV%': u'2.86',
  u'app': 288655,
  u'click': 0,
  u'conversion': 0,
  u'conversionrate': 0,
  u'cpa': 0,
  u'cpc': 0.02,
  u'ctr': 0,
  u'hour': 5,
  u'publisher': 246565,
  u'push': 8
}

"""

import datetime
import requests
import json
import sys
import logging
import time
import os
from requests.exceptions import ConnectionError, Timeout

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('/Users/Miha/Documents/PythonScripts/Airpush/log_%s.log' % time.strftime("%Y-%m-%d"))
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
fh.setFormatter(formatter)
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

if len(sys.argv) > 1:
    process_date = sys.argv[1]
    apikey = sys.argv[2]
else:
    logger.error("no input date!")
    sys.exit(1)

logger.info("Starting OPT APPLICATION per hour for process date %s" % process_date)

# Clean error.txt if exists
try:
    os.remove('/Users/Miha/Documents/PythonScripts/Airpush/error.txt')
except Exception as e:
    pass


# Function to create an empty file
def create_error_file():
    with open('/Users/Miha/Documents/PythonScripts/Airpush/error.txt', 'w') as err:
        err.write('')


process_date = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
end_date = process_date

opt_application = '/Users/Miha/Documents/PythonScripts/Airpush/opt_application_%s.csv' % process_date

with open(opt_application, 'w') as output_file:
    output_file.write(
        'CAMPAIGN_ID,APPLICATION_ID,HOUR,PUBLISHER,PUSH,CLICK,CTR,CPC,CONVERSION,CONVERTIOSN_RATE,CPA,SOV\n')

# Write campaignIds into the cids list
cids=[]

try:
    url="http://openapi.airpush.com/getAdvertiserReports?apikey=%s&startDate=%s&endDate=%s" % (apikey, process_date, end_date)
    req=requests.get(url)
    for item in req.json()["advertiser_data"]:
        cids.append(item['campaignid'])
except Exception as e:
    logger.error("API call failed: %s" % e)
    create_error_file()

all_cids=','.join(cids)
all_hours=['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23']


# do a request for each hour in a day
for hour in all_hours:
    count=1
    url="http://openapi.airpush.com/getCampaignOptimizerData?apikey=%s&startDate=%s&endDate=%s&campaignIds=%s&reportType=hour&DRILLDOWN=application&hour=%s" % (
        apikey, process_date, end_date, all_cids, hour)
    req=requests.get(url)

    # if the HTTP status is not 200, wait 10 sec and retry
    while req.status_code != 200 and count <= 3:
        logger.info("Sleeping for 10 seconds and retrying...")
        time.sleep(10)
        req=requests.get(url)
        count+=1

    if count == 3:
        create_error_file()
        logger.error("Too many retries. Failed to create a request.")
        sys.exit(1)

    # go through all campaign IDs and write to file the specified fields
    for cid in cids:
        if cid in req.json():
            for subitem in req.json()[cid]:
                if 'publisher' in subitem:
                    with open(opt_application, 'a') as output_file:
                        output_file.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % ( \
                            cid, subitem['app'], subitem['hour'], subitem['publisher'], subitem['push'], \
                            subitem['click'], subitem['ctr'], subitem['cpc'], subitem['conversion'], \
                            subitem['conversionrate'], subitem['cpa'], subitem["SOV%"]))
                else:
                    logger.info("No data was found for %s" % cid)
        else:
            logger.info("%s not found in %s" % (cid, str(req.json())))
else:
    logger.info("Sucessfully finished OPT APPLICATION")
