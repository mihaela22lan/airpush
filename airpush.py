"""
This script calls the optimizer APIs and enerates the CSVs files.
"""
import datetime
import requests
import sys
import logging
import time
import os
from requests.exceptions import ConnectionError, Timeout

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('C:\IN_ORA\Quick Solutions\DSP\AIRPUSH\log_%s.log' % time.strftime("%Y-%m-%d"))
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

logger.info("Starting OPT APPLICATION pr hour for process date %s" % process_date)

# Clean error.txt if exists
try:
    os.remove('C:\IN_ORA\Quick Solutions\DSP\AIRPUSH\error.txt')
except Exception as e:
    pass


# Function to create an empty file
def create_error_file():
    with open('C:\IN_ORA\Quick Solutions\DSP\AIRPUSH\error.txt', 'w') as err:
        err.write('')


process_date = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
end_date = process_date

opt_application = 'C:\IN_ORA\Quick Solutions\DSP\AIRPUSH\opt_application_%s.csv' % process_date

with open(opt_application, 'w') as output_file:
    output_file.write(
        'CAMPAIGN_ID,APPLICATION_ID,HOUR,PUBLISHER,PUSH,CLICK,CTR,CPC,CONVERSION,CONVERTIOSN_RATE,CPA,SOV\n')

# Write campaignIds into the cids list
cids = []

try:
    url = "http://openapi.airpush.com/getAdvertiserReports?apikey=%s&startDate=%s&endDate=%s" % (
    apikey, process_date, end_date)
    req = requests.get(url)
    for item in req.json()["advertiser_data"]:
        cids.append(item['campaignid'])
except Exception as e:
    logger.error("API call failed: %s" % e)
    create_error_file()

all_cids = ','.join(cids)
print all_cids

all_hours = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17',
             '18', '19', '20', '21', '22', '23']
hour = all_hours[0]
print hour

# For each campaingID, get the optimizer data
try:
    url = "http://openapi.airpush.com/getCampaignOptimizerData?apikey=%s&startDate=%s&endDate=%s&campaignIds=%s&reportType=hour&DRILLDOWN=application&hour=%s" % (
    apikey, process_date, end_date, all_cids, hour)
    req = requests.get(url)
    print url
    for item1 in cids:
        for item2 in all_hours:
            while True:
                if req.status_code == 200:
                    print ("Item1 is %s & item2 is %s" % (item1, item2))
                    for subitem in req.json()(item1, item2):
                        with open(opt_application, 'a') as output_file:
                            output_file.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
                            item1, subitem['app'], item2, subitem['publisher'], subitem['push'], \
                            subitem['click'], subitem['ctr'], subitem['cpc'], subitem['conversion'],
                            subitem['conversionrate'], subitem['cpa'], subitem["SOV%"]))
                    break
                else:
                    logger.error("Status: %s. Retrying." % req.status_code)
except Exception as e:
    logger.error("Error fetching Optimizer Application data: %s" % e)

logger.info("Data loaded successfully for %s" % all_cids)

'''
logger.error(req.text)
print ("API call failed: %s" % e)
create_error_file()
logger.info("Ending OPT APPLICATION3 for process date %s" % process_date)
'''