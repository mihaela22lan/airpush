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

logger.info("Starting OPT APPLICATION pr hour for process date %s" % process_date)

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
cids = []

try:
    url = "http://openapi.airpush.com/getAdvertiserReports?apikey=%s&startDate=%s&endDate=%s" % (apikey, process_date, end_date)
    req = requests.get(url)
    for item in req.json()["advertiser_data"]:
        cids.append(item['campaignid'])
except Exception as e:
    logger.error("API call failed: %s" % e)
    create_error_file()

all_cids = ','.join(cids)
print all_cids

all_hours = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23']
hour = all_hours[0]
print hour

# For each campaingID, get the optimizer data

for hour in all_hours:
    try:
        url = "http://openapi.airpush.com/getCampaignOptimizerData?apikey=%s&startDate=%s&endDate=%s&campaignIds=%s&reportType=hour&DRILLDOWN=application&hour=%s" % (
            apikey, process_date, end_date, all_cids, hour)
        req = requests.get(url)
        print url
        while True:
            if req.status_code == 200:
                print ('The hour is %s' % hour)
                for subitem in req.json():
                    with open(opt_application, 'a') as output_file:
                        output_file.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % ( \
                            subitem['hour'], subitem['app'], item2, subitem['publisher'], subitem['push'], \
                            subitem['click'], subitem['ctr'], subitem['cpc'], subitem['conversion'], \
                            subitem['conversionrate'], subitem['cpa'], subitem["SOV%"]))
                break
            else:
                logger.error("Status: %s. Retrying." % req.status_code)
    except Exception as e:
        logger.error("Error fetching Optimizer Application data: %s" % e)

logger.info("Data loaded successfully for %s" % all_cids)


