#line 29,116
#!/usr/bin/env python

__author__ = "Devendra Singh"
__copyright__ = "Copyright 2023, Devendra Singh "
__license__ = "GPL"
__version__ = "1.0.4"
__maintainer__ = "Devendra"
__email__ = "devendra.scs@gmail.com"
__status__ = "Production"
# Script for webscrapping demonstration from https://www.sportstimingsolutions.in
# This program is only for demonstrations and author do not held any responsiblity of any license breach or malfuncitons.

from bs4 import BeautifulSoup
import urllib3
from db.dbutil import DatabaseUtil
from concurrent.futures import ThreadPoolExecutor
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',filename="web-scrapping_update.log",filemode='w')

# Update here to change event id based on the event
BASE_URL ='https://www.sportstimingsolutions.in/share.php?event_id=50377&bib='
#<END Modify>

http = urllib3.PoolManager(cert_reqs='CERT_NONE')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
dbutil = DatabaseUtil()
event_id = 3 # Update event id here
logging.info("Event ID:"+str(event_id))


def parseAndWriteResponse(event_id, record_id, soup, bibNumber, url):
    
    name=soup.find("h3",{"class":"txt-color img-padding"})
    
    if name is  None:
        logging.debug(name+ " Name not found")
        return False
    #row=['BIB', 'Name', 'Finished Time', 'Chip Pace (min/km)', 'Rank Overall', "Category Rank", "Category"]
    name = name.text.strip()
    
    finishedText= soup.find("td", {"class":"text-center neww-table he"})
    if finishedText is None:
        return False

    finishedTime= finishedText.text
    timers= soup.find_all("td", {"class":"text-center neww-table"})
    
    pace= timers[0].text
    rankOverall= timers[1].text
    genderRank =""
    if len(timers) >2:    
        genderRank = timers[2].text
    categoryRank=""
    if len(timers) >3:
        categoryRank= timers[3].text
    
    thlist= soup.find_all("th", {"class":"text-center"})
    category = thlist[len(thlist)-1].text if thlist else ""
    #print("Category:", category)
    Gender =""
    if category.find('Female') != -1:
       Gender = "Women"

    if category.find('Female') != -1:
       Gender = "Men"

    distance="10"
    try:
        distance_text = soup.find("h3",{"class":"txt-color text-center h3_race_name padding-left"})
        #print(distance_text)

        if distance_text.text is not None:
            distance_text = distance_text.text.strip()
            if distance_text.find("Half Marathon") != -1:
                distance="21"
            elif distance_text.find("Marathon") != -1:
                distance="42"
            elif distance_text.find("10K") != -1:
                distance="10"

        else:
            logging.error("Error: Distance text not found.")
    except:
        logging.error("Error in finding distance")

    splits={}
    pace=""
    
    #Write details into database
    runners_id = dbutil.insert_runners_details(name, Gender);
    print(event_id, runners_id, bibNumber, finishedTime, "".format(), rankOverall, category, categoryRank, genderRank )
    dbutil.update_row_in_db(event_id, record_id, runners_id, bibNumber, finishedTime, "".format(), rankOverall, category, categoryRank, genderRank, distance, url )
   
    return True

def process_bib(record_id,  bibNumber):
    resultURL = BASE_URL + str(bibNumber)
    logging.info(" Fetching details of BIB:"+ resultURL)
    try:
        result = http.request('GET', resultURL)
        html = result.data
        if result.status == 200:
            soup = BeautifulSoup(html, "html5lib")            
            parseAndWriteResponse(event_id, record_id, soup, bibNumber, resultURL)            
    except Exception as e:
        logging.error("Error occurred during HTTP request:", str(e))
    return True    

# Main program
# Fetch details of all bib numbers
logging.info("Fetching details of all bib numbers")

records = dbutil.get_event_all_record_list(event_id)

logging.info("Total records to process:"+ str(len(records)))
# Create a thread pool of 5 workers that process records directly from the database
count = 0
for k, v in records.items():        
    logging.info("Completed so far:"+str(count)+" Processing BIB:"+ str(v))
    process_bib(k, v)
    count += 1

logging.info("Completed successfully")
