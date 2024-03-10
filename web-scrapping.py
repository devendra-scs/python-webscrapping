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
import logging
from bs4 import BeautifulSoup
import urllib3
from db.dbutil import DatabaseUtil
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',filename="webscrapping.log",filemode='w')

#<BEGIN Modify>
#Change below paramenter
EVENT_NAME="Tata Mumbai Marathon 2024"
EVENT_CITY="Mumbai"
EVENT_DATE="JANUARY 21, 2024"
EVENT_YEAR="2024"
START_BIB_NUMBER=50000
END_BIB_NUMBER=50001
BASE_URL ='https://www.sportstimingsolutions.in/share.php?event_id=78282&bib='
#<END Modify>

http = urllib3.PoolManager(cert_reqs='CERT_NONE')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
dbutil = DatabaseUtil()
event_id = dbutil.insert_event_details(EVENT_NAME, EVENT_CITY, EVENT_DATE, EVENT_YEAR,BASE_URL)
logging.info("Event ID:"+str(event_id))


def parseAndWriteResponse(event_id, soup, bibNumber, url):
    name=soup.find("h3",{"class":"txt-color img-padding"})
    
    if name is  None:
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
        logging.error("This is an error message")

    splits={}
    pace=""
    
    #Write details into database
    runners_id = dbutil.insert_runners_details(name, Gender);
    #print(event_id, runners_id, bibNumber, finishedTime, "".format(), rankOverall, category, categoryRank, genderRank )
    dbutil.insert_row_in_db(event_id, runners_id, bibNumber, finishedTime, "".format(), rankOverall, category, categoryRank, genderRank, distance, url )
    table =soup.find_all("table", class_='table')
    # Parse split data
    #Interval	Gun Time	Chip Time	Chip Pace (min/km)	Speed
    for row in table:
        td = row.find_all('td')
        idx=0 
        for columns in td:
        # print("Col:",columns)
            val = columns.text.strip()
            if idx==0:
               distance = val
            elif idx == 1:
               gunTime = val
            elif idx == 2:
               chipTime = val
            elif idx == 3:
               pace = val
            elif idx == 4:
               speed = val

               dbutil.Insert_splits_data(event_id, runners_id, bibNumber, distance, gunTime)
               idx =0
               continue
            idx = idx+1
    return True

def process_bib(bibNumber):
    resultURL = BASE_URL + str(bibNumber)
    logging.info(" Fetching details of BIB:"+resultURL)
    if bibNumber % 100 == 0:
        logging.info(" Fetching details of BIB:"+ bibNumber)    
    try:
        result = http.request('GET', resultURL)
        html = result.data
        if result.status == 200:
            soup = BeautifulSoup(html, "html5lib")            
            parseAndWriteResponse(event_id, soup, bibNumber, resultURL)            
    except Exception as e:
        logging.error("Error occurred during HTTP request:", str(e))
    return True    

with ThreadPoolExecutor(max_workers=5) as executor:
    bibNumbers = range(START_BIB_NUMBER, END_BIB_NUMBER)
    executor.map(process_bib, bibNumbers)
    logging.info("Thread pool completed succesfully")

    
logging.info("completed succesfully")
