#!/usr/bin/env python

__author__ = "Devendra Singh"
__copyright__ = "Copyright 2019, Devendra SIngh "
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Devendra"
__email__ = "devendra.scs@gmail.com"
__status__ = "Production"
# This is test python program for we scrapping, I have used this to fetch running data from TCS Open10k event 
# This will create .csv and .excel files after fetching data
# This program is only for demonstrations and author do not held any responsiblity of any license breach or malfuncitons. 
import requests
from bs4 import BeautifulSoup
import urllib3
import base64
from db.dbutil import DatabaseUtil
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',filename="web-scrapping-timing-tech.log",filemode='w')



#Change below values 
EVENT_NAME="IDBI Federal Life Insurance New Delhi Marathon 2019"
EVENT_CITY="Delhi"
EVENT_DATE="24 Feb 2019"
EVENT_YEAR="2019"
START_BIB_NUMBER=25100
END_BIB_NUMBER=30000
BASE_URL ="https://www.timingindia.com/my-result-details/"

  
def getURL(bibNumber):
#https://www.timingindia.com/includes/details.php?bib=MjY4NzY,&tble=dGltaW5nX3IxODAyX2RlbG1hcl9obQ,,&eid=SURCSSBGZWRlcmFsIExpZmUgSW5zdXJhbmNlIE5ldyBEZWxoaSBNYXJhdGhvbiAyMDE4
#https://www.timingindia.com/includes/details.php?bib=NDUxMjM=,&eid=SURCSSBGZWRlcmFsIExpZmUgSW5zdXJhbmNlIE5ldyBEZWxoaSBNYXJhdGhvbiAyMDE4

    ENCODED_EVENT_NAME_YEAR =str(bibNumber)+":timing_r1902_delmar_hm:IDBI Federal Life Insurance New Delhi Marathon 2019"
    URL = BASE_URL+base64.b64encode(str(ENCODED_EVENT_NAME_YEAR).encode("ascii")).decode("ascii")
    return URL

def parseAndWriteResponse(dbutil, event_id, soup, bibNumber, url):
    
    distance = soup.find_all("h3", {"id": "head"})
    distance_found = False
    for dist in distance:
        distance = dist.text.strip()
        distance_found = True

    if distance_found is False:
       #print("Not found")
       return False
    
    if distance.find("10K") !=-1:
        distance ="10KM"
    elif distance.find("HALF") !=-1:
         distance ="21.1"
    elif distance.find("Marathon") !=-1:
         distance ="42.2"
    
    table = soup.find("table",class_='table table-curved tborder')
    name =""
    Gender =""
    finishedTime=""
    rankOverall=""
    category=""
    categoryRank="" 
    distance=""
    splits={}
    pace=""
    
    for row in table.find_all("tr"):
        col = row.find_all("td")
        k = col[0].text.strip()
        val = col[1].text.strip()
        
        if k.find("Name") !=-1:
             name = val
        elif k.find("Gender")!=-1:
             Gender = val
        elif k.find("Category Rank")!=-1:
            categoryRank = val
        elif k.find("Category")!=-1:
            category= val
        elif k.find("Rank")!=-1:
            rankOverall= val
            if len(val)<1:
                return False
        elif k.find("Split")!=-1:
            if len(val)<=1:
                continue
            splits[k]= val.split()[0]
        elif k.find("Net Time")!=-1:
             finishedTime = val

    #print("Name", name, " Gender:", Gender," Category:", category, " RankOverAll:", rankOverall," CategoryRank:", categoryRank, " FinishedTime:", finishedTime," Pace:", pace)
    
    runners_id = dbutil.insert_runners_details(name, Gender);
    dbutil.insert_row_in_db(event_id, runners_id, bibNumber, finishedTime, pace, rankOverall, category, categoryRank, '21.1', "", url )
    for key in splits:
        dbutil.Insert_splits_data(event_id, runners_id, bibNumber, key, splits[key])
        #print("Key:", key, " Val:", splits[key])
   
    return True


    
dbutil = DatabaseUtil()
bibNumber = START_BIB_NUMBER
#http = urllib3.PoolManager(cert_reqs='CERT_NONE')
#urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
count = 0
event_id = dbutil.insert_event_details(EVENT_NAME, EVENT_CITY, EVENT_DATE, EVENT_YEAR,BASE_URL)

while( bibNumber < END_BIB_NUMBER):
    
    resultURL=getURL(bibNumber)
    logging.info("Fetching details of BIB:", bibNumber, "URL:"+ resultURL)
    #if bibNumber % 100 == 0:
    #print(" Fetching details of BIB:", bibNumber)
        
    #result = http.request('GET', resultURL)
    result = requests.get(resultURL)    
    if result.status_code >= 200 and result.status_code < 300:
       html = result.content
       soup = BeautifulSoup(html, "html.parser")
       parseAndWriteResponse(dbutil, event_id, soup, bibNumber, resultURL)
       
    bibNumber = bibNumber+1
    
logging.info("Completed successfully")   
