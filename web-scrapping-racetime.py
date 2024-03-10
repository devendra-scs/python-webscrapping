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

from bs4 import BeautifulSoup
import urllib3
import csv
import sqlite3
import json
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',filename="web-scrapping_racetime.log",filemode='w')


EVENT_NAME="BANGALORE ULTRA 2022"
EVENT_CITY="Bangalore"
EVENT_DATE="18TH DECEMBER 2022"
RACE_URL="https://appapi.racetime.in/result/details?raceID=c6a17c95-fdcf-4d69-a36d-4a5e222f05ca&event="
EVENT_YEAR="2022"
START_BIB_NUMBER=71401
END_BIB_NUMBER=80000


#Change url based on event category
def get_result_url(bibNumber):
    resultURL=""
    if bibNumber <100:
        resultURL= RACE_URL+"100K&bibNo="+str(bibNumber)
    else:    
        if bibNumber <25000:
            resultURL= RACE_URL+"12.5K&bibNo="+str(bibNumber)
        else:
            if bibNumber <37000:
                resultURL= RACE_URL+"25K&bibNo="+str(bibNumber)
            else:
                if bibNumber <50000:
                    resultURL= RACE_URL+"37.5K&bibNo="+str(bibNumber)
                else:
                    if bibNumber <75000:
                        resultURL= RACE_URL+"50K&bibNo="+str(bibNumber)
                    else:
                        resultURL= RACE_URL+"75K&bibNo="+str(bibNumber)          
                
    return resultURL
                

def get_event_ID(conn, event_name):
    EventID=0
    sql="SELECT ID from EventDetails where EventName='"+EVENT_NAME+"'";
    cursor = conn.execute(sql);
    for row in cursor:
       EventID = row[0]
    return EventID;

def insert_event_details(conn):
    EventID=get_event_ID(conn, EVENT_NAME);
    
    #print("EventID:",EventID)
    if(EventID>0):
        #print("Event Already Found")
        return EventID
    
    sql="INSERT INTO EventDetails(EventName, EventCity, EventDate, EventURL, EventCity, EventYear) VALUES('"+EVENT_NAME+"','"+EVENT_CITY+"','"+EVENT_DATE+"','"+RACE_URL+"','"+EVENT_CITY+"','"+EVENT_YEAR+"')" 
    conn.execute(sql);
    conn.commit()
    EventID=get_event_ID(conn, EVENT_NAME);
    
    return EventID;

def get_runners_ID(conn, name, gender):
    runners_id=0
    sql="SELECT ID from RunnersDetails where Name='"+name+"' AND Gender='"+gender+"'" 
    cursor = conn.execute(sql);
    for row in cursor:
       runners_id = row[0]
    return runners_id;
    
def insert_runners_details(conn, name, gender):
    runners_id=get_runners_ID(conn, name, gender);
        
    if(runners_id>0):        
        return runners_id
    sql="INSERT INTO RunnersDetails (Name, Gender) VALUES ( '"+name+"', '"+gender+"')";
    #print(sql)    
    conn.execute(sql);
    conn.commit()
    runners_id=get_runners_ID(conn, name,gender);
    
    return runners_id;
    
def get_record_id_from_event_data_table(conn, event_id, BIB):
    record_id=0
    sql="SELECT ID from EventData where EventId='"+str(event_id)+"' and BIB='"+str(BIB)+"'";
    cursor = conn.execute(sql)
    for row in cursor:
       record_id = row[0]
    return record_id;
    
#BIB,Finished Time,Pace (min/km),Rank Overall,Category Rank
def insert_row_in_db(conn, event_id, runners_id, BIB, NetTime, GunTime, OverallRank, Category, CategoryRank, Distance , url):
    #Check if runners data for this event already present in database
    record_id = get_record_id_from_event_data_table(conn, event_id, BIB)
    
    if record_id >0:
       #print("Record already Present");
       return 0;

    sql = "INSERT INTO EventData ( BIB, RunnersID, EventID, FinishTime, GunTime, RankOverall, RankCategory, Distance, Category, ResultURL ) VALUES ('"+str(BIB)+ "','"+str(runners_id)+"','"+str(event_id)+"','"+ str(NetTime)+"','"+ str(GunTime)+"','"+ str(OverallRank)+"','"+ str(CategoryRank)+"','"+str(Distance)+"', '"+Category+"','"+url+"')"
    #print("SQL",sql)
    conn.execute(sql);
    conn.commit()
    return 1;


def Get_splits_data(conn, event_id, runners_id, BIB, Distance):
    id=0
    sql="SELECT ID from SplitsDetails where EventID='"+str(event_id)+"' AND RunnersID='"+str(runners_id)+"' AND BIB='"+str(BIB)+"' AND Distance='"+str(Distance)+"'"
    #print("SQL:",sql)
    cursor = conn.execute(sql);
    for row in cursor:
       id = row[0]
    return id;
    

def Insert_splits_data(conn, event_id, runners_id, BIB, Distance, Time):
    split_id = Get_splits_data(conn, event_id, runners_id, BIB, Distance)
    if split_id >0:
        #print("Split Data already present")
        return True
    sql="INSERT INTO SplitsDetails(EventID, RunnersID, BIB, Distance, Time) VALUES('"+str(event_id)+"','"+str(runners_id)+"','"+str(BIB)+"','"+str(Distance)+"','"+str(Time)+"')"
    #print("Spilits Insert SQL:",sql)    
    conn.execute(sql);
    conn.commit()
    
    return True

def parseAndWriteResponse(conn, event_id, json_data, url):
    BIB = ""
    if 'bibNo' in json_data:
        BIB = json_data['bibNo']
    else:
        #print(" Invalid Json, Bib not found:", json_data)
        return False
    Name = ""
    if 'name' in json_data:
        Name =  json_data['name']
        
    Category = ""
    if 'category' in json_data:
        Category = json_data['category']
        
    Distance = ""
    if 'distance' in json_data:
        Distance = json_data['distance']
    
    GunTime = ""
    
    if 'gunTime' in json_data:
        GunTime = json_data['gunTime']
    
    NetTime = ""
    if 'netTime' in json_data:
        NetTime = json_data['netTime']

    OverallRank = ""
    if 'overallRank' in json_data:
        OverallRank = json_data['overallRank']
        
    CategoryRank = ""
    if 'categoryRank' in json_data:
        CategoryRank = json_data['categoryRank']
    
    Gender = ""
    if 'gender' in json_data:
       Gender = json_data['gender']
    
    GenderRank = ""
    if 'genderRank' in json_data:
        GenderRank = json_data['genderRank']

#Write details into database
    runners_id = insert_runners_details(conn, Name, Gender);
    insert_row_in_db(conn, event_id, runners_id, BIB, NetTime, GunTime, OverallRank, Category, CategoryRank, Distance,url )
    #print("Inserting Splits Data")
    for item in json_data['laps']:
        Insert_splits_data(conn, event_id, runners_id, BIB, item['distance'], item['time'])
    return True

def collectData(conn, http, resultURL, event_id, url):
    result = http.request('GET', resultURL)
    
    if result.status == 200:
       json_data = result.data             
       result = json.loads(json_data)       
       parseAndWriteResponse(conn, event_id, result['data'],url)
       return True
    return False

conn = sqlite3.connect('data/RunningData.db')
#print("Inserting Event ID Details")
event_id = insert_event_details(conn);
bibNumber = START_BIB_NUMBER    
count = 0
logging.info("Started collecting data for event"+EVENT_NAME)
http = urllib3.PoolManager(cert_reqs='CERT_NONE')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

while( bibNumber < END_BIB_NUMBER):    
    resultURL=get_result_url(bibNumber)
    #print(resultURL)
    if count == 100:
        logging.info(" Fetching details of BIB:", bibNumber)
        count =0
    result = collectData(conn, http, resultURL, event_id, resultURL)
    count = count + 1    
    bibNumber = bibNumber+1

logging.info("Completed successfully")
conn.close()