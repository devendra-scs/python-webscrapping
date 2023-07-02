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
import sqlite3

DB_FILE_PATH="data/RunningData.db"

class DatabaseUtil:
    type ='DBUtil'
    def __init__(self):
        self.conn = sqlite3.connect(DB_FILE_PATH)
        self.conn_active = True
    def __del__(self):
        self.conn.close()

    def get_event_ID(self, event_name):
        EventID=0
        sql="SELECT ID from EventDetails where EventName='"+event_name+"'";
        cursor = self.conn.execute(sql);
        for row in cursor:
           EventID = row[0]
        return EventID;

    def insert_event_details(self, event_name, event_city, event_date, event_year, base_url):
        EventID=self.get_event_ID(event_name);
        
        #print("EventID:",EventID)
        if(EventID>0):
            #print("Event Already Found")
            return EventID
        
        sql="INSERT INTO EventDetails(EventName, EventCity, EventDate, EventURL, EventYear) VALUES('"+event_name+"','"+event_city+"','"+event_date+"','"+base_url+"','"+event_year+"')"
        self.conn.execute(sql);
        self.conn.commit()
        EventID=self.get_event_ID( event_name);
        
        return EventID;

    def get_runners_ID(self, name, gender):
        runners_id=0
        sql="SELECT ID from RunnersDetails WHERE Name='"+name+"' AND Gender='"+gender+"'" 
        cursor = self.conn.execute(sql);
        for row in cursor:
           runners_id = row[0]
        return runners_id;
        
    def insert_runners_details(self, name, gender):
        runners_id=self.get_runners_ID( name, gender);
            
        if(runners_id>0):        
            return runners_id
        sql="INSERT INTO RunnersDetails (Name, Gender) VALUES ( '"+name+"', '"+gender+"')";
        #print(sql)    
        self.conn.execute(sql);
        self.conn.commit()
        runners_id=self.get_runners_ID(name, gender);    
        return runners_id;
        
    def get_record_id_from_event_data_table(self, event_id, BIB):
        record_id=0
        sql="SELECT ID from EventData where EventId='"+str(event_id)+"' and BIB='"+str(BIB)+"'";
        cursor = self.conn.execute(sql)
        for row in cursor:
           record_id = row[0]
        return record_id;
        
    #BIB,Finished Time,Pace (min/km),Rank Overall,Category Rank
    def insert_row_in_db(self, event_id, runners_id, BIB, NetTime, GunTime, OverallRank, Category, CategoryRank, Distance ):
        #Check if runners data for this event already present in database
        record_id = self.get_record_id_from_event_data_table(event_id, BIB)
        
        if record_id >0:
           #print("Record already Present");
           return 0;

        sql = "INSERT INTO EventData ( BIB, RunnersID, EventID, FinishTime, GunTime, RankOverall, RankCategory, Distance,Category ) VALUES ('"+str(BIB)+ "','"+str(runners_id)+"','"+str(event_id)+"','"+ str(NetTime)+"','"+ str(GunTime)+"','"+ str(OverallRank)+"','"+ str(CategoryRank)+"','"+str(Distance)+"', '"+str(Category)+"')"
        #print("SQL",sql)
        self.conn.execute(sql);
        self.conn.commit()
        return 1;

    def Get_splits_data(self, event_id, runners_id, BIB, Distance):
        id=0
        sql="SELECT ID from SplitsDetails where EventID='"+str(event_id)+"' AND RunnersID='"+str(runners_id)+"' AND BIB='"+str(BIB)+"' AND Distance='"+str(Distance)+"'"
        #print("SQL:",sql)
        cursor = self.conn.execute(sql);
        for row in cursor:
           id = row[0]
        return id;
        
    def Insert_splits_data(self,  event_id, runners_id, BIB, Distance, Time):
        split_id = self.Get_splits_data( event_id, runners_id, BIB, Distance)
        if split_id >0:
            #print("Split Data already present")
            return True
        sql="INSERT INTO SplitsDetails(EventID, RunnersID, BIB, Distance, Time) VALUES('"+str(event_id)+"','"+str(runners_id)+"','"+str(BIB)+"','"+str(Distance)+"','"+str(Time)+"')"
        #print("Spilits Insert SQL:",sql)
        self.conn.execute(sql)
        self.conn.commit()
        return True
  