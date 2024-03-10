import csv
import sqlite3
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',filename="sqlitedbtojson.log",filemode='w')

#Script to covert sqlite to csv
OUTPUT_FILE_NAME="Consolidate-Report.csv"

def writeCSVRow(csvWriter, row):
    csvWriter.writerow(row)
    return

def covert_sqlite_to_csv(soup, csvWriter, row):
    query= "SELECT  BIB, RD.name, EVD.EventYear, Distance, FinishTime, Pace, OverallRank, GenderRank, Category, CategoryRank, EVD.EventCity, EVD.EventName, ED.RESULTURL FROM EventData ED,RunnersDetails RD, EventDetails EVD  WHERE ED.RunnersID=RD.ID  AND EVD.ID=ED.EventID"
    row.clear()
    cursor = conn.execute(query)
    for row in cursor:
        writeCSVRow(csvWriter, row)

    return

csvFile= open(OUTPUT_FILE_NAME,  'w', newline='') 
count=0

row=[ 'BIB Number', 'Name', 'Year','Distance','Finished Time', 'PACE(min/km)', 'Overall Rank', 'Category', 'Category Rank', 'Gender Rank', 'City', 'EventName', 'ResultURL']
csvWriter = csv.writer(csvFile)
writeCSVRow(csvWriter, row)
conn = sqlite3.connect('data/RunningData.db')
covert_sqlite_to_csv(conn,csvWriter,row)
conn.close()
csvFile.close()
logging.info("succesfully converted")
