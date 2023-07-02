import csv
import sqlite3

#Script to covert sqlite to csv
OUTPUT_FILE_NAME="Consolidate-Report.csv"

def writeCSVRow(csvWriter, row):
    csvWriter.writerow(row)
    return

def covert_sqlite_to_csv(soup, csvWriter, row):
    query= "SELECT  RD.name, BIB, EVD.EventYear,Distance, FinishTime, Pace, RankOverall, RankCategory, EVD.EventCity, EVD.EventName FROM EventData ED,RunnersDetails RD, EventDetails EVD  WHERE ED.RunnersID=RD.ID  AND EVD.ID=ED.EventID"
    row.clear()
    cursor = conn.execute(query);
    for row in cursor:
        writeCSVRow(csvWriter, row)
        
    return

csvFile= open(OUTPUT_FILE_NAME,  'w', newline='') 
count=0

row=['Name', 'BIB Number','Year','Distance','Finished Time', 'PACE(min/km)', 'Rank Overall', "Category Rank", "City", "EventName"]
csvWriter = csv.writer(csvFile)
writeCSVRow(csvWriter, row)
conn = sqlite3.connect('data/RunningData.db')
covert_sqlite_to_csv(conn,csvWriter,row)
conn.close()
csvFile.close()
print("Successfully Coverted");
