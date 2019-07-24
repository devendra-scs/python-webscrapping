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

def writeCSVRow(row):
    csvWriter = csv.writer(csvFile)
    csvWriter.writerow(row)
    return

def parseAndWriteResponse(soup, row):
    row.clear()
    idx =0
    td=soup.find_all('table')[1].find('tbody').find('tr').find_all('td')
    
    for item in td:       
        row.insert(idx,item.text)
        idx=idx+1
    #print(row)	
    writeCSVRow(row)
    return

csvFile= open('stadium_run_corporate.csv', 'a') 

# Create a workbook and add a worksheet.
#workbook = xlsxwriter.Workbook('tcsopen10k.xlsx')
#worksheet = workbook.add_worksheet('open10k')

row=['Team','BIB', 'Name', 'Category', 'Distance', 'Laps', "Team Rank", "Team Distance", 'Team Laps']
#writeExcelRow(worksheet,count,row)
writeCSVRow(row)
bibNumber=500

while( bibNumber < 600):
    #baseURL ='https://www.sportstimingsolutions.in/share.php?event_id=50377&bib='
	#https://racetime.in/2019bsr-6/?bibNo=235C&submit=SUBMIT
    teamBIB=['A', 'B', 'C', 'D', 'E', 'F']
    baseURL='https://racetime.in/2019bsr-5/?bibNo='
    for idx in teamBIB:
        resultURL=baseURL+str(bibNumber)+str(idx)+ '&submit=SUBMIT'
        print(resultURL)
        http = urllib3.PoolManager(cert_reqs='CERT_NONE')
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        result = http.request('GET', resultURL)
        html = result.data
        if result.status == 200:
           soup = BeautifulSoup(html, "html5lib")
           try:
               parseAndWriteResponse(soup, row )
           except:
                  print("Error")
                  break
        
    bibNumber =bibNumber+1

csvFile.close()
#workbook.close()

