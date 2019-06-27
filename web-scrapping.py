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
import xlsxwriter

def writeExcelRow(worksheet, count, row):
    col=0
    for item in row:
        worksheet.write(count, col, item)
        col=col+1
    return

def writeCSVRow(row):
    csvWriter = csv.writer(csvFile)
    csvWriter.writerow(row)
    return

def parseAndWriteResponse(soup, row, count, bibNumber):
    row.clear()
    row.insert(0,bibNumber)
    name=soup.find("h3",{"class":"txt-color img-padding"})
    #print(name.text)
    if name is  not None:
        row.insert(1,name.text)
        finishedTime= soup.find("td", {"class":"text-center neww-table he"})
        #print(finishedTime.text)
        row.insert(2,finishedTime.text)
        timers= soup.find_all("td", {"class":"text-center neww-table"})
        row.insert(3,timers[0].text)
        row.insert(4,timers[1].text)
        row.insert(5,timers[2].text)
        headers=soup.find_all("th", {"class":'text-center'})
        row.insert(6,headers[6].text)
        #writeExcelRow(worksheet,count,row)
        writeCSVRow(row)
        count=count+1
    return

csvFile= open('tcs10k2019.csv', 'a') 

# Create a workbook and add a worksheet.
workbook = xlsxwriter.Workbook('tcsopen10k.xlsx')
worksheet = workbook.add_worksheet('open10k')

count=0

row=['BIB', 'Name', 'Finished Time', 'Chip Pace (min/km)', 'Rank Overall', "Category Rank", "Category"]
writeExcelRow(worksheet,count,row)
writeCSVRow(row)
count=count+1
bibNumber=400

while( bibNumber < 241000):
    baseURL ='https://www.sportstimingsolutions.in/share.php?event_id=50377&bib='
    resultURL=baseURL+str(bibNumber) 
    print(resultURL)
    http = urllib3.PoolManager(cert_reqs='CERT_NONE')
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    result = http.request('GET', resultURL)
    html = result.data
    if result.status == 200:
        soup = BeautifulSoup(html, "html5lib")
        parseAndWriteResponse(soup, row, count, bibNumber)
    bibNumber =bibNumber+1

csvFile.close()
workbook.close()

