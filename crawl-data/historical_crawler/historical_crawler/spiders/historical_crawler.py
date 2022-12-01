from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import scrapy

from datetime import date, timedelta
import datetime
from datetime import timezone
from time import sleep

import pandas as pd

import os
import json
import threading

def createPathIfNotExist(path):
    isExist = os.path.exists(path)
    if not isExist:
        os.makedirs(path)

# notebookPath Không có dấu '/' ở cuối
notebookPath = '/Users/4rr311/Documents/VectorA/KHTN/Nam3/NMKHDL/Project01/Project-1/crawl-data'
dataPath = f'{notebookPath}/data'

createPathIfNotExist(notebookPath)
createPathIfNotExist(dataPath)

datetime_format = '%Y-%m-%d %H:%M:%S'

headers = {
    'Accepts': 'application/json',
    # 'X-CMC_PRO_API_KEY': '66879404-05fb-433b-b1b8-c6b2620f733c' # Đăng ký tài khoản để được cấp API KEY
}

def datetimeStringToUTCTimestamp(str_datetime, datetime_format):
    timestamp = datetime.datetime.strptime(str_datetime, datetime_format)
    timestamp = int(timestamp.replace(tzinfo=timezone.utc).timestamp())
    return timestamp

def addDayToDateString(str_date, nDayToAdd, datetime_format):
    nDay = timedelta(days=nDayToAdd)
    result = datetime.datetime.strptime(str_date, datetime_format) + nDay
    result = datetime.datetime.strftime(result, datetime_format)
    return result

def getHistoricalURLs(coinID, year):
    url_list = []
    rootURL = 'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/historical'
    for i in range(1, 13):
        str_timeStart = f'{year}-{0 if i < 10 else ""}{i}-01 00:00:00'
        str_timeEnd = f'{year}-{0 if i + 1 < 10 else ""}{i + 1}-01 00:00:00'
        
        if i == 12:
            str_timeEnd = f'{year + 1}-01-01 00:00:00'
        str_timeEnd = addDayToDateString(str_timeEnd, -1, datetime_format)

        timeStart = datetimeStringToUTCTimestamp(str_timeStart, datetime_format)
        timeEnd = datetimeStringToUTCTimestamp(str_timeEnd, datetime_format)
        
        parameters = {
            'id' : coinID,
            'convertId' : 2781,
            'timeStart' : timeStart,
            'timeEnd' : timeEnd
        }
        url = f'{rootURL}?id={coinID}&convertId={2781}&timeStart={timeStart}&timeEnd={timeEnd}'
        url_list.append(
            {
                'url' : url,
                'str_startTime' : str_timeStart,
                'str_endTime' : str_timeEnd
            }
        )
    return url_list

idList = []

id_map_path = f'{dataPath}/coinmarketcap-id-map.json'

id_map = []
with open(id_map_path, 'r') as f:
    id_map = json.load(f)

for i in id_map:
    idList.append(i['id'])

class coinHistoricalData(scrapy.Spider):
    name='historical_crawler' 
    year = 2021
    historicalDataPath = f'{dataPath}/historical-data'

    def start_requests(self):
        for coinID in idList:
            url_list = getHistoricalURLs(coinID, self.year)        

            coinIDPath = f'{self.historicalDataPath}/coin-id-{coinID}'
            createPathIfNotExist(coinIDPath)
            
            for urlInfo in url_list:
                filename = f"{urlInfo['str_startTime'].split(' ')[0]}.json"
                filepath = f'{coinIDPath}/{filename}'
                sleep(5)
                yield scrapy.Request(
                    url=urlInfo['url'], 
                    callback=self.parse, headers=headers,
                    meta={
                        'filepath' : filepath
                    }
                )
                
    def parse(self, response):
        filepath = response.meta.get('filepath')
        with open(filepath, 'w') as file:
            data = response.body.decode('utf8')
            json.dump(json.loads(data)['data'], file, indent=4)