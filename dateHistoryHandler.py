import json
from datetime import datetime, timedelta
import requests
import os
import logging
import time
import json
days = 5517
root_URL = "https://links.sgx.com/1.0.0/derivatives-historical/"

#Build log system for this file 
logger = logging.getLogger("SGXlog")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s: \t %(levelname)s \t %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)

file_handler = logging.FileHandler("handler.log", mode='a')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)



def createDateHistory(days : int):
    for i in range(1,days+1):
        if not os.path.exists("DateHistory.txt"):

            file1 = open("DateHistory.txt", "w") 
            URL = root_URL + str(i) + "/TC.txt"
            response = requests.get(URL)
            header =  response.headers.get('Content-Disposition')
            file1.write(header)
            file1.close()

def createDateHistoryAuto():
    current_id = 1 
    while  "/CustomerErrorPage.aspx" not in requests.get("https://links.sgx.com/1.0.0/derivatives-historical/" + str(current_id) + "/TC.txt").url: 
        if not os.path.exists("DateHistory.txt"):
            file1 = open('DateHistory.txt','w')
            URL = root_URL + str(current_id) + "/TC.txt"
            response = requests.get(URL)
            header =  response.headers.get('Content-Disposition')
            file1.write(header)
            file1.close()

def processDateHistory(filename):
    d = {}
    file  = open(filename,'r')
    lines = file.read().splitlines()
    count = 0
    outfile = open("dateHistory.json","w")
    for line in lines:         
        count += 1 
        print(line)
        if line == "None" :
            continue
        elif line[-4:] == ".txt":
            
            date = line[-12:-8] + "-" + line[-8:-6] + "-" + line[-6:-4]
            print(date)
            d[date] = count
        elif line[-6:] == ".atic1" : 
            print(len(line))
            date = line[21:25] + "-" + line[25:27] + "-" + line[27:29]
            d[date] = count
    print(d)
    json_obj = json.dumps(d,indent= 4)
    outfile.write(json_obj)

if __name__ == '__main__':
    createDateHistoryAuto()
    #createDateHistory(5519) #
    processDateHistory('DateHistory.txt')
