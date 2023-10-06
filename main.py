import json
from datetime import datetime, timedelta
import requests
import os
import logging
import time
import re
global date 

#logging module
logger = logging.getLogger("SGXlog")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s: \t %(levelname)s \t %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)

file_handler = logging.FileHandler("SGXlog.log", mode='a')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def readDateHistory(filename):
    global date
    with open(filename,'r') as file:
        date  = json.load(file)

def latestTradingDay(trade_date):
    #handling if the trading day in the weekend then find the nearest business day
    today = datetime.today()
    diff = 1
    if today.weekday() == 0:
        diff = 3
    elif today.weekday() == 6:
        diff = 2
    else:
        diff = 1
    return (today - timedelta(days=diff)).strftime('%Y-%m-%d')

def is_valid_datetime(input_str):
    try:
        # Attempt to parse the input string as a datetime with the specified format
        datetime_obj = datetime.strptime(input_str, "%Y-%m-%d")
        
        # Check if the parsed datetime is a valid date (e.g., it's not February 30)
        if datetime_obj.strftime("%Y-%m-%d") == input_str:
            return True
        else:
            logger.error("Wrong formatted of day is inputted")
            return False
    except ValueError:
        logger.error("Day that you inputted is not existed")
        return False
def string_to_datetime(input_str, format_str):
    try:
        # Attempt to parse the input string as a datetime using the specified format
        datetime_obj = datetime.strptime(input_str, format_str)
        return datetime_obj
    except ValueError:
        # Handle the case where the input string doesn't match the specified format
        return None
def getDate():
    desired = input('Input day you want to download with format (yyyy-mm-dd):\n')
    if (is_valid_datetime(desired)):
        if datetime.today() < string_to_datetime(desired,"%Y-%m-%d"):
            logger.error("Date you inputted is not available")
            return
        return desired
    else : 
        print("Not available plz reinput:\n")
        return 
def getDateInterval():
    desired_s = input("Input start day with format (yyyy-mm-dd):\n")
    desired_e = input("Input end day with format (yyyy-mm-dd):\n")
    #handling valid date format
    if (is_valid_datetime(desired_s) and is_valid_datetime(desired_s)):
        #handling if start time is after end time 
        if (string_to_datetime(desired_s,"%Y-%m-%d") < string_to_datetime(desired_e,"%Y-%m-%d")):
            for i in date.keys():
                if string_to_datetime(desired_e,"%Y-%m-%d") >= string_to_datetime(i,"%Y-%m-%d") and string_to_datetime(desired_s,"%Y-%m-%d") <= string_to_datetime(i,"%Y-%m-%d"):
                    downloadBusinessDay(i)
            logger.info("Finish download data from %s to %s",desired_s,desired_e)
        else : 
            logger.error("Start date is after end date\n")

    else : 
        logger.error("Invalid date format\n")
        return

    return
def downloadBusinessDay(directory, max_retries=3):
    # Create folder if it doesn't exist
    if directory not in date.keys():
        logger.info("This date is not available for the database")
        return
    cvDate = str(date[directory])
    

    missingfiles = ["WEBPXTICK_DT.zip", "TickData_structure.dat", "TC.txt", "TC_structure.dat"]
    
    if os.path.exists(directory):
        metadata_file_path = os.path.join(directory, "metadata.json")
        if os.path.isfile(metadata_file_path):
            with open(metadata_file_path, "r") as metadata_file:
                metadata = json.load(metadata_file)
            #handling mising files with different name with extension
            missingfiles = [expected_file for expected_file, actual_file in metadata.items() if not os.path.exists(os.path.join(directory, actual_file))]
            
            if not missingfiles:
                logger.info('The date %s has already been downloaded.', directory)
            else:
                logger.info('Some files in %s are missing. Re-downloading...', directory)
        else:
            logger.warning('Metadata file not found in %s. Assuming all files are missing.', directory)
    else:
        logger.info('  Downloading the date %s', directory)
        
    URLpart = "https://links.sgx.com/1.0.0/derivatives-historical/"

    # Create a metadata dictionary to store file associations
    metadata = {}

    for file in missingfiles:
        retry_count = 0
        while retry_count < max_retries:
            try:
                response = requests.get(URLpart + cvDate + "/" + file)
                response.raise_for_status()  # Raise an exception if the request was unsuccessful
                logger.info('Downloading...\t %s', file)
                if not os.path.exists(directory):
                    os.mkdir(directory)

                # Extract the actual file name from the Content-Disposition header, if available
                content_disposition = response.headers.get('Content-Disposition')
                if content_disposition:
                    match = re.search(r'filename=([^;]+)', content_disposition)
                    if match:
                        actual_file_name = match.group(1)
                    else:
                        actual_file_name = file
                else:
                    actual_file_name = file

                # Save the actual file name and path in the metadata
                metadata[file] = actual_file_name
                # Save the file with the actual file name
                with open(os.path.join(directory, actual_file_name), "wb") as f:
                    f.write(response.content)
                logger.info('File downloaded\t : %s', actual_file_name)
                break  # Break out of the retry loop if the download is successful
            except requests.exceptions.RequestException as e:
                retry_count += 1
                logger.warning('Failed to download %s (Attempt %d). Error: %s', file, retry_count, str(e))
                time.sleep(3)  # Wait for a while before retrying
        else:
            logger.error('Failed to download %s after %d attempts. Giving up.', file, max_retries)

    # Save the updated metadata to a JSON file
    metadata_file_path = os.path.join(directory, "metadata.json")
    with open(metadata_file_path, "w") as metadata_file:
        json.dump(metadata, metadata_file, indent=4)


if __name__ == '__main__':
    if not os.path.exists("dateHistory.json"):
        logger.info("Date History is not found. Check for the folder again or regenerate using dateHistoryHandler.py\n")
        
    else  :
        
        curdate = latestTradingDay(datetime.today())
        with open("dateHistory.json",'r') as file : 
            date = json.load(file)
        
        current_id = list(date.values())[-1]
        
        if curdate > list(date)[-1]:
            logger.info("Stored data is not keeping up with the current date. Stored data date is  %s. Updating dateHistory to current business date:%s.",list(date)[-1], curdate)
            current_id += 1 
            #URL = "https://links.sgx.com/1.0.0/derivatives-historical/" + str(current_id) + "/TC.txt"
            #response = requests.get(URL)
            #print(response.headers)
            while  "/CustomerErrorPage.aspx" not in requests.get("https://links.sgx.com/1.0.0/derivatives-historical/" + str(current_id) + "/TC.txt").url:
                
                line = requests.get("https://links.sgx.com/1.0.0/derivatives-historical/" + str(current_id) + "/TC.txt").headers.get('Content-Disposition')
                if line  == None:
                    break

                elif line[-4:] == ".txt":
                
                    date_new = line[-12:-8] + "-" + line[-8:-6] + "-" + line[-6:-4]
                    date[date_new] = current_id
                elif line[-6:] == ".atic1" : 
                    
                    date_new = line[21:25] + "-" + line[25:27] + "-" + line[27:29]
                    date[date_new] = current_id
                current_id += 1 
                
                with open("dateHistory.json",'w') as file : 
                    
                    json_obj = json.dumps(date,indent= 4)
                    file.write(json_obj)
                    file.close()
        logger.info("Finish update dateHistory.")
        
    with open("dateHistory.json",'r') as file : 
        date = json.load(file)
    
    while True : 
        print("=====================SGX Derivative Downloader=====================")
        print("1. Download derivative data from most recent business day\n")
        print("2. Download derivative data from chosen date\n")
        print("3. Download derivative data within interval\n")
        print("4. Download full derivative data\n")
        print("5. Exit the program")
        print("===================================================================")
        choice = input("Input choice for function: ")
        if choice == "1":
            cur = latestTradingDay(datetime.today())
            if os.path.exists(cur):
                logger.debug("Folder for day %s already exist",cur)
            else : 
                downloadBusinessDay(cur)
                logger.info("Folder for day %s is downloaded",cur)
        elif choice == "2":
                downloadBusinessDay(getDate())
        elif choice == "3":
                getDateInterval()
        elif choice == "4":
                for i in date.keys():
                    downloadBusinessDay(i)
        elif choice == "x":
            logger.info("End the program")
            break
        else : 
            logger.info("Invalid choice inputted")
            continue

