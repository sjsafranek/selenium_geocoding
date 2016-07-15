#!/usr/bin/python3
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import time
import csv
import multiprocessing
import threading
import os
import sys
import random


is_py2 = sys.version[0] == '2'
if is_py2:
    import Queue as queue
else:
    import queue as queue


# create results
results = []


outFile = open("geocoding_results.csv", "w")
csvwrite = csv.writer(open("geocoding_results.csv", "w"), delimiter='\t')
# csvwrite.writerows(results)


class Worker(threading.Thread):

    def __init__(self, n, queue):
        threading.Thread.__init__(self)
        self.id = str(n)
        self._queue = queue
        self.driver = webdriver.Firefox()
        self.driver.get("https://www.google.com/maps/")

    def get_lat_lng(self):
        time.sleep(random.randint(4,8))
        url = self.driver.current_url
        arr = []

        index = url.index("@")
        for item in url[index+1::].split(","):
            arr.append(item)

        lat = arr[0]
        lng = arr[1]

        return([lat, lng])

    def search_address(self, row):
        try:
            result = []
            address = ""
            search_tool = self.driver.find_element(By.ID, "searchboxinput")
            search_tool.send_keys(row["street"] + " " + row["number"])
            time.sleep(random.randint(2,4))
            # Prioritize Curaçao addresses
            found = False
            suggestions = self.driver.find_element(By.CLASS_NAME, "suggestions")
            list_items = suggestions.find_elements(By.TAG_NAME, "li")
            for item in list_items:
                if "Curaçao" in item.text:
                    found = True
                    item.click()
                    break
            # If no Curaçao addresses get first suggestion
            if not found:
                address = self.driver.find_element(By.CSS_SELECTOR, "div.suggest-left-cell").text
                if "Add a missing place to Google Maps" in address:
                    search_tool.clear()
                    return ["not found", "not found", row["street"], row["number"]]
                else:
                    search_tool.send_keys(Keys.DOWN)
                    search_tool.send_keys(Keys.ENTER)      
            time.sleep(2)
            lat_lng = self.get_lat_lng()
            # google_address = self.driver.find_element(By.ID, "searchbox").text
            google_address = search_tool.get_attribute("value")
            result = [
                lat_lng[0],
                lat_lng[1],
                row["street"],
                row["number"],
                google_address
            ]
            print("[Worker-{0}] {1} {2}".format(self.id, google_address, lat_lng))
            search_tool.clear()
            return result
        except Exception as e:
            print(e)
            search_tool = self.driver.find_element(By.ID, "searchboxinput")
            search_tool.clear()
            return ["error", "error", row["street"], row["number"]]

    def run(self):
        while True:
            # read from queue
            # stop worker when queue is empty
            if self._queue.empty():
                print("die!") # if so, exists the loop
                break
            # search locations on google maps
            else:
                time.sleep(random.randint(6,12))
                msg = self._queue.get()
                print("remaining jobs:", self._queue.qsize())
                result = self.search_address(msg)
                # print("[Worker-{0}] {1}".format(self.id, str(msg)))
                # results.append(result)
                csvwrite.writerow(result)
        self.driver.quit()





# create job queue
queue = queue.Queue()

def process():
    # spawn workers
    workers = []
    # pool_size =  multiprocessing.cpu_count() * 2
    pool_size =  multiprocessing.cpu_count() * 2
    for i in range(pool_size):
        worker = Worker(i, queue)
        workers.append(worker)

    # fill queue with jobs
    # with open("curacao_addresses.csv", encoding="utf-8") as infile:
    with open("curacao_addresses.csv", encoding="ISO-8859-1") as infile:
        reader = csv.DictReader(infile, delimiter=',')
        for row in reader:
            if row["number"] != '':
                queue.put(row)

    # start workers
    for i in range(len(workers)):
        workers[i].start()

    # wait for the thread to close down
    for i in range(len(workers)):
        worker.join()

    # print(results)
    # with open("ibobber_data.csv", "w") as file:
    #     spamwriter = csv.writer(file, delimiter='\t')
    #     spamwriter.writerows(results)


process()
print("closed")
outFile.close()




# queue = queue.Queue()
# with open("curacao_addresses.csv", encoding="ISO-8859-1") as infile:
#     reader = csv.DictReader(infile, delimiter=',')
#     for row in reader:
#         if row["number"] != '':
#             queue.put(row)

# worker = Worker(0, queue)
# msg = worker._queue.get()
# worker.search_address(msg)


# search_tool = worker.driver.find_element(By.ID, "searchboxinput")
# search_tool.send_keys(msg["street"] + " ", msg["number"])

# suggestions = worker.driver.find_element(By.CLASS_NAME, "suggestions")
# list_items = suggestions.find_elements(By.TAG_NAME, "li")
# for item in list_items:
#     if "Curaçao" in item.text:
#         item.click()
