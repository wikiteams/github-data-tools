__author__ = 'snipe'

import csv, glob, sys, urllib2
from pymongo import MongoClient
from github import Github
import github, threading, time, math, requests
import dateutil.parser
from lxml import html

class MongoImporter(threading.Thread):
    def __init__(self, threadId):
        self.threadId = threadId
        threading.Thread.__init__(self)
        self.daemon = True
        self.db = MongoClient(host='localhost', port=27017)

    def run(self):
        events = list(self.db.wikiteams.events.find({ "web_opened_author": { "$exists": False } }).limit(1000).skip(1000* self.threadId))

        for event in events:
            print 'Fetching issue: %s' % event['url']

            page = requests.get(event['url'], verify=False)

            if page.status_code != 200:
                print "ommit"
                continue

            tree = html.fromstring(page.text)
            author = tree.xpath('//a[@class = "author"]/text()')[0]

            #print author

            #closedDate = tree.xpath('//div[contains(@class, "discussion-event-status-closed")]//time[@class = "js-relative-date"]/@datetime')

            event['web_opened_author'] = author
            self.db.wikiteams.events.save(event)

            time.sleep(2)
        
if __name__ == "__main__":
    threads = []

    db = MongoClient(host='localhost', port=27017)
    eventsCount = db.wikiteams.events.count()
    threadsCount = int(math.ceil(float(eventsCount) / 1000))

    for num in xrange(0, threadsCount):
        threads.append(MongoImporter(num).start())
    
    while True:
        time.sleep(10)
