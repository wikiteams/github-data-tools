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
        events = list(self.db.wikiteams.events.find({ "web_start_date": { "$exists": False } }).limit(10000).skip(10000* self.threadId))

        for event in events:
            print 'Fetching issue: %s' % event['url']

            page = requests.get(event['url'])

            if page.status_code != 200:
                print "ommit"
                continue

            tree = html.fromstring(page.text)
            openedDate = tree.xpath('//time[@class = "js-relative-date"]/@datetime')

            #closedDate = tree.xpath('//div[contains(@class, "discussion-event-status-closed")]//time[@class = "js-relative-date"]/@datetime')

            dateParsed = dateutil.parser.parse(openedDate[0])

            event['web_start_date'] = dateParsed

            self.db.wikiteams.events.save(event)
        
if __name__ == "__main__":
    threads = []

    db = MongoClient(host='localhost', port=27017)
    eventsCount = db.wikiteams.events.count()
    threadsCount = int(math.ceil(float(eventsCount) / 10000))

    for num in xrange(0, threadsCount):
        threads.append(MongoImporter(num).start())
    
    while True:
        time.sleep(10)
