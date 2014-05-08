__author__ = 'snipe'

import csv, glob, sys, urllib2
from pymongo import MongoClient
from github import Github
import github, threading, time, math, requests
from lxml import html

class MongoImporter(threading.Thread):
    def __init__(self, threadId):
        self.threadId = threadId
        threading.Thread.__init__(self)
        self.daemon = True
        self.db = MongoClient(host='localhost', port=27017)

    def run(self):
        events = list(self.db.wikiteams.events.find().limit(10000).skip(10000* self.threadId))

        for event in events:
            mongoEvent = self.db.wikiteams.labels.find({ "issue": event['payload']['issue'] })

            if len(list(mongoEvent)) >= 1:
                print "ommiting"
                continue
            else:
                print 'Fetching issue: %s' % event['url']

                page = requests.get(event['url'])
                tree = html.fromstring(page.text)
                labels = tree.xpath('//span[contains(@class, "label")]/text()')

                data = {
                    'repository_owner': event['repository']['owner'],
                    'repository_name': event['repository']['name'],
                    'repository_url': event['repository']['url'],
                    'number': event['payload']['number'],
                    'issue': event['payload']['issue'],
                    'labels': labels
                }
                
                self.db.wikiteams.labels.insert(data)
        
if __name__ == "__main__":
    threads = []

    db = MongoClient(host='localhost', port=27017)
    eventsCount = db.wikiteams.events.count()
    threadsCount = int(math.ceil(float(eventsCount) / 10000))

    for num in xrange(0, threadsCount):
        threads.append(MongoImporter(num).start())
    
    while True:
        time.sleep(10)
