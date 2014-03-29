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
        events = list(self.db.wikiteams.events.find({ "web_fetched": False }).limit(100).skip(100* self.threadId))
        #events = list(self.db.wikiteams.events.find({}).limit(5000).skip(5000 * self.threadId))

        for event in events:
            page = requests.get(event['url'], verify=False)

            if page.status_code != 200:
                print "ommit"
                continue

            print 'Updating issue: %s' % event['url']

            tree = html.fromstring(page.text)
            openedDate = tree.xpath('//time[@class = "js-relative-date"]/@datetime')

            closedDate = tree.xpath('//div[contains(@class, "discussion-event-status-closed")][last()]//time[@class = "js-relative-date"]/@datetime')
            closedAuthor = tree.xpath('//div[contains(@class, "discussion-event-status-closed")][last()]//a[@class = "author"]/text()')

            if len(closedDate) == 0 or len(closedAuthor) == 0:
                event['web_fetched'] = True
                self.db.wikiteams.events.save(event)
                continue


            dateParsed = dateutil.parser.parse(closedDate[0])

            event['web_closed_date'] = dateParsed
            event['web_closed_author'] = closedAuthor[0]
            event['web_fetched'] = True

            self.db.wikiteams.events.save(event)
        
if __name__ == "__main__":
    threads = []

    db = MongoClient(host='localhost', port=27017)
    eventsCount = db.wikiteams.events.count()
    threadsCount = int(math.ceil(float(eventsCount) / 100))

    for num in xrange(0, threadsCount):
        threads.append(MongoImporter(num).start())
    
    while True:
        time.sleep(10)
