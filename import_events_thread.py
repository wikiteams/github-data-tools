__author__ = 'snipe'

import csv, glob, sys, gzip, json, datetime, calendar, threading, time
import dateutil.parser
from pymongo import MongoClient
from itertools import izip_longest, permutations

def iterload(file):
    buffer = ""
    dec = json.JSONDecoder()
    for line in file:
        buffer = buffer.strip(" \n\r\t") + line.strip(" \n\r\t")
        while(True):
            try:
                r = dec.raw_decode(buffer)
            except:
                break
            yield r[0]
            buffer = buffer[r[1]:].strip(" \n\r\t")

class EventsImporter(threading.Thread):
    db = None

    def __init__(self, threadId, files):
        self.threadId = threadId
        threading.Thread.__init__(self)
        self.daemon = True
        self.db = MongoClient(host='localhost', port=27017)
        self.files = files

    def run(self):
        for file in self.files:
            with gzip.open(file, 'rb') as json_file:
                print '%s - %s' % (self.threadId, file)
                for i in iterload(json_file):
                    i['created_at'] = dateutil.parser.parse(i['created_at'])
                    self.db.github.events.insert(i)

            print ''


if __name__ == "__main__":
    files = glob.glob('../githubarchive/*.gz')

    filesParts = map(None, *[iter(files)]*50)
 
    threads = []
    index = 0
    for filePart in filesParts:
       threads.append(EventsImporter(index, filePart).start())
       index += 1

    while True:
        time.sleep(10)
