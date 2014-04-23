__author__ = 'oski'

import glob
import gzip
import json
import threading
import time
import dateutil.parser
from pymongo import MongoClient


db = MongoClient(host='localhost', port=27017)


def iterload(file):
    buffer = ""
    dec = json.JSONDecoder()
    for line in file:
        buffer += line.strip()
        # http://bugs.python.org/issue15393
        # srednio na jeza podoba mi sie ta wrazliwosc na biale znaki
        while(True):
            try:
                r = dec.raw_decode(buffer)
            except:
                break
            yield r[0]
            buffer = buffer[r[1]:]


class WatchEventsImporter(threading.Thread):
    global db

    def __init__(self, threadId, files):
        self.threadId = threadId
        threading.Thread.__init__(self)
        self.daemon = True
        self.files = files

    def run(self):
        for file in self.files:
            with gzip.open(file, 'rb') as json_file:
                print '%s - %s' % (self.threadId, file)
                for i in iterload(json_file):
                    if i['type'] == 'WatchEvent':
                        i['created_at'] = dateutil.parser.parse(i['created_at'])
                        db.wikiteams.stargazing.insert(i)

            print ''


if __name__ == "__main__":
    files = glob.glob('/mnt/data1/wikiteams/GitHubArchive/*.gz')

    filesParts = map(None, *[iter(files)]*50)

    threads = []
    index = 0
    for filePart in filesParts:
        threads.append(WatchEventsImporter(index, filePart).start())
        index += 1

    while True:
        time.sleep(10)
