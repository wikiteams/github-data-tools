__author__ = 'oski'

import csv
import glob
import sys
import gzip
import json
import datetime
import calendar
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


class EventsImporter():
    db = None
    csv_files = None
    csv_data = None
    first_row = None

    def get_csv_files(self):
        self.files = glob.glob('/mnt/data1/wikiteams/GitHubArchive/*.gz')
        return self.files

    def open_mongo_db(self):
        self.db = MongoClient(host='localhost', port=27017)

    def import_data(self):
        for file in self.files:
            self.first_row = None
            with gzip.open(file, 'rb') as json_file:
                print file
                for i in iterload(json_file):
                    i['created_at'] = dateutil.parser.parse(i['created_at'])
                    self.db.wikiteams.events.insert(i)
                    print '.',
            print ''


if __name__ == "__main__":
    importer = EventsImporter()
    importer.get_csv_files()
    importer.open_mongo_db()

    importer.import_data()
