import requests
import github

__author__ = 'snipe'

import csv, glob, sys, gzip, json, ijson, datetime, operator, os
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

class EventsGetter():
    db = None
    csv_files = None
    csv_data = None
    first_row = None

    def open_mongo_db(self):
        self.db = MongoClient(host='localhost', port=27017)

    def print_to_csv(self, data):
        with open('../csv/export_closed_bug_issues_global.csv', 'a+') as f:
            f.write("user_name;closed_bug_issues_count\r\n")

            for item in data:
                f.write('"%s";"%s"\r\n' % (item[0], item[1]))

    def get_data(self):
        doneRepositories = 0
        fetchedIssues = []

        pushesCount = self.db.wikiteams.pushes.find().count()
        pushes = list(self.db.wikiteams.pushes.find())
        authors = {}

        i = 0

        for push in pushes:
            url = 'http://osrc.dfm.io/%s.json' % push['actor']
            response = requests.get(url)

            jsonObject = response.json()
            usage = jsonObject['usage']['day'].index(max(jsonObject['usage']['day']))

            if push['actor'] not in authors.keys():
                authors[push['actor']] = {}

            authors[push['actor']]['hour'] = usage
            authors[push['actor']]['timezone'] = jsonObject['timezone']

            i += 1
            print 'Pushes left: %s' % (pushesCount - i)

            with open('export_hours.json', 'w') as outfile:
                json.dump(authors, outfile)

if __name__ == "__main__":
    importer = EventsGetter()
    importer.open_mongo_db()
    importer.get_data()
