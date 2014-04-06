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

        TOKENS = [
            '2bc539d7f125c28e2d44adba01f6a9aa35373a66', #bgruszka
            #'d3c1492ba0a1baea7a31f2b1580c73c16b31c39e',
            'e92c7b5de5c5ce2225f5e1932e5e4abbbf0d05c6', #wiki-worker1
            '2364969c040e048531ee52437096f3e1e0316999', #wiki-worker2
            '6315ce5aef0478498abbff29a3703aac9dfe9bd2', #wiki-worker3
            'a4a888415301b475dcfb19c7dc04705a50c0948f', #wiki-worker4
        ]

        TOKEN_INDEX = 0
        i = 0

        for push in pushes:
            response = requests.get(push['url'].replace('github.com', 'api.github.com/repos'), headers = { 'Authorization': 'token %s' % TOKENS[TOKEN_INDEX] })

            if response.headers['X-RateLimit-Remaining'] == 1:
                if TOKEN_INDEX + 1 == len(TOKENS):
                    TOKEN_INDEX = 0
                else:
                    TOKEN_INDEX += 1

                print 'Changed token index to: %s' % TOKEN_INDEX
            else:
                print 'Requests left: %s' % response.headers['X-RateLimit-Remaining']

            if response.status_code != 200:
                continue

            jsonObject = response.json()

            if not push['actor'] in authors.keys():
                authors[push['actor']] = {}

            for file in jsonObject['files']:
                fileExtension = os.path.splitext(file['filename'])[1].split('.')[-1]

                if len(fileExtension) == 0:
                    continue

                if fileExtension in authors[push['actor']]:
                    authors[push['actor']][fileExtension] += 1
                else:
                    authors[push['actor']][fileExtension] = 1

            i += 1
            print 'Pushes left: %s' % (pushesCount - i)
        print authors
        with open('export_skills.json', 'w') as outfile:
            json.dump(authors, outfile)

        #self.print_to_csv(sorted(authors.iteritems(), key=operator.itemgetter(1), reverse=True))

if __name__ == "__main__":
    importer = EventsGetter()
    importer.open_mongo_db()
    importer.get_data()
