__author__ = 'snipe'

import csv, glob, sys, gzip, json, ijson, datetime, operator
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
        with open('../csv/export_closed_feature_issues_global.csv', 'a+') as f:
            f.write("user_name;closed_bug_issues_count\r\n")

            for item in data:
                f.write('"%s";"%s"\r\n' % (item[0], item[1]))

    def get_data(self):
        doneRepositories = 0
        fetchedIssues = []

        issues = self.db.wikiteams.events.find({ "web_closed_author": { "$exists": True } }) #.distinct("payload.issue"))


        bugLabels = ['features', 'enhancement', 'suggestion', 'improvement', 'performance']

        usedIssues = []

        authors = {}

        for issue in issues:
            labels = self.db.wikiteams.labels.find_one({ "issue":  issue['payload']['issue']})
            if len(labels['labels']) > 0:
                foundLabels = [label for label in labels['labels'] if label in bugLabels]

                if len(foundLabels) > 0:
                    if issue['payload']['issue'] not in usedIssues:
                        usedIssues.append(issue['payload']['issue'])

                        if issue['web_closed_author'] in authors.keys():
                            authors[issue['web_closed_author']] += 1
                        else:
                            authors[issue['web_closed_author']] = 1

        self.print_to_csv(sorted(authors.iteritems(), key=operator.itemgetter(1), reverse=True))

if __name__ == "__main__":
    importer = EventsGetter()
    importer.open_mongo_db()
    importer.get_data()
