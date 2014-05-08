import requests
import github

__author__ = 'snipe'

import csv, glob, sys, gzip, json, datetime, operator, os
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

    def print_to_csv(self, data, fileName):
        with open(fileName, 'a+') as f:
            f.write("user_name;hour;timezone\r\n")

            for key in data.keys():
                if len(data[key]) == 0:
                    continue

                f.write('%s"";"%s";"%s"\r\n' % (key, data[key]['hour'], data[key]['timezone']))

    def get_data(self):
        jsonObject = json.loads(open('export_hours.json', 'r').read())
        self.print_to_csv(jsonObject, 'export_hours.csv')

        jsonObject = json.loads(open('export_commit_hours.json','r').read())
        self.print_to_csv(jsonObject, 'export_commit_hours.csv')

if __name__ == "__main__":
    importer = EventsGetter()
    importer.get_data()
