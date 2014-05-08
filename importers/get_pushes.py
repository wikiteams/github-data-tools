__author__ = 'snipe'

import csv, glob, sys, gzip, json, ijson, datetime
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

class PushesGetter():
    db = None
    csv_files = None
    csv_data = None
    first_row = None

    def open_mongo_db(self):
        self.db = MongoClient(host='localhost', port=27017)

    def print_to_csv(self, data, repI):
        with open('csv/pushes/export_all_pushes.csv', 'a+') as f:
            for item in data:
                f.write('"%(repository_owner)s";"%(repository_url)s";"%(repository_name)s";"%(push_author)s";"%(pushed_at)s";"%(push_url)s";"%(commits_size)s";"%(head_sha1)s"\r\n' % item)

    def get_data(self):
        doneRepositories = 0
        fetchedPushes = []

        repositories = list(self.db.wikiteams.repositories.find())

        repI = 0
        for repository in repositories:
            ev = []
            try:
                pushes = list(self.db.wikiteams.pushes.find({ "repository.url": repository['repository_url']}))

                for push in pushes:
                    if push in fetchedPushes:
                        continue
                    else:
                        fetchedPushes.append(push)

                    tmp = {'repository_owner': push['repository']['owner'], 'repository_url': repository['repository_url'], 'repository_name': push['repository']['name'], 'push_author': push['actor'], 'pushed_at': push['created_at'], 'push_url': push['url'], 'commits_size': push['payload']['size'], 'head_sha1': push['payload']['head']}
                    ev.append(tmp)

                print "save to csv"
                self.print_to_csv(ev, repI)
                repI+=1
            except Exception, e:
                print "Error in repository: %s" % repository['repository_url']
                continue

            doneRepositories+=1

            print "Done repositories: %s" % doneRepositories

if __name__ == "__main__":
    importer = PushesGetter()
    importer.open_mongo_db()
    importer.get_data()
