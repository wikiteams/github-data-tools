__author__ = 'snipe'

import csv, glob, sys, urllib2
from pymongo import MongoClient
from github import Github
import github

class MongoImporter():
    db = None
    csv_files = None
    csv_data = None
    first_row = None

    def open_mongo_db(self):
        self.db = MongoClient(host='localhost', port=27017)

    def import_data(self):
        g = Github('2bc539d7f125c28e2d44adba01f6a9aa35373a66')

        events = self.db.wikiteams.events.find()

        for event in  events:
            mongoEvent = self.db.wikiteams.labels.find({ "issue": event['payload']['issue'] })

            if len(list(mongoEvent)) >= 1:
                print "omijam"
                continue

            url = event['repository']['url'][19:]

            try:
                ghRepo = g.get_repo(url)
            except github.UnknownObjectException, e:
                print url
                url = urllib2.urlopen(event['repository']['url']).geturl()
                ghRepo = g.get_repo(url[19:])

            if ghRepo.has_issues:
                try:
                    issue = ghRepo.get_issue(event['payload']['number'])
                    labels = [label.name for label in issue.get_labels()]
                except github.UnknownObjectException, e:
                    labels = [None]
            else:
                labels = [None]

            if len(labels) > 0:
                data = {
                    'owner': ghRepo.owner.login,
                    'name': ghRepo.name,
                    'url': 'https://github.com/%s/%s' % (ghRepo.owner.login, ghRepo.name),
                    'number': event['payload']['number'],
                    'issue': event['payload']['issue'],
                    'labels': labels
                }

                self.db.wikiteams.labels.insert(data)
                print '.',


if __name__ == "__main__":
    importer = MongoImporter()
    importer.open_mongo_db()
    importer.import_data()
