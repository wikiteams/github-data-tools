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

class EventsGetter():
    db = None
    csv_files = None
    csv_data = None
    first_row = None

    def open_mongo_db(self):
        self.db = MongoClient(host='localhost', port=27017)

    def print_to_csv(self, data, repI):
        with open('csv/additionals3/export_all.csv', 'a+') as f:
        #with open('csv/additionals3/export_%s.csv' % repI, 'a+') as f:
            #f.write("repository_owner;repository_url;repository_name;issue;status;opened_by;closed_by;opened_at;closed_at;diff_in_minutes;diff_in_hours;diff_in_days\r\n")

            for item in data:
                if item['closed_at'] is not None and item['opened_at'] is not None:
                    diff_in_minutes = (item['closed_at'] - item['opened_at']).total_seconds()
                    diff_in_minutes = round(float(diff_in_minutes) / 60, 2)
                    item.update({'diff_in_minutes': diff_in_minutes})

                    diff_in_hours = (item['closed_at'] - item['opened_at']).total_seconds()
                    diff_in_hours = round(float(diff_in_hours) / 3600, 2)
                    item.update({'diff_in_hours': diff_in_hours})

                    diff_in_days = (item['closed_at'] - item['opened_at']).total_seconds()
                    diff_in_days = round(float(diff_in_days) / 86400, 2)
                    item.update({'diff_in_days': diff_in_days})
                else:
                    item.update({'diff_in_minutes': None, 'diff_in_hours': None, 'diff_in_days': None})

                f.write('"%(repository_owner)s";"%(repository_url)s";"%(repository_name)s";"%(issue)s";"%(status)s";"%(opened_by)s";"%(closed_by)s";"%(opened_at)s";"%(closed_at)s";"%(diff_in_minutes)s";"%(diff_in_hours)s";"%(diff_in_days)s"\r\n' % item)

    def get_data(self):
        doneRepositories = 0
        fetchedIssues = []
        repositoriesCount = self.db.wikiteams.repositories.find().count()

        repositories = list(self.db.wikiteams.repositories.find())

        repI = 0
        for repository in repositories:
            ev = []
            try:
                events = list(self.db.wikiteams.events.find({ "repository.url": repository['repository_url']}).distinct("payload.issue"))

                for event in events:
                    if event in fetchedIssues:
                        continue
                    else:
                        fetchedIssues.append(event)

                    corelatedEvents = list(self.db.wikiteams.events.find({ "payload.issue": event }, { "repository_owner": 1, "repository_url": 1, "repository_name": 1, "payload": 1, "created_at": 1, "web_start_date": 1, "web_closed_date": 1, "web_opened_author": 1, "web_closed_author": 1 }).sort("created_at"))

                    tmp = {'repository_owner': repository['repository_owner'], 'repository_url': repository['repository_url'], 'repository_name': repository['repository_name'], 'issue': corelatedEvents[0]['payload']['issue'], 'opened_at': None, 'closed_at': None}

                    if len(corelatedEvents) == 1:
                        if  corelatedEvents[0]['payload']['action'] == 'closed':
                            tmp.update({'opened_at': None, 'closed_at': corelatedEvents[0]['created_at'], 'status': corelatedEvents[0]['payload']['action']})
                            ev.append(tmp)
                        elif corelatedEvents[0]['payload']['action'] == 'opened':
                            tmp.update({'opened_at': corelatedEvents[0]['created_at'], 'closed_at': None, 'status': corelatedEvents[0]['payload']['action']})
                            ev.append(tmp)
                        elif corelatedEvents[0]['payload']['action'] == 'reopened':
                            tmp.update({'opened_at': None, 'closed_at': None, 'status': corelatedEvents[0]['payload']['action']})
                            ev.append(tmp)
                    else:
                        if corelatedEvents[0]['payload']['action'] == 'opened' and corelatedEvents[-1]['payload']['action'] == 'closed':
                            tmp.update({'opened_at': corelatedEvents[0]['created_at'], 'closed_at': corelatedEvents[-1]['created_at'], 'status': corelatedEvents[-1]['payload']['action']})
                            ev.append(tmp)
                        elif corelatedEvents[0]['payload']['action'] == 'opened' and corelatedEvents[-1]['payload']['action'] == 'reopened':
                            tmp.update({'opened_at': corelatedEvents[0]['created_at'], 'closed_at': None, 'status': corelatedEvents[-1]['payload']['action']})
                            ev.append(tmp)
                        elif corelatedEvents[0]['payload']['action'] == 'reopened' and corelatedEvents[-1]['payload']['action'] == 'reopened':
                            tmp.update({'opened_at': None, 'closed_at': None, 'status': corelatedEvents[-1]['payload']['action']})
                            ev.append(tmp)
                        elif corelatedEvents[0]['payload']['action'] == 'reopened' and corelatedEvents[-1]['payload']['action'] == 'closed':
                            tmp.update({'opened_at': None, 'closed_at': corelatedEvents[-1]['created_at'], 'status': corelatedEvents[-1]['payload']['action']})
                            ev.append(tmp)
                        elif corelatedEvents[0]['payload']['action'] == 'closed' and corelatedEvents[-1]['payload']['action'] == 'reopened':
                            tmp.update({'opened_at': None, 'closed_at': None, 'status': corelatedEvents[-1]['payload']['action']})
                            ev.append(tmp)
                        elif corelatedEvents[0]['payload']['action'] == 'closed' and corelatedEvents[-1]['payload']['action'] == 'closed':
                            tmp.update({'opened_at': None, 'closed_at': corelatedEvents[-1]['created_at'], 'status': corelatedEvents[-1]['payload']['action']})
                            ev.append(tmp)

                    if tmp['opened_at'] is None and 'web_start_date' in corelatedEvents[0]:
                        tmp['opened_at'] = corelatedEvents[0]['web_start_date']

                    if tmp['closed_at'] is None and 'web_closed_date' in corelatedEvents[0]:
                        tmp['closed_at'] = corelatedEvents[0]['web_closed_date']
                        tmp['status'] = 'closed'

                    if 'web_opened_author' in corelatedEvents[0]:
                        tmp['opened_by'] = corelatedEvents[0]['web_opened_author']
                    else:
                        tmp['opened_by'] = None

                    if 'web_closed_author' in corelatedEvents[0]:
                        tmp['closed_by'] = corelatedEvents[0]['web_closed_author']
                    else:
                        tmp['closed_by'] = None

                print "save to csv"
                self.print_to_csv(ev, repI)
                repI+=1
            except Exception, e:
                print "Error in repository: %s" % repository['repository_url']
                continue

            doneRepositories+=1

            print "Done repositories: %s" % doneRepositories

if __name__ == "__main__":
    importer = EventsGetter()
    importer.open_mongo_db()
    importer.get_data()
