import codecs
import cStringIO
import csv
import threading
import time
from pymongo import MongoClient
import dateutil.parser
from dateutil.relativedelta import relativedelta
from intelliUser import GitUser

__author__ = 'doctor ko'
db = None

users = dict()


class MyDialect(csv.Dialect):
    strict = True
    skipinitialspace = True
    quoting = csv.QUOTE_ALL
    delimiter = ';'
    escapechar = '\\'
    quotechar = '"'
    lineterminator = '\n'


class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=MyDialect, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


#1. Ilosc osob, ktore dany deweloper followuje; [FollowEvent]
#2. Ilosc osob, ktore followuja dewelopera; [FollowEvent]
# informacje o uzytkownikach bede przechwoywal w dictionary
# o nazwie users, bedzie to informacja uzupelniajaca dla
# glownego zbiornika danych - dictionary o repozytoriach
class FollowersGetter(threading.Thread):
    global db
    finished = None
    date_from = None
    date_to = None

    def __init__(self, threadId, date_from, date_to):
        self.threadId = threadId
        threading.Thread.__init__(self)
        self.daemon = True
        self.date_from = date_from
        self.date_to = date_to

    def run(self):
        print 'FollowersGetter starts work...'
        self.finished = False
        self.get_data()

    def is_finished(self):
        return self.finished

    def set_finished(self, finished):
        self.finished = finished

    def get_data(self):
        i = 0

        following = self.db.wikiteams.events.find({"created_at": {"$gte":
                                                  self.date_from,
                                                  "lt": self.date_to}},
                                                  {"type": "FollowEvent"}
                                                  ).sort({"created_at": 1})
        try:
            while(following.alive):
                follow = following.next()
                print 'Working on follow event no: ' + str(follow['_id'])
                print 'date of activity: ' + str(follow['created_at'])
                datep = follow['created_at']
                actor_login = follow['actor']['login']
                i += 1
                target_login = follow['target']['login']
                target_followers = follow['target']['followers']
                if actor_login in users:
                    # update his info
                    print 'actor ' + actor_login + ' found'
                    gu = users[actor_login]
                else:
                    # create user info
                    gu = GitUser(actor_login)
                    print 'adding actor ' + actor_login + ' login'
                    users[actor_login] = gu
                if target_login in users:
                    # update his info
                    print 'actor ' + target_login + ' found'
                    gu = users[actor_login]
                else:
                    # create user info
                    gu = GitUser(target_login)
                    print 'adding actor ' + target_login + ' login'
                    users[target_login] = gu
                print 'Follows processed: ' + str(i)
        except StopIteration:
            print 'Cursor depleted'

        self.finished = True


# 3. Ilosc deweloperow, ktorzy sa w projektach
# przez niego utworzonych [PushEvent]
# 8. Czas spedzony w repo; [PushEvent]
# 9. Ilosc committow(rozbic na skille) [PushEvent]
class PushesGetter(threading.Thread):
    global db
    finished = None
    date_from = None
    date_to = None

    def __init__(self, threadId, date_from, date_to):
        self.threadId = threadId
        threading.Thread.__init__(self)
        self.daemon = True
        self.date_from = date_from
        self.date_to = date_to

    def run(self):
        print 'PushesGetter starts work...'
        self.finished = False
        self.get_data()

    def is_finished(self):
        return self.finished

    def set_finished(self, finished):
        self.finished = finished

    def get_data(self):
        i = 0

        pushing = self.db.wikiteams.events.find({"created_at": {"$gte":
                                                self.date_from,
                                                 "lt": self.date_to}},
                                                {"type": "PushEvent"}
                                                ).sort({"created_at": 1})
        try:
            while(pushing.alive):
                push = pushing.next()
                print 'Working on push event no: ' + str(push['_id'])
                print 'date of activity: ' + str(push['created_at'])
                datep = push['created_at']
                actor_login = push['actor']['login']
                i += 1
                print 'Pushes processed: ' + str(i)
        except StopIteration:
            print 'Cursor depleted'

        self.finished = True


class IssuesGetter(threading.Thread):
    global db
    finished = None
    date_from = None
    date_to = None

    def __init__(self, threadId, date_from, date_to):
        self.threadId = threadId
        threading.Thread.__init__(self)
        self.daemon = True
        self.date_from = date_from
        self.date_to = date_to

    def run(self):
        print 'IssuesGetter starts work...'
        self.finished = False
        self.get_data()

    def is_finished(self):
        return self.finished

    def set_finished(self, finished):
        self.finished = finished

    def get_data(self):
        i = 0

        issues = self.db.wikiteams.events.find({"created_at": {"$gte":
                                                self.date_from,
                                                 "lt": self.date_to}},
                                                {"type": "IssuesEvent"}
                                                ).sort({"created_at": 1})
        try:
            while(issues.alive):
                issue = issues.next()
                print 'Working on issue event no: ' + str(issue['_id'])
                print 'date of activity: ' + str(issue['created_at'])
                datep = issue['created_at']
                actor_login = issue['actor']['login']
                i += 1
                print 'Issues processed: ' + str(i)
        except StopIteration:
            print 'Cursor depleted'

        self.finished = True


def all_finished(threads):
    are_finished = True
    for thread in threads:
        if not thread.is_finished():
            return False
    return are_finished


def all_advance(threads, date_begin, date_end):
    for thread in threads:
        thread.set_finished(false)
        thread.set_dates(date_begin, date_end)


if __name__ == "__main__":
    global db
    tt = 0;
    db = MongoClient(host='localhost', port=27017)
    threads = []

    d1 = '2011-02-12T00:00:00Z'
    date_begin = dateutil.parser.parse(d1)
    date_end = date_begin + relativedelta(months=+1)

    fg = FollowersGetter(1, date_begin, date_end)
    threads.append(fg.start())
    pg = PushesGetter(2, date_begin, date_end)
    threads.append(pg.start())
    ig = IssuesGetter(3, date_begin, date_end).start()
    threads.append(ig.start())
    prg = PullRequestsGetter(4, date_begin, date_end)
    threads.append(prg.start())
    #threads.append(GollumGetter(5).start())
    #threads.append(TeamAddGetter(6).start())
    #threads.append(MemberGetter(7).start())

    while True:
        time.sleep(100)
        tt=+100
        # check if all thread finish 1-month job
        if all_finished(threads):
            print 'month finished'
            # if yes, dump data to csv
            dump_data()
            date_begin = date_end
            date_end = date_begin + relativedelta(months=+1)
            # and start new month
            all_advance(date_begin, date_end)
        else:
            print date_begin + ' still processing already for ' + tt + ' ms'
