import codecs
import cStringIO
import csv
import threading
import time
import scream
from pymongo import MongoClient
import dateutil.parser
from dateutil.relativedelta import relativedelta
from intelliUser import GitUser
from intelliRepository import GitRepository

__author__ = 'doctor ko'

users = dict()
repos = dict()
contributions = dict()


def report_contribution(repo_name, actor_login, commits_count):
    existing_push_count = None
    existing_commit_count = None
    if repo_name in contributions:
        scream.say('reporting contribution to existing repository')
        contributors = contributions[repo_name]
        if actor_login in contributors:
            scream.say('found that existing contributor added smth new')
            existing_push_count = contributors[actor_login]['pushes']
            contributors[actor_login]['pushes'] += 1
            existing_commit_count = contributors[actor_login]['commits']
            contributors[actor_login]['commits'] += commits_count
        else:
            scream.say('new contributor detected')
            existing_push_count = 0
            existing_commit_count = 0
            contributors[actor_login] = {'pushes': 1, 'commits': commits_count}
    else:
        scream.say('reporting contribution to unknown repo')
        contributors = dict()
        existing_push_count = 0
        existing_commit_count = 0
        contributors[actor_login] = {'pushes': 1, 'commits': commits_count}
        contributions[repo_name] = contributors
    assert existing_push_count == contributions[repo_name][actor_login]['pushes'] - 1
    assert existing_commit_count == contributions[repo_name][actor_login]['commits'] - commits_count


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
        scream.cout('FollowersGetter starts work...')
        self.finished = False
        self.get_data()

    def is_finished(self):
        return self.finished

    def set_finished(self, finished):
        self.finished = finished

    def get_data(self):
        i = 0

        following = db.wikiteams.events.find({"created_at": {"$gte":
                                              self.date_from,
                                              "$lt": self.date_to},
                                             "type": "FollowEvent"}
                                             ).sort([("created_at", 1)])
        try:
            while(following.alive):
                follow = following.next()
                scream.cout('Working on follow event no: ' + str(follow['_id']))
                scream.cout('date of activity: ' + str(follow['created_at']))
                datep = follow['created_at']
                actor_login = follow['actor']['login']
                i += 1
                target_login = follow['target']['login']
                target_followers = follow['target']['followers']
                target_repos = follow['target']['repos']
                if actor_login in users:
                    # update his info
                    scream.cout('actor ' + actor_login + ' found')
                    gu = users[actor_login]
                    #gu_followed = getOrCreate(target_login)
                    gu.addFollowing(target_login)
                    gu.setFollowingDate(datep)
                    #gu_followed.addFollower(gu)
                else:
                    # create user info
                    gu = GitUser(actor_login)
                    scream.cout('adding actor ' + actor_login + ' login')
                    gu.addFollowing(target_login)
                    gu.setFollowingDate(datep)
                    users[actor_login] = gu
                if target_login in users:
                    # update his info
                    scream.cout('actor ' + target_login + ' found')
                    gu = users[target_login]
                    gu.addFollower(actor_login)
                    gu.setFollowerDate(datep)
                    gu.setFollowersCount(target_followers)
                    gu.setRepositoriesCount(target_repos)
                else:
                    # create user info
                    gu = GitUser(target_login)
                    scream.cout('adding actor ' + target_login + ' login')
                    gu.addFollower(actor_login)
                    gu.setFollowerDate(datep)
                    gu.setFollowersCount(target_followers)
                    gu.setRepositoriesCount(target_repos)
                    users[target_login] = gu
                print 'Follows processed: ' + str(i)
        except StopIteration:
            print 'Cursor depleted'

        self.finished = True


# 3. Ilosc deweloperow, ktorzy sa w projektach
# przez niego utworzonych [PushEvent]
# 6. Ilosc repo, ktorych nie tworzyl, w ktorych jest contributorem [PushEvent]
# 8. Czas spedzony w repo [PushEvent]
# 9. Ilosc committow (rozbic na skille) [PushEvent]
#   a. Ilosc commitow globalnie
#   b. Ilosc commitow w repo
#   c. Stosunek b/a
# 13. Czas od ostatniego commitu [PushEvent]
# 14. Czas od pierwszego commitu [PushEvent]
# 15. Odstep czasu pomiedzy commitami [PushEvent]
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
        scream.cout('PushesGetter starts work...')
        self.finished = False
        self.get_data()

    def is_finished(self):
        return self.finished

    def set_finished(self, finished):
        self.finished = finished

    def get_data(self):
        i = 0

        pushing = db.wikiteams.events.find({"created_at": {"$gte":
                                           self.date_from,
                                           "$lt": self.date_to},
                                           "type": "PushEvent"},
                                           ).sort([("created_at", 1)])
        try:
            while(pushing.alive):
                push = pushing.next()
                scream.cout('Working on push event no: ' + str(push['_id']))
                scream.say(push)
                scream.cout('date of activity: ' + str(push['created_at']))
                datep = push['created_at']
                repo_url = push['repo']['url']
                repo_name = push['repo']['name']
                actor_login = push['actor']['login']
                payload_size = push['payload']['size']
                if repo_name in repos:
                    # update his info
                    scream.say('repo ' + repo_name + ' found')
                    gr = repos[repo_name]
                    gr.addPushCount(1)
                    gr.addCommitCount(payload_size)
                    report_contribution(repo_name, actor_login, payload_size)
                else:
                    # create user info
                    gr = GitRepository(repo_url, repo_name)
                    gr.addPushCount(1)
                    gr.addCommitCount(payload_size)
                    scream.say('adding repo ' + repo_name + ' name')
                    repos[repo_name] = gr
                    report_contribution(repo_name, actor_login, payload_size)
                i += 1
                scream.say('Pushes processed: ' + str(i))
        except StopIteration:
            print 'Cursor depleted'

        self.finished = True


class PullRequestsGetter(threading.Thread):
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
        scream.say('PullRequestsGetter starts work...')
        self.finished = False
        self.get_data()

    def is_finished(self):
        return self.finished

    def set_finished(self, finished):
        self.finished = finished

    def get_data(self):
        i = 0

        issues = db.wikiteams.events.find({"created_at": {"$gte":
                                          self.date_from,
                                          "$lt": self.date_to},
                                          "type": "PullRequestEvent"}
                                          ).sort([("created_at", 1)])
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
        scream.say('IssuesGetter starts work...')
        self.finished = False
        self.get_data()

    def is_finished(self):
        return self.finished

    def set_finished(self, finished):
        self.finished = finished

    def get_data(self):
        i = 0

        issues = db.wikiteams.events.find({"created_at": {"$gte":
                                           self.date_from,
                                           "$lt": self.date_to},
                                          "type": "IssuesEvent"}
                                          ).sort([("created_at", 1)])
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


class GollumGetter(threading.Thread):
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
        print 'GollumGetter starts work...'
        self.finished = False
        self.get_data()

    def is_finished(self):
        return self.finished

    def set_finished(self, finished):
        self.finished = finished

    def get_data(self):
        i = 0

        gollums = db.wikiteams.events.find({"created_at": {"$gte":
                                           self.date_from,
                                           "$lt": self.date_to},
                                           "type": "GollumEvent"}
                                           ).sort([("created_at", 1)])
        try:
            while(gollums.alive):
                gollum = gollums.next()
                print 'Working on gollum event no: ' + str(gollum['_id'])
                print 'date of activity: ' + str(gollum['created_at'])
                datep = gollum['created_at']
                if gollum['actor'] is str:
                    actor_login = gollum['actor']
                else:
                    actor_login = gollum['actor']['login']
                if 'repo' in gollum['payload']:
                    repo_name = gollum['payload']['repo']
                else:
                    repo_is_fork = gollum['repository']['fork']
                    repo_watchers = gollum['repository']['watchers']
                    repo_description = gollum['repository']['description']
                    repo_language = gollum['repository']['language']
                    repo_has_downloads = gollum['repository']['has_downloads']
                    repo_url = gollum['repository']['url']
                    repo_stargazers = gollum['repository']['stargazers']
                    repo_created_at = gollum['repository']['created_at']
                    repo_master_branch = gollum['repository']['master_branch']
                    repo_is_private = gollum['repository']['private']
                    repo_pushed_at = gollum['repository']['pushed_at']
                    repo_open_issues = gollum['repository']['open_issues']
                    repo_has_wiki = gollum['repository']['has_wiki']
                    repo_organization = gollum['repository']['organization']
                    repo_owner = gollum['repository']['owner']
                    repo_has_issues = gollum['repository']['has_issues']
                    repo_forks = gollum['repository']['forks']
                    repo_size = gollum['repository']['size']
                    repo_homepage = gollum['repository']['homepage']
                    repo_id = gollum['repository']['id']
                    repo_name = gollum['repository']['name']

                i += 1
                print 'Gollums processed: ' + str(i)
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
    tt = 0
    db = MongoClient(host='localhost', port=27017)
    threads = []

    d1 = '2011-02-12T00:00:00Z'
    date_begin = dateutil.parser.parse(d1)
    date_end = date_begin + relativedelta(months=+1)
    print 'starting from date: ' + str(date_begin)
    print 'ending on date: ' + str(date_end)

    fg = FollowersGetter(1, date_begin, date_end)
    threads.append(fg.start())
    pg = PushesGetter(2, date_begin, date_end)
    threads.append(pg.start())
    ig = IssuesGetter(3, date_begin, date_end)
    threads.append(ig.start())
    prg = PullRequestsGetter(4, date_begin, date_end)
    threads.append(prg.start())
    gg = GollumGetter(5, date_begin, date_end)
    threads.append(gg.start())
    #threads.append(TeamAddGetter(6).start())
    #threads.append(MemberGetter(7).start())

    while True:
        time.sleep(100)
        tt += 100
        # check if all thread finish 1-month job
        if all_finished(threads):
            print 'month finished'
            # if yes, dump data to csv
            dump_data()
            date_begin = date_end
            date_end = date_begin + relativedelta(months=+1)
            # and start new month
            print 'advancing the `from date`: ' + date_begin
            print 'advancing the `ending on date`: ' + date_end
            all_advance(date_begin, date_end)
        else:
            print date_begin + ' still processing already for ' + tt + ' ms'
