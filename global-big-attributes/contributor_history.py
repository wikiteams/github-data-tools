#####################################################################
#
# This is a script for basing getter of repo contributors and
# collaborators. There are no sources of dynamic data for history
# of contributors and collaborators in time, even GHTorrent won't
# keep history of this, thats why we utilize our MongoDB collection
# of events to restore information of collaborators of time
#
# A team member is someone who added at least one bit of information
# to a repository, which is: code, integration, issue, wiki, etc.
#
#####################################################################

# update: 26.05.2014
# version 1.02 codename: Blk

import codecs
import cStringIO
import csv
import threading
import time
import scream
from pymongo import MongoClient
import dateutil.parser
from dateutil.relativedelta import relativedelta
import pytz
from intelliRepository import GitRepository
import gexf
import getopt
import sys
import __builtin__

__author__ = 'doctor ko'

users = dict()

# Here we hold uniqe repositories identified by keys
# It is a most important collection in this script
repos = dict()

contributions = dict()
farthest_date_processed = None

gexf_second_file = None


def usage():
    f = open('usage-ch.txt', 'r')
    for line in f:
        print line


def useZulu():
    return not __builtin__.tango


def report_contribution(repo_key, actor_login, datep):
    global contributions
    if repo_key in contributions:
        scream.say('reporting contribution to existing repository')
        contributors = contributions[repo_key]
        if actor_login in contributors:
            scream.say('found that existing contributor added smth new')
            # do nothing
            #existing_push_count = contributors[actor_login]['pushes']
            #contributors[actor_login]['pushes'] += 1
            #existing_commit_count = contributors[actor_login]['commits']
            #contributors[actor_login]['commits'] += commits_count
        else:
            scream.say('new contributor detected')
            #existing_push_count = 0
            #existing_commit_count = 0
            contributors[actor_login] = datep
    else:
        scream.say('reporting contribution to unknown repo')
        contributions[repo_key] = dict()
        contributors = dict()
        contributors[actor_login] = datep
        contributions[repo_key] = contributors


def dump_aggregated_csv():
    for repo in repos.keys:
        scream.say('Procesing repo, key: ' + repo)
        scream.say('Initiating days contr count')
        dev_1_day = 0
        dev_2_day = 0
        dev_3_day = 0
        dev_1_week = 0
        dev_2_week = 0
        dev_1_month = 0
        dev_3_month = 0
        dev_6_month = 0
        dev_8_month = 0
        dev_12_month = 0
        date_created = repos[repo].getRepositoryCreatedAt()
        for contributor in contributions[repo].keys:
            scream.say('processing contributor name: ' + str(contributor))
            date_added = contributor[repo][contributor]
            scream.say('date when become member: ' + str(date_added))
            assert date_created <= date_added
            if (date_added - date_created).days <= 1:
                dev_1_day += 1
            elif (date_added - date_created).days <= 2:
                dev_2_day += 1
            elif (date_added - date_created).days <= 3:
                dev_3_day += 1
            elif (date_added - date_created).days <= 7:
                dev_1_week += 1
            elif (date_added - date_created).days <= 14:
                dev_2_week += 1
            elif (date_added - date_created).months <= 1:
                dev_1_month += 1
            elif (date_added - date_created).months <= 3:
                dev_3_month += 1
            elif (date_added - date_created).months <= 6:
                dev_6_month += 1
            elif (date_added - date_created).months <= 8:
                dev_8_month += 1
            elif (date_added - date_created).months <= 12:
                dev_12_month += 1


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


class CreateGetter(threading.Thread):

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
        scream.cout('CreateGetter starts work...')
        self.finished = False
        self.get_data()

    def is_finished(self):
        return self.finished

    def set_finished(self, finished):
        self.finished = finished

    def get_data(self):
        global db
        global repos
        global contributions

        i = 0

        creating = db.wikiteams.events.find({"created_at": {"$gte":
                                             self.date_from,
                                             "$lt": self.date_to},
                                             "type": "CreateEvent"}
                                            ).sort([("created_at", 1)])
        try:
            while(creating.alive):
                created = creating.next()
                scream.ssay('Working on create event no: ' + str(created['_id']))
                scream.say(created)
                scream.ssay('date of activity: ' + str(created['created_at']))
                datep = created['created_at']
                utc = pytz.UTC if useZulu() else pytz.timezone('Etc/GMT-7')
                datep = utc.localize(datep)
                scream.log(type(datep).__name__)
                # basicly a date of repo creation !
                if 'payload' in created:
                    if 'object' in created['payload']:
                        # ignore non-repo creation
                        if created['payload']['object'] != 'repository':
                            continue
                    elif 'ref_type' in created['payload']:
                        # Sir, ignore non-repo creation
                        if created['payload']['ref_type'] != 'repository':
                            continue
                else:
                    # not the kind of fields set we wanted ?
                    # let me sing you the song of my people
                    raise Exception('nope nope nope Report ! Report Nopevill !!')
                if 'repository' in created:
                    # goodie
                    repo_url = created['repository']['url']
                    repo_name = repo_url.split('/')[-1]
                    repo_owner = repo_url.split('/')[-2]
                elif 'repo' in created:
                    # also goodie
                    repo_url = created['repo']['url']
                    repo_name = repo_url.split('/')[-1]
                    repo_owner = repo_url.split('/')[-2]
                else:
                    # definitly not positive
                    # let me show you the effects of 3rd party source constant change
                    raise Exception('nope nope nope Report ! Report Nopevill !!')
                # doesnt matter who was creating - i already got owner
                repo_key = repo_owner + '/' + repo_name
                if repo_key in repos:
                    # update repository information
                    scream.say('repo ' + repo_key + ' found')
                    gr = repos[repo_key]
                    gr.setRepositoryCreatedAt(datep)
                    #gr.addPushCount(1)
                    #gr.addCommitCount(payload_size)
                    #report_contribution(repo_name, actor_login)
                else:
                    # create repository in dictionary
                    gr = GitRepository(repo_url, name=repo_name, owner=repo_owner)
                    gr.setRepositoryCreatedAt(datep)
                    #gr.addPushCount(1)
                    #gr.addCommitCount(payload_size)
                    scream.say('adding repo ' + repo_key + ' object to collection')
                    repos[repo_key] = gr
                    #report_contribution(repo_name, actor_login)
                i += 1
                scream.ssay('Creates processed: ' + str(i))
        except StopIteration:
            scream.err('Cursor `Create Event` depleted')
        except KeyError, k:
            scream.err(str(k))
            scream.err(created)
            sys.exit(-1)

        self.finished = True


class PushesGetter(threading.Thread):

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
        global db
        global repos
        global contributions
        global farthest_date_processed
        i = 0

        pushing = db.wikiteams.events.find({"created_at": {"$gte":
                                           self.date_from,
                                           "$lt": self.date_to},
                                           "type": "PushEvent"}
                                           ).sort([("created_at", 1)])
        try:
            while(pushing.alive):
                push = pushing.next()
                scream.ssay('Working on push event no: ' + str(push['_id']))
                scream.say(push)
                scream.ssay('date of activity: ' + str(push['created_at']))
                datep = push['created_at']
                utc = pytz.UTC if useZulu() else pytz.timezone('Etc/GMT-7')
                datep = utc.localize(datep)
                #datep = datetime.strptime(str(push['created_at']), '%Y-%m-%dT%H:%M:%SZ')
                if datep > farthest_date_processed:
                    farthest_date_processed = datep
                if 'repo' in push:
                    repo_url = push['repo']['url']
                    # parsing url is imho much safer
                    # prove otherwise than i'll fix it
                    repo_name = repo_url.split('/')[-1]
                    repo_owner = repo_url.split('/')[-2]
                else:
                    repo_url = push['repository']['url']
                    repo_name = repo_url.split('/')[-1]
                    repo_owner = repo_url.split('/')[-2]
                    #repo_name = push['repository']['name']
                    # who is the repo owner?
                    #repo_owner = push['repository']['owner']
                # who was pushing ?
                if 'login' in push['actor']:
                    actor_login = push['actor']['login']
                elif 'actor' in push['payload']:
                    actor_login = push['payload']['actor']
                else:
                    actor_login = push['actor_attributes']['login']
                assert actor_login is not None
                repo_key = repo_owner + '/' + repo_name
                if repo_key in repos:
                    # update repository information
                    scream.say('repo ' + repo_key + ' found')
                    gr = repos[repo_key]
                    #gr.addPushCount(1)
                    #gr.addCommitCount(payload_size)
                    report_contribution(repo_key, actor_login, datep)
                else:
                    # create repository in dictionary
                    gr = GitRepository(repo_url, name=repo_name, owner=repo_owner)
                    #gr.addPushCount(1)
                    #gr.addCommitCount(payload_size)
                    scream.say('adding repo ' + repo_key + ' name')
                    repos[repo_key] = gr
                    report_contribution(repo_key, actor_login, datep)
                i += 1
                scream.say('Pushes processed: ' + str(i))
        except StopIteration:
            scream.err('Cursor `Pushes Event` depleted')
        except KeyError, k:
            scream.err(str(k))
            scream.err(push)
            sys.exit(-1)

        self.finished = True


class PullRequestsGetter(threading.Thread):

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
        scream.cout('PullRequestsGetter starts work...')
        self.finished = False
        self.get_data()

    def is_finished(self):
        return self.finished

    def set_finished(self, finished):
        self.finished = finished

    def get_data(self):
        global db
        global repos
        global contributions

        i = 0

        pullrequests = db.wikiteams.events.find({"created_at": {"$gte":
                                                self.date_from,
                                                "$lt": self.date_to},
                                                "type": "PullRequestEvent"}
                                                ).sort([("created_at", 1)])
        try:
            while(pullrequests.alive):
                pullrequest = pullrequests.next()
                scream.ssay('Working on `Pull Request Event` no: ' + str(pullrequest['_id']))
                scream.say(pullrequest)
                scream.ssay('date of activity: ' + str(pullrequest['created_at']))
                datep = pullrequest['created_at']
                utc = pytz.UTC if useZulu() else pytz.timezone('Etc/GMT-7')
                datep = utc.localize(datep)
                if 'repo' in pullrequest:
                    repo_url = pullrequest['repo']['url']
                    # parsing url is imho much safer
                    # prove otherwise than i'll fix it
                    repo_name = repo_url.split('/')[-1]
                    repo_owner = repo_url.split('/')[-2]
                elif 'head' in pullrequest:
                    repo_url = pullrequest['head']['repo']['url']
                    repo_name = repo_url.split('/')[-1]
                    repo_owner = repo_url.split('/')[-2]
                if 'login' in pullrequest['actor']:
                    actor_login = pullrequest['actor']['login']
                else:
                    actor_login = pullrequest['payload']['actor']
                assert actor_login is not None
                repo_key = repo_owner + '/' + repo_name
                if repo_key in repos:
                    # update repository information
                    scream.say('repo ' + repo_key + ' found')
                    gr = repos[repo_key]
                    #gr.addPushCount(1)
                    #gr.addCommitCount(payload_size)
                    report_contribution(repo_key, actor_login, datep)
                else:
                    # create repository in dictionary
                    gr = GitRepository(repo_url, name=repo_name, owner=repo_owner)
                    #gr.addPushCount(1)
                    #gr.addCommitCount(payload_size)
                    scream.say('adding repo ' + repo_key + ' name')
                    repos[repo_key] = gr
                    report_contribution(repo_key, actor_login, datep)
                i += 1
                scream.say('Pull requests processed: ' + str(i))
        except StopIteration:
            scream.err('Cursor `Pull Request Event` depleted')
        except KeyError, k:
            scream.err(str(k))
            scream.err(pullrequest)
            sys.exit(-1)

        self.finished = True


class TeamAddGetter(threading.Thread):

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
        scream.say('TeamAddGetter starts work...')
        self.finished = False
        self.get_data()

    def is_finished(self):
        return self.finished

    def set_finished(self, finished):
        self.finished = finished

    def get_data(self):
        global db

        i = 0

        teamadds = db.wikiteams.events.find({"created_at": {"$gte":
                                            self.date_from,
                                            "$lt": self.date_to},
                                            "type": "TeamAddEvent"}
                                            ).sort([("created_at", 1)])
        try:
            while(teamadds.alive):
                teamadd = teamadds.next()
                print 'Working on team add event no: ' + str(teamadd['_id'])
                print 'date of activity: ' + str(teamadd['created_at'])
                datep = teamadd['created_at']
                actor_login = teamadd['actor']['login']
                i += 1
                print 'Team adds processed: ' + str(i)
        except StopIteration:
            print 'Cursor `TeamAddGetter` depleted'

        self.finished = True


class MemberGetter(threading.Thread):

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
        scream.say('MemberGetter starts work...')
        self.finished = False
        self.get_data()

    def is_finished(self):
        return self.finished

    def set_finished(self, finished):
        self.finished = finished

    def get_data(self):
        global db

        i = 0

        memberadds = db.wikiteams.events.find({"created_at": {"$gte":
                                              self.date_from,
                                              "$lt": self.date_to},
                                              "type": "MemberEvent"}
                                              ).sort([("created_at", 1)])
        try:
            while(memberadds.alive):
                memberadd = memberadds.next()
                print 'Working on member event no: ' + str(memberadd['_id'])
                print 'date of activity: ' + str(memberadd['created_at'])
                datep = memberadd['created_at']
                actor_login = memberadd['actor']['login']
                i += 1
                print 'Member adds processed: ' + str(i)
        except StopIteration:
            print 'Cursor `MemberGetter` depleted'

        self.finished = True


def all_finished(threads):
    are_finished = True
    for thread in threads:
        if not thread.is_finished():
            return False
    return are_finished


def all_advance(threads, date_begin, date_end):
    for thread in threads:
        thread.set_finished(False)
        thread.set_dates(date_begin, date_end)


def dump_contributions_network():
    global gexf_second_file
    scream.cout('preparing to write contributions network...')
    output_file = open('contributions-' + str(date_begin) + '.gexf', 'w')
    gexf_second_file.write(output_file)
    scream.cout('cn file for ' + str(date_begin) + ' created')


if __name__ == "__main__":
    __builtin__.verbose = False
    __builtin__.tango = False

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hu:v", ["help", "utf8=", "verbose"])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err)  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    for o, a in opts:
        if o in ("-v", "--verbose"):
            __builtin__.verbose = True
            scream.ssay('Enabling verbose mode.')
        elif o in ("-t", "--tango"):
            __builtin__.tango = True
            scream.ssay('Enabling switch to Tango (GMT-7) time.')
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-u", "--utf8"):
            use_utf8 = (a not in ['false', 'False'])

    gexf_second_file = gexf.Gexf("PJWSTK laboratories", "contributions_network" + '.gexf')
    contibutions_graph = gexf_second_file.addGraph("undirected", "dynamic", "github")
    tt = 0
    db = MongoClient(host='localhost', port=27017)
    threads = []

    d1 = '2011-02-12T00:00:00Z'
    date_begin = dateutil.parser.parse(d1)
    farthest_date_processed = date_begin
    date_end = date_begin + relativedelta(months=+1)
    scream.cout('starting from date: ' + str(date_begin))
    scream.cout('ending on date: ' + str(date_end))

    cg = CreateGetter(1, date_begin, date_end)
    threads.append(cg)
    threads[-1].start()
    pg = PushesGetter(2, date_begin, date_end)
    threads.append(pg)
    threads[-1].start()
    prg = PullRequestsGetter(3, date_begin, date_end)
    threads.append(prg)
    threads[-1].start()
    #tadg = TeamAddGetter(6, date_begin, date_end)
    #threads.append(tadg.start())
    #mbg = MemberGetter(7, date_begin, date_end)
    #threads.append(mbg.start())

    scream.cout('All threads started..')
    scream.cout('Start routine checks....')

    while True:
        #scream.std_fwrite('T')
        time.sleep(0.1)  # these are seconds :)
        #scream.std_fwrite('*')
        tt += 100
        #scream.std_fwrite('*')
        # check if all thread finish 1-month job
        if all_finished(threads):
            scream.std_fwrite('!')
            scream.cout('Month ' + str(date_begin.month) + 'finished')
            # if yes, dump data to csv
            scream.cout('Preparing to dump contributions network...')
            dump_contributions_network()
            scream.cout('Incrementing month...')
            date_begin = date_end
            date_end = date_begin + relativedelta(months=+1)
            # and start new month
            scream.cout('advancing the `from date`: ' + str(date_begin))
            scream.cout('advancing the `ending on date`: ' + str(date_end))
            all_advance(date_begin, date_end)
        elif tt % 1000 == 0:
            scream.cout('Still working on ' + str(date_begin) + 'already for ' + str(tt) + ' ms')
            scream.cout('Repos in database: ' + str(len(repos)))
            scream.cout('Farthest push date processed: ' + str(farthest_date_processed))
        else:
            scream.std_fwrite('+')
            scream.say(str(date_begin) + ' still processing already for ' + str(tt) + ' ms')

    dump_aggregated_csv()
