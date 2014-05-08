import codecs
import cStringIO
import csv
from pymongo import MongoClient

__author__ = 'doctor ko'


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


class StarsGetter():
    db = None
    csv_files = None
    csv_data = None
    first_row = None

    def open_mongo_db(self):
        self.db = MongoClient(host='localhost', port=27017)

    def make_csv_headers(self):
        with open('stargazers_count_all.csv', 'ab') as f:
            f.write("repo_key;repo_owner;repo_name;stargazers_count;entry_date\r\n")

    def print_to_csv(self, csv_writer, items):
        csv_writer.writerow(items)

    def get_data(self):
        #pushesCount = self.db.wikiteams.pushes.find().count()
        pushes = self.db.wikiteams.pushes.find()

        #eventsCount = self.db.wikiteams.events.find().count()
        events = self.db.wikiteams.events.find()

        #stargazingCount = self.db.wikiteams.stargazing.find().count()
        stargazing = self.db.wikiteams.stargazing.find()
        i = 0
        j = 0
        w = 0

        with open('stargazers_count_all.csv', 'ab') as f:
            starsWriter = UnicodeWriter(f)
            try:
                while(pushes.alive):
                    push = pushes.next()
                    print 'Working on activity no: ' + str(push['_id'])
                    print 'date of activity: ' + str(push['created_at'])
                    #datep = dateutil.parser.parse(str(push['created_at']).split('"')[1])
                    datep = push['created_at']
                    self.print_to_csv(starsWriter, [str(push['repository']['url']), str(push['repository']['owner']),
                                      str(push['repository']['name']), str(push['repository']['stargazers']), str(datep)])
                    i += 1
                    #print 'Pushes left: %s' % (pushesCount - i)
                    print 'Pushes processed: ' + str(i)
            except StopIteration:
                print 'Cursor depleted, moving on to general events'

            try:
                while(events.alive):
                    event = events.next()
                    print 'Working on event no: ' + str(event['_id'])
                    print 'date of activity: ' + str(event['created_at'])
                    try:
                        datep = event['created_at']
                        self.print_to_csv(starsWriter, [str(event['repository']['url']), str(event['repository']['owner']),
                                          str(event['repository']['name']), str(event['repository']['stargazers']), str(datep)])
                    except KeyError:
                        print 'Just no info about number of stars found, calmly move to next object'
                    j += 1
                    #print 'Events left: %s' % (eventsCount - j)
                    print 'General events processed: ' + str(j)
            except StopIteration:
                print 'Cursor depleted, moving on to watch events'

            try:
                while(stargazing.alive):
                    watch = stargazing.next()
                    print 'Working on watch event no: ' + str(watch['_id'])
                    print 'date of watch activity: ' + str(watch['created_at'])
                    try:
                        datep = watch['created_at']
                        self.print_to_csv(starsWriter, [str(watch['repository']['url']), str(watch['repository']['owner']),
                                          str(watch['repository']['name']), str(watch['repository']['stargazers']), str(datep)])
                    except KeyError:
                        print 'Something went wrong. Missing repository info. Moving on.'
                    w += 1
                    #print 'Watch Events left: %s' % (stargazingCount - w)
                    print 'Watch Events processed: ' + str(w)
            except StopIteration:
                print 'Last cursor depleted'

        #self.print_to_csv(sorted(authors.iteritems(), key=operator.itemgetter(1), reverse=True))

if __name__ == "__main__":
    importer = StarsGetter()
    importer.open_mongo_db()
    importer.make_csv_headers()
    importer.get_data()
