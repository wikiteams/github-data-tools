__author__ = 'oski'

import glob
import gzip
import json
import dateutil.parser
from pymongo import MongoClient


_decoder = json.JSONDecoder()
_encoder = json.JSONEncoder()
_space = '\n'


def loads(s):
    """A generator reading a sequence of JSON values from a string."""
    while s:
        s = s.strip()
        obj, pos = _decoder.raw_decode(s)
        if not pos:
            raise ValueError('no JSON object found at %i' % pos)
        yield obj
        s = s[pos:]


def iterload(fp):
    """Like `loads` but reads from a file-like object."""
    return loads(fp.read())


class WatchEventsImporter():
    db = None
    csv_files = None
    csv_data = None
    first_row = None

    def get_csv_files(self):
        self.files = glob.glob('/mnt/data1/wikiteams/GitHubArchive/*.gz')
        return self.files

    def open_mongo_db(self):
        self.db = MongoClient(host='localhost', port=27017)

    def import_data(self):
        for file in self.files:
            self.first_row = None

            with gzip.open(file, 'rb') as json_file:
                print file
                for i in iterload(json_file):
                    if i['type'] == 'WatchEvent':
                        i['created_at'] = dateutil.parser.parse(i['created_at'])
                        self.db.wikiteams.stargazing.insert(i)
                        print '.',

            print ''


if __name__ == "__main__":
    importer = WatchEventsImporter()
    importer.get_csv_files()
    importer.open_mongo_db()

    importer.import_data()
