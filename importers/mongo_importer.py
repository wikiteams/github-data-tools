__author__ = 'snipe'

import csv, glob, sys
from pymongo import MongoClient

class MongoImporter():
    db = None
    csv_files = None
    csv_data = None
    first_row = None

    def get_csv_files(self):
        self.files = glob.glob('data/*.csv')
        return self.files

    def open_mongo_db(self):
        self.db = MongoClient(host='localhost', port=27017)

    def import_data(self):
        for file in self.files:
            self.first_row = None

            print file
            with open(file, 'rb') as csv_file:
                reader = csv.reader(csv_file, delimiter=';')


                for row in reader:
                    if self.first_row is None:
                        self.first_row = row
                        continue

                    jsonObject = {'repository_owner': row[0], 'repository_url': row[1], 'repository_name': row[2]}
                    self.db.wikiteams.repositories.insert(jsonObject)
                    print '.'


if __name__ == "__main__":
    importer = MongoImporter()
    importer.get_csv_files()
    importer.open_mongo_db()
    importer.import_data()
