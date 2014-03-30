__author__ = 'snipe'

import csv, glob, sys, urllib2, os
from pymongo import MongoClient
from github import Github
from git import Repo, exc
from subprocess import check_output, CalledProcessError
import json
import github
import shutil

class MongoImporter():
    db = None
    csv_files = None
    csv_data = None
    first_row = None
    path = '/media/snipe/Data/git-cloned'

    def open_mongo_db(self):
        self.db = MongoClient(host='localhost', port=27017)

    def import_data(self):
        pushes = list(self.db.wikiteams.pushes.find({ "repository.language": "JavaScript" }))

        for push in  pushes:
            repositoryUrl = push['repository']['url'].split('/compare')[0]
            print 'Cloning: %s@%s' % (repositoryUrl, push['payload']['head'])
            repoDestinationPath = self.path + '/' + push['repository']['owner'] + '/' + push['repository']['name']

            if not os.path.exists(repoDestinationPath):
                repo = Repo.clone_from(repositoryUrl, repoDestinationPath)
            else:
                repo = Repo(repoDestinationPath)

            try:
                repo.git.checkout(push['payload']['head'])

                # copy/paste detection
                #cpd = check_output(["jscpd", "--min-tokens", "50", "--languages", "javascript", "--path", repoDestinationPath])
                #print cpd
                #sys.exit(0)
                #clones = r'Found (\d+) exact clones with (\d+) duplicated lines in 35 files'
                #total = r'(\d+\.\d+) \((\d+) lines\) duplicated lines out of (\d+) total lines of code'

                # complexity-raport
                cr = unicode(check_output(["cr", "--maxfiles", "5120", "--format", "json", repoDestinationPath]), encoding='utf-8')
                jsonObject = json.loads(cr)

                result = {
                    'sha1': push['payload']['head'],
                    'repository_owner': push['repository']['owner'],
                    'repository_name': push['repository']['name'],
                    'repository_url': repositoryUrl,
                    'files': [],
                }

                for report in jsonObject['reports']:
                    filePath = report['path'].replace(repoDestinationPath+'/', '')
                    aggregate = report['aggregate']
                    file = {
                        'sloc': aggregate['sloc'],
                        'cyclomatic': aggregate['cyclomatic'],
                        'cyclomaticDensity': aggregate['cyclomaticDensity'],
                        'halstead': {
                            'difficulty': aggregate['halstead']['difficulty'],
                            'volume': aggregate['halstead']['volume'],
                            'effort': aggregate['halstead']['effort'],
                            'bugs': aggregate['halstead']['bugs'],
                            'time': aggregate['halstead']['time'],
                        },
                        'maintainability': report['maintainability'],
                        'path': filePath,
                    }

                    result['files'].append(file)

                print "Insert new data into codequality db"
                self.db.wikiteams.codequality.insert(result)
            except exc.GitCommandError, e:
                print "Error: %s" % e.message
            except CalledProcessError, e:
                print "Error: %s" % e.message
            #except ValueError:
            #    print "Error: %s" % e.message
            #shutil.rmtree(repoDestinationPath)


if __name__ == "__main__":
    importer = MongoImporter()
    importer.open_mongo_db()
    importer.import_data()
