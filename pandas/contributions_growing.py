import pandas as pd
import dateutil.parser
from termcolor import colored
import json
import getopt
import sys
from pandasql import *
import __builtin__

WAIT_FOR_USER = False
empty_string = ''

created_at_filename = 'created.csv'

pushes_csv_filename = 'pushes.csv'
pulls_csv_filename = 'pulls.csv'
follows_csv_filename = 'follows.csv'
issues_csv_filename = 'issues.csv'

team_adds_csv_filename = 'team-adds.csv'
# there is some serious issue with team-adds, probably from the side of githubarchive
# because payload.user is basicly nonexistent, only payload.team entries are found
# which means entries hold information about team creation and add to repo, not
# about adding a particular user to the team, shame on you GHA
member_adds_csv_filename = 'member-adds.csv'

pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)

ultimate_path = '../'


def usage():
    f = open('usage.txt', 'r')
    for line in f:
        print line


def remove_links(source_string):
    return source_string.replace('https://github.com/', '').replace('https://api.github.com/repos/', '').replace('https://api.github.dev/repos/', '').replace('https://api.github.com/', '')


if __name__ == "__main__":
    resume = None
    aggr = 'sql'

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hr:via:", ["help", "resume=", "verbose", "interactive", "aggregate="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err)  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    for o, a in opts:
        if o in ("-v", "--verbose"):
            __builtin__.verbose = True
            print 'Enabling verbose mode.'
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-i", "--interactive"):
            WAIT_FOR_USER = True
        elif o in ("-r", "--resume"):
            resume = str(a)
        elif o in ("-a", "--aggregate"):
            assert str(a) in ['sql', 'native', 'default']
            aggr = str(a).replace('default', 'sql')

    if resume == 'TeamAdds' or resume is None:
        resume = None
        print colored('Reading the ' + team_adds_csv_filename + ' file.. it may take a while...', 'red')
        team_adds_df = pd.read_csv(ultimate_path + team_adds_csv_filename, header=0,
                                   sep=',', na_values=['', None], error_bad_lines=False, quotechar='"')
        print colored('Reading team_adds_csv_filename done.', 'green')
        print colored('Index of team_adds_df is:', 'green')
        print team_adds_df.index[0:5]
        print team_adds_df.dtypes
        print team_adds_df.head(20)
        print team_adds_df.tail(20)
        print colored('Parsing IsoDate Zulu time to proper datetime object', 'green')
        team_adds_df['created_at'] = team_adds_df['created_at'].apply(lambda x: dateutil.parser.parse(x))
        #team_adds_df['created_at'] = team_adds_df['created_at'].astype(datetime)
        print colored('Fixing 4 columns types..', 'green')
        team_adds_df['actor.login'] = team_adds_df['actor.login'].astype(str)
        team_adds_df['repo.url'] = team_adds_df['repo.url'].astype(str)
        team_adds_df['repository.url'] = team_adds_df['repository.url'].astype(str)
        team_adds_df['payload.repository.url'] = team_adds_df['payload.repository.url'].astype(str)
        print team_adds_df.dtypes  # can verify
        print team_adds_df.head(20)
        print team_adds_df.tail(20)
        print colored('Starting normalizing username', 'green')
        #team_adds_df['username'] = pd.concat([team_adds_df['actor'], team_adds_df['actor.login'], team_adds_df['actor_attributes.login']], ignore_index=True)
        team_adds_df['username'] = team_adds_df.apply(lambda x: empty_string.join(list(set([x['actor'] if x['actor'] != 'nan' else '', x['actor.login'] if x['actor.login'] != 'nan' else '', x['actor_attributes.login'] if x['actor_attributes.login'] != 'nan' else '']))), 1)
        print colored('End normalizing username', 'green')
        assert '' not in team_adds_df['username']
        print 'Are there any nulls in username column?: ' + str(pd.isnull(team_adds_df['username']).any())
        print colored('End verifiying usernames', 'green')
        print colored('Starting normalizing repository', 'green')
        #team_adds_df['repository'] = pd.concat([team_adds_df['repository.url'].fillna(''), team_adds_df['repo.url'].fillna(''), team_adds_df['payload.repository.url'].fillna('')], ignore_index=True)
        team_adds_df['repository'] = team_adds_df.apply(lambda x: remove_links(empty_string.join(list(set([remove_links(x['repository.url']) if x['repository.url'] != 'nan' else '', remove_links(x['repo.url']) if x['repo.url'] != 'nan' else '', remove_links(x['payload.repository.url']) if x['payload.repository.url'] != 'nan' else ''])))), 1)
        print colored('End normalizing repository', 'green')
        assert '' not in team_adds_df['repository']
        print 'Are there any nulls in repository column?: ' + str(pd.isnull(team_adds_df['repository']).any())
        print colored('End verifiying repository', 'green')
        print colored('Droping useless before-concat columns', 'red')
        team_adds_df = team_adds_df.drop('actor', axis=1)
        team_adds_df = team_adds_df.drop('actor.login', axis=1)
        team_adds_df = team_adds_df.drop('actor_attributes.login', axis=1)
        team_adds_df = team_adds_df.drop('repository.url', axis=1)
        team_adds_df = team_adds_df.drop('repo.url', axis=1)
        team_adds_df = team_adds_df.drop('payload.repository.url', axis=1)
        print colored('Drop of 6 columns complete', 'red')
        print team_adds_df.dtypes  # can verify
        print team_adds_df.head(20)
        print team_adds_df.tail(20)
        print colored('Writing normalized CSV..', 'blue')
        team_adds_df.to_csv(ultimate_path + 'normalized_' + team_adds_csv_filename, mode='wb', sep=';', encoding='UTF-8')

        if WAIT_FOR_USER:
            raw_input("Press Enter to continue to member add events...")

    if resume == 'MemberAdds' or resume is None:
        resume = None
        print colored('Reading the ' + member_adds_csv_filename + ' file.. it may take a while... (0.8 GB)', 'red')
        member_adds_df = pd.read_csv(ultimate_path + member_adds_csv_filename, header=0,
                                     sep=',', na_values=['', None], error_bad_lines=False, quotechar='"')
        print colored('Reading member_adds_csv_filename done.', 'green')
        print colored('Index of member_adds_df is:', 'green')
        print member_adds_df.index[0:5]
        print member_adds_df.dtypes
        print member_adds_df.head(20)
        print member_adds_df.tail(20)
        print colored('Parsing IsoDate Zulu time to proper datetime object', 'green')
        member_adds_df['created_at'] = member_adds_df['created_at'].apply(lambda x: dateutil.parser.parse(x))
        print colored('Fixing 5 columns types..', 'green')
        member_adds_df['payload.member'] = member_adds_df['payload.member'].astype(str)
        member_adds_df['payload.member.login'] = member_adds_df['payload.member.login'].astype(str)
        member_adds_df['repository.url'] = member_adds_df['repository.url'].astype(str)
        member_adds_df['payload.repository.url'] = member_adds_df['payload.repository.url'].astype(str)
        member_adds_df['repo.url'] = member_adds_df['repo.url'].astype(str)
        print member_adds_df.dtypes  # can verify
        print member_adds_df.head(20)
        print member_adds_df.tail(20)
        print colored('Starting normalizing username', 'green')
        member_adds_df['payload.member'] = member_adds_df['payload.member'].apply(lambda x: json.loads(x)['login'] if ',' in str(x) else x)
        #member_adds_df['username'] = member_adds_df.apply(lambda x: x['payload.member.login'] if ((not pd.isnull(x['payload.member.login'])) and (x['payload.member.login'] != '')) else x['payload.member'], 1)
        member_adds_df['username'] = member_adds_df.apply(lambda x: empty_string.join(list(set([x['payload.member.login'] if x['payload.member.login'] != 'nan' else '', x['payload.member'] if x['payload.member'] != 'nan' else '']))), 1)
        print colored('End normalizing username', 'green')
        assert '' not in member_adds_df['username']
        print 'Are there any nulls in repository column?: ' + str(pd.isnull(member_adds_df['username']).any())
        print colored('End verifiying usernames', 'green')
        print colored('Starting normalizing repository', 'green')
        #member_adds_df['repository'] = pd.concat([member_adds_df['repository.url'].fillna(''), member_adds_df['payload.repository.url'].fillna(''), member_adds_df['repo.url'].fillna('')], ignore_index=True)
        member_adds_df['repository'] = member_adds_df.apply(lambda x: remove_links(empty_string.join(list(set([remove_links(x['repository.url']) if x['repository.url'] != 'nan' else '', remove_links(x['payload.repository.url']) if x['payload.repository.url'] != 'nan' else '', remove_links(x['repo.url']) if x['repo.url'] != 'nan' else ''])))), 1)
        print colored('End normalizing repository', 'green')
        assert '' not in member_adds_df['repository']
        print 'Are there any nulls in repository column?: ' + str(pd.isnull(member_adds_df['repository']).any())
        print colored('End verifiying repository', 'green')
        print colored('Droping useless before-concat columns', 'red')
        member_adds_df = member_adds_df.drop('payload.member.login', axis=1)
        member_adds_df = member_adds_df.drop('payload.member', axis=1)
        member_adds_df = member_adds_df.drop('repository.url', axis=1)
        member_adds_df = member_adds_df.drop('payload.repository.url', axis=1)
        member_adds_df = member_adds_df.drop('repo.url', axis=1)
        print colored('Drop of 5 columns complete', 'red')
        print member_adds_df.dtypes  # can verify
        print member_adds_df.head(20)
        print member_adds_df.tail(20)
        print colored('Writing normalized CSV..', 'blue')
        member_adds_df.to_csv(ultimate_path + 'normalized_' + member_adds_csv_filename, mode='wb', sep=';', encoding='UTF-8')

        if WAIT_FOR_USER:
            raw_input("Press Enter to continue to repo creation events...")

    if resume == 'Created' or resume is None:
        resume = None
        print colored('Reading the ' + created_at_filename + ' file.. it may take a while... (1.6 GB)', 'red')
        created_df = pd.read_csv(ultimate_path + created_at_filename, header=0,
                                 sep=',', na_values=['', None], error_bad_lines=False, quotechar='"')
        print colored('Reading created_at_filename done.', 'green')
        print colored('Index of created_df is:', 'green')
        print created_df.index[0:5]
        print created_df.dtypes
        print created_df.head(20)
        print created_df.tail(20)
        print colored('Parsing IsoDate Zulu time to proper datetime object', 'green')
        created_df['created_at'] = created_df['created_at'].apply(lambda x: dateutil.parser.parse(x))
        print colored('Fixing 4 columns types..', 'green')
        created_df['payload.object'] = created_df['payload.object'].astype(str)
        created_df['payload.ref_type'] = created_df['payload.ref_type'].astype(str)
        created_df['repository.url'] = created_df['repository.url'].astype(str)
        created_df['repo.url'] = created_df['repo.url'].astype(str)
        print created_df.dtypes  # can verify
        print created_df.head(20)
        print created_df.tail(20)
        print colored('Starting normalizing payload object', 'green')
        #created_df['object'] = pd.concat([created_df['payload.object'].fillna(''), created_df['payload.ref_type'].fillna('')], ignore_index=True)
        created_df['object'] = created_df.apply(lambda x: empty_string.join(list(set([x['payload.object'] if x['payload.object'] != 'nan' else '', x['payload.ref_type'] if x['payload.ref_type'] != 'nan' else '']))), 1)
        print colored('End normalizing payload object', 'green')
        assert '' not in created_df['object']
        print 'Are there any nulls in the `object` column?: ' + str(pd.isnull(created_df['object']).any())
        print colored('End verifiying payload object', 'green')
        print colored('Starting normalizing repository', 'green')
        #created_df['repository'] = pd.concat([created_df['repository.url'].fillna(''), created_df['repo.url'].fillna('')], ignore_index=True)
        created_df['repository'] = created_df.apply(lambda x: remove_links(empty_string.join(list(set([remove_links(x['repository.url']) if x['repository.url'] != 'nan' else '', remove_links(x['repo.url']) if x['repo.url'] != 'nan' else '']))) ), 1)
        print colored('End normalizing repository', 'green')
        assert '' not in created_df['repository']
        print 'Are there any nulls in the `repository` column?: ' + str(pd.isnull(created_df['repository']).any())
        print colored('End verifiying repository', 'green')
        print colored('Droping useless before-concat columns', 'red')
        created_df = created_df.drop('payload.object', axis=1)
        created_df = created_df.drop('payload.ref_type', axis=1)
        created_df = created_df.drop('repository.url', axis=1)
        created_df = created_df.drop('repo.url', axis=1)
        print colored('Drop of 4 columns complete', 'red')
        print created_df.dtypes  # can verify
        print created_df.head(20)
        print created_df.tail(20)
        print colored('Writing normalized CSV..', 'blue')
        created_df.to_csv(ultimate_path + 'normalized_' + created_at_filename, mode='wb', sep=';', encoding='UTF-8')

        print colored('-------------------- AGGREGATION --------------', 'red')
        if aggr == 'native':
            # merge is a function in the pandas namespace, and it is also available as a
            # DataFrame instance method, with the calling DataFrame being
            # implicitly considered the left object in the join.
            member_adds_df = member_adds_df.merge(created_df[created_df['object'] == 'repository'], on='repository', how='left', suffixes=('_memb', '_cr'))
        elif aggr == 'sql':
            #print colored('reading globals to sqldf', 'yellow')
            #pysqldf = lambda q: sqldf(q, globals())
            print colored('preparing SQL..', 'yellow')
            q = """
            SELECT
            mb.created_at, mb.username, mb.repository, cr.created_at
            FROM
            member_adds_df mb
            LEFT JOIN
            created_df cr
            ON mb.repository = cr.repository
            WHERE
            cr.object == 'repository';
            """
            print colored('executing SQL..', 'yellow')
            member_adds_df = sqldf(q, globals())
            print colored('done executing SQL...', 'green')
        else:
            assert False
        print colored('-------------------- AGGREGATION completed --------------', 'yellow')
        print member_adds_df.index[0:5]
        print member_adds_df.dtypes
        print member_adds_df.head(50)
        print member_adds_df.tail(50)

        print colored('Writing normalized CSV 2..', 'blue')
        member_adds_df.to_csv(ultimate_path + 'normalized_cr_' + member_adds_csv_filename, mode='wb', sep=';', encoding='UTF-8')

        if WAIT_FOR_USER:
            raw_input("Press Enter to continue to pulls events...")

    if resume == 'Pulls' or resume is None:
        resume = None
        print colored('Reading the ' + pulls_csv_filename + ' file.. it may take a while...', 'red')
        pulls_df = pd.read_csv(ultimate_path + pulls_csv_filename, header=0,
                               sep=',', na_values=['', None], error_bad_lines=False, quotechar='"')
        print colored('Reading pulls_csv_filename done.', 'green')
        print colored('Index of pulls_df is:', 'green')
        print pulls_df.index[0:5]
        print pulls_df.dtypes
        print pulls_df.head(20)
        print pulls_df.tail(20)
        print colored('Parsing IsoDate Zulu time to proper datetime object', 'green')
        pulls_df['created_at'] = pulls_df['created_at'].apply(lambda x: dateutil.parser.parse(x))
        print colored('Fixing 4 columns types..', 'green')
        pulls_df['actor.login'] = pulls_df['actor.login'].astype(str)
        pulls_df['payload.actor'] = pulls_df['payload.actor'].astype(str)
        pulls_df['head.repo.url'] = pulls_df['head.repo.url'].astype(str)
        pulls_df['repo.url'] = pulls_df['repo.url'].astype(str)
        print pulls_df.dtypes  # can verify
        print pulls_df.head(20)
        print pulls_df.tail(20)
        print colored('Starting normalizing actor username', 'green')
        #created_df['object'] = pd.concat([created_df['payload.object'].fillna(''), created_df['payload.ref_type'].fillna('')], ignore_index=True)
        pulls_df['username'] = pulls_df.apply(lambda x: empty_string.join(list(set([x['actor.login'] if x['actor.login'] != 'nan' else '', x['payload.actor'] if x['payload.actor'] != 'nan' else '']))), 1)
        print colored('End normalizing payload username', 'green')
        assert '' not in pulls_df['username']
        print 'Are there any nulls in the `username` column?: ' + str(pd.isnull(pulls_df['username']).any())
        print colored('End verifiying payload username', 'green')
        print colored('Starting normalizing repository', 'green')
        #created_df['repository'] = pd.concat([created_df['repository.url'].fillna(''), created_df['repo.url'].fillna('')], ignore_index=True)
        pulls_df['repository'] = pulls_df.apply(lambda x: remove_links(empty_string.join(list(set([remove_links(x['head.repo.url']) if x['head.repo.url'] != 'nan' else '', remove_links(x['repo.url']) if x['repo.url'] != 'nan' else ''])))), 1)
        print colored('End normalizing repository', 'green')
        assert '' not in pulls_df['repository']
        print 'Are there any nulls in the `repository` column?: ' + str(pd.isnull(pulls_df['repository']).any())
        print colored('End verifiying repository', 'green')
        print colored('Droping useless before-concat columns', 'red')
        pulls_df = pulls_df.drop('actor.login', axis=1)
        pulls_df = pulls_df.drop('payload.actor', axis=1)
        pulls_df = pulls_df.drop('head.repo.url', axis=1)
        pulls_df = pulls_df.drop('repo.url', axis=1)
        print colored('Drop of 4 columns complete', 'red')
        print pulls_df.dtypes  # can verify
        print pulls_df.head(20)
        print pulls_df.tail(20)
        print colored('Writing normalized CSV..', 'blue')
        pulls_df.to_csv(ultimate_path + 'normalized_' + pulls_csv_filename, mode='wb', sep=';', encoding='UTF-8')

        print colored('-------------------- AGGREGATION --------------', 'red')
        # assign creation dates to repos in MemberAdd table
        if aggr == 'native':
            pulls_df = pulls_df.merge(created_df[created_df['object'] == 'repository'], on='repository', how='left', suffixes=('_pull', '_cr'))
        elif aggr == 'sql':
            #print colored('reading globals to sqldf', 'yellow')
            #pysqldf = lambda q: sqldf(q, globals())
            print colored('preparing SQL..', 'yellow')
            q = """
            SELECT
            pll.created_at, pll.username, pll.repository, cr.created_at
            FROM
            pulls_df pll
            LEFT JOIN
            created_df cr
            ON pll.repository = cr.repository
            WHERE
            cr.object == 'repository';
            """
            print colored('executing SQL..', 'yellow')
            pulls_df = sqldf(q, globals())
            print colored('done executing SQL...', 'green')
        else:
            assert False
        print colored('-------------------- AGGREGATION completed --------------', 'yellow')
        print pulls_df.index[0:5]
        print pulls_df.dtypes
        print pulls_df.head(50)
        print pulls_df.tail(50)

        print colored('Writing normalized CSV 2..', 'blue')
        pulls_df.to_csv(ultimate_path + 'normalized_cr_' + pulls_csv_filename, mode='wb', sep=';', encoding='UTF-8')

        if WAIT_FOR_USER:
            raw_input("Press Enter to continue to follows events...")

    if resume == 'Follows' or resume is None:
        resume = None
        print colored('Reading the ' + follows_csv_filename + ' file.. it may take a while...', 'red')
        follows_df = pd.read_csv(ultimate_path + follows_csv_filename, header=0,
                                 sep=',', na_values=['', None], error_bad_lines=False, quotechar='"')
        print colored('Reading follows_csv_filename done.', 'green')
        print colored('Index of follows_df is:', 'green')
        print follows_df.index[0:5]
        print follows_df.dtypes
        print follows_df.head(20)
        print follows_df.tail(20)
        print colored('Parsing IsoDate Zulu time to proper datetime object', 'green')
        follows_df['created_at'] = follows_df['created_at'].apply(lambda x: dateutil.parser.parse(x))
        print colored('Fixing 5 columns types..', 'green')
        follows_df['actor.login'] = follows_df['actor.login'].astype(str)
        follows_df['actor'] = follows_df['actor'].astype(str)
        follows_df['actor_attributes.login'] = follows_df['actor_attributes.login'].astype(str)
        follows_df['payload.target.login'] = follows_df['payload.target.login'].astype(str)
        follows_df['target.login'] = follows_df['target.login'].astype(str)
        follows_df['payload.target.repos'] = follows_df['payload.target.repos'].astype(str)
        follows_df['target.repos'] = follows_df['target.repos'].astype(str)
        follows_df['payload.target.followers'] = follows_df['payload.target.followers'].astype(str)
        follows_df['target.followers'] = follows_df['target.followers'].astype(str)
        print follows_df.dtypes  # can verify
        print follows_df.head(20)
        print follows_df.tail(20)
        print colored('Starting normalizing actor username and target username', 'green')
        follows_df['actor'] = follows_df['actor'].apply(lambda x: x.split(':')[3].split(',')[0].strip() if ',' in str(x) else x)
        follows_df['username'] = follows_df.apply(lambda x: empty_string.join(list(set([x['actor'] if x['actor'] != 'nan' else '', x['actor.login'] if x['actor.login'] != 'nan' else '', x['actor_attributes.login'] if x['actor_attributes.login'] != 'nan' else '']))), 1)
        follows_df['target'] = follows_df.apply(lambda x: empty_string.join(list(set([x['payload.target.login'] if x['payload.target.login'] != 'nan' else '', x['target.login'] if x['target.login'] != 'nan' else '']))), 1)
        print colored('End normalizing payload username and target username', 'green')
        assert '' not in follows_df['username']
        print 'Are there any nulls in the `username` column?: ' + str(pd.isnull(follows_df['username']).any())
        print 'Are there any nulls in the `target` column?: ' + str(pd.isnull(follows_df['target']).any())
        print colored('End verifiying payload username', 'green')
        print colored('Starting normalizing countables', 'green')
        #created_df['repository'] = pd.concat([created_df['repository.url'].fillna(''), created_df['repo.url'].fillna('')], ignore_index=True)
        follows_df['target-followers'] = follows_df.apply(lambda x: empty_string.join(list(set([x['target.followers'] if x['target.followers'] != 'nan' else '', x['payload.target.followers'] if x['payload.target.followers'] != 'nan' else '']))), 1)
        follows_df['target-repos'] = follows_df.apply(lambda x: empty_string.join(list(set([x['target.repos'] if x['target.repos'] != 'nan' else '', x['payload.target.repos'] if x['payload.target.repos'] != 'nan' else '']))), 1)
        print colored('End normalizing countables', 'green')
        #assert '' not in follows_df['repository']
        #print 'Are there any nulls in the `repository` column?: ' + str(pd.isnull(follows_df['repository']).any())
        #print colored('End verifiying repository', 'green')
        print colored('Droping useless before-concat columns', 'red')
        follows_df = follows_df.drop('actor.login', axis=1)
        follows_df = follows_df.drop('actor', axis=1)
        follows_df = follows_df.drop('actor_attributes.login', axis=1)
        follows_df = follows_df.drop('payload.target.login', axis=1)
        follows_df = follows_df.drop('target.login', axis=1)
        follows_df = follows_df.drop('target.followers', axis=1)
        follows_df = follows_df.drop('target.repos', axis=1)
        follows_df = follows_df.drop('payload.target.followers', axis=1)
        follows_df = follows_df.drop('payload.target.repos', axis=1)
        print colored('Drop of 5 columns complete', 'red')
        print follows_df.dtypes  # can verify
        print follows_df.head(20)
        print follows_df.tail(20)
        print colored('Writing normalized CSV..', 'blue')
        follows_df.to_csv(ultimate_path + 'normalized_' + follows_csv_filename, mode='wb', sep=';', encoding='UTF-8')

        if WAIT_FOR_USER:
            raw_input("Press Enter to continue to issues events...")

    if resume == 'Issues' or resume is None:
        resume = None
        print colored('Reading the ' + issues_csv_filename + ' file.. it may take a while...', 'red')
        issues_df = pd.read_csv(ultimate_path + issues_csv_filename, header=0,
                                sep=',', na_values=['', None], error_bad_lines=False, quotechar='"')
        print colored('Reading issues_csv_filename done.', 'green')
        print colored('Index of issues_df is:', 'green')
        print issues_df.index[0:5]
        print issues_df.dtypes
        print issues_df.head(20)
        print issues_df.tail(20)
        print colored('Parsing IsoDate Zulu time to proper datetime object', 'green')
        issues_df['created_at'] = issues_df['created_at'].apply(lambda x: dateutil.parser.parse(x))
        print colored('Fixing 6 columns types..', 'green')
        issues_df['repo.url'] = issues_df['repo.url'].astype(str)
        issues_df['repository.url'] = issues_df['repository.url'].astype(str)
        issues_df['actor'] = issues_df['actor'].astype(str)
        issues_df['actor.login'] = issues_df['actor.login'].astype(str)
        issues_df['payload.actor'] = issues_df['payload.actor'].astype(str)
        issues_df['actor_attributes.login'] = issues_df['actor_attributes.login'].astype(str)
        print issues_df.dtypes  # can verify
        print issues_df.head(20)
        print issues_df.tail(20)
        print colored('Starting normalizing actor username', 'green')
        #created_df['object'] = pd.concat([created_df['payload.object'].fillna(''), created_df['payload.ref_type'].fillna('')], ignore_index=True)
        issues_df['username'] = issues_df.apply(lambda x: empty_string.join(list(set([x['actor.login'] if x['actor.login'] != 'nan' else '', x['payload.actor'] if x['payload.actor'] != 'nan' else '', x['actor'] if x['actor'] != 'nan' else '', x['actor_attributes.login'] if x['actor_attributes.login'] != 'nan' else '']))), 1)
        print colored('End normalizing payload username', 'green')
        assert '' not in issues_df['username']
        print 'Are there any nulls in the `username` column?: ' + str(pd.isnull(issues_df['username']).any())
        print colored('End verifiying payload username', 'green')
        print colored('Starting normalizing repository', 'green')
        #created_df['repository'] = pd.concat([created_df['repository.url'].fillna(''), created_df['repo.url'].fillna('')], ignore_index=True)
        issues_df['repository'] = issues_df.apply(lambda x: remove_links(empty_string.join(list(set([remove_links(x['repository.url']) if x['repository.url'] != 'nan' else '', remove_links(x['repo.url']) if x['repo.url'] != 'nan' else ''])))), 1)
        print colored('End normalizing repository', 'green')
        assert '' not in issues_df['repository']
        print 'Are there any nulls in the `repository` column?: ' + str(pd.isnull(issues_df['repository']).any())
        print colored('End verifiying repository', 'green')
        print colored('Droping useless before-concat columns', 'red')
        issues_df = issues_df.drop('repo.url', axis=1)
        issues_df = issues_df.drop('repository.url', axis=1)
        issues_df = issues_df.drop('actor', axis=1)
        issues_df = issues_df.drop('actor.login', axis=1)
        issues_df = issues_df.drop('payload.actor', axis=1)
        issues_df = issues_df.drop('actor_attributes.login', axis=1)
        print colored('Drop of 4 columns complete', 'red')
        print issues_df.dtypes  # can verify
        print issues_df.head(20)
        print issues_df.tail(20)
        print colored('Writing normalized CSV..', 'blue')
        issues_df.to_csv(ultimate_path + 'normalized_' + issues_csv_filename, mode='wb', sep=';', encoding='UTF-8')

        print colored('-------------------- AGGREGATION --------------', 'red')
        # assign creation dates to repos in MemberAdd table
        if aggr == 'native':
            issues_df = issues_df.merge(created_df[created_df['object'] == 'repository'], on='repository', how='left', suffixes=('"_issue', '_cr'))
        elif aggr == 'sql':
            #print colored('reading globals to sqldf', 'yellow')
            #pysqldf = lambda q: sqldf(q, globals())
            print colored('preparing SQL..', 'yellow')
            q = """
            SELECT
            iss.created_at, iss.username, iss.repository, cr.created_at
            FROM
            issues_df iss
            LEFT JOIN
            created_df cr
            ON iss.repository = cr.repository
            WHERE
            cr.object == 'repository';
            """
            print colored('executing SQL..', 'yellow')
            issues_df = sqldf(q, globals())
            print colored('done executing SQL...', 'green')
        else:
            assert False
        print colored('-------------------- AGGREGATION completed --------------', 'yellow')
        print issues_df.index[0:5]
        print issues_df.dtypes
        print issues_df.head(50)
        print issues_df.tail(50)

        print colored('Writing normalized CSV 2..', 'blue')
        issues_df.to_csv(ultimate_path + 'normalized_cr_' + issues_csv_filename, mode='wb', sep=';', encoding='UTF-8')

        if WAIT_FOR_USER:
            raw_input("Press Enter to continue to pushes events...")

    if resume == 'Pushes' or resume is None:
        resume = None
        print colored('Reading the ' + pushes_csv_filename + ' file.. it may take a while...', 'red')
        pushes_df = pd.read_csv(ultimate_path + pushes_csv_filename, header=0,
                                sep=',', na_values=['', None], error_bad_lines=False, quotechar='"')
        print colored('Reading pushes_csv_filename done.', 'green')
        print colored('Index of pushes_df is:', 'green')
        print pushes_df.index[0:5]
        print pushes_df.dtypes
        print pushes_df.head(20)
        print pushes_df.tail(20)
        print colored('Parsing IsoDate Zulu time to proper datetime object', 'green')
        pushes_df['created_at'] = pushes_df['created_at'].apply(lambda x: dateutil.parser.parse(x))
        print colored('Fixing 5 columns types..', 'green')
        pushes_df['actor.login'] = pushes_df['actor.login'].astype(str)
        pushes_df['payload.actor'] = pushes_df['payload.actor'].astype(str)
        pushes_df['actor_attributes.login'] = pushes_df['actor_attributes.login'].astype(str)
        pushes_df['repository.url'] = pushes_df['repository.url'].astype(str)
        pushes_df['repo.url'] = pushes_df['repo.url'].astype(str)
        print pushes_df.dtypes  # can verify
        print pushes_df.head(20)
        print pushes_df.tail(20)
        print colored('Starting normalizing actor username', 'green')
        #created_df['object'] = pd.concat([created_df['payload.object'].fillna(''), created_df['payload.ref_type'].fillna('')], ignore_index=True)
        pushes_df['username'] = pushes_df.apply(lambda x: empty_string.join(list(set([x['actor.login'] if x['actor.login'] != 'nan' else '', x['payload.actor'] if x['payload.actor'] != 'nan' else '', x['actor_attributes.login'] if x['actor_attributes.login'] != 'nan' else '']))), 1)
        print colored('End normalizing payload username', 'green')
        assert '' not in pushes_df['username']
        print 'Are there any nulls in the `username` column?: ' + str(pd.isnull(pushes_df['username']).any())
        print colored('End verifiying payload username', 'green')
        print colored('Starting normalizing repository', 'green')
        #created_df['repository'] = pd.concat([created_df['repository.url'].fillna(''), created_df['repo.url'].fillna('')], ignore_index=True)
        pushes_df['repository'] = pushes_df.apply(lambda x: remove_links(empty_string.join(list(set([remove_links(x['repository.url']) if x['repository.url'] != 'nan' else '', remove_links(x['repo.url']) if x['repo.url'] != 'nan' else ''])))), 1)
        print colored('End normalizing repository', 'green')
        assert '' not in pushes_df['repository']
        print 'Are there any nulls in the `repository` column?: ' + str(pd.isnull(pushes_df['repository']).any())
        print colored('End verifiying repository', 'green')
        print colored('Droping useless before-concat columns', 'red')
        pushes_df = pushes_df.drop('actor.login', axis=1)
        pushes_df = pushes_df.drop('payload.actor', axis=1)
        pushes_df = pushes_df.drop('actor_attributes.login', axis=1)
        pushes_df = pushes_df.drop('repository.url', axis=1)
        pushes_df = pushes_df.drop('repo.url', axis=1)
        print colored('Drop of 5 columns complete', 'red')
        print pushes_df.dtypes  # can verify
        print pushes_df.head(20)
        print pushes_df.tail(20)
        print colored('Writing normalized CSV..', 'blue')
        pushes_df.to_csv(ultimate_path + 'normalized_' + pushes_csv_filename, mode='wb', sep=';', encoding='UTF-8')

        print colored('-------------------- AGGREGATION --------------', 'red')
        # assign creation dates to repos in MemberAdd table
        if aggr == 'native':
            pushes_df = pushes_df.merge(created_df[created_df['object'] == 'repository'], on='repository', how='left', suffixes=('_push', '_cr'))
        elif aggr == 'sql':
            #print colored('reading globals to sqldf', 'yellow')
            #pysqldf = lambda q: sqldf(q, globals())
            print colored('preparing SQL..', 'yellow')
            q = """
            SELECT
            ps.created_at, ps.username, ps.repository, cr.created_at
            FROM
            pushes_df ps
            LEFT JOIN
            created_df cr
            ON ps.repository = cr.repository
            WHERE
            cr.object == 'repository';
            """
            print colored('executing SQL..', 'yellow')
            pushes_df = sqldf(q, globals())
            print colored('done executing SQL...', 'green')
        else:
            assert False
        print colored('-------------------- AGGREGATION completed --------------', 'yellow')
        print pushes_df.index[0:5]
        print pushes_df.dtypes
        print pushes_df.head(50)
        print pushes_df.tail(50)

        print colored('Writing normalized CSV 2..', 'blue')
        pushes_df.to_csv(ultimate_path + 'normalized_cr_' + pushes_csv_filename, mode='wb', sep=';', encoding='UTF-8')

        if WAIT_FOR_USER:
            raw_input("Press Enter to quit program...")
