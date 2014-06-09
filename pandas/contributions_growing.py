import pandas as pd
import dateutil.parser
from termcolor import colored
import json
import getopt
import sys
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

if __name__ == "__main__":
    resume = None

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hr:vi", ["help", "resume=", "verbose", "interactive"])
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
        team_adds_df['username'] = team_adds_df.apply(lambda x: empty_string.join(list(set([x['actor'], x['actor.login'], x['actor_attributes.login']]))), 1)
        print colored('End normalizing username', 'green')
        assert '' not in team_adds_df['username']
        print 'Are there any nulls in username column?: ' + str(pd.isnull(team_adds_df['username']).any())
        print colored('End verifiying usernames', 'green')
        print colored('Starting normalizing repository', 'green')
        #team_adds_df['repository'] = pd.concat([team_adds_df['repository.url'].fillna(''), team_adds_df['repo.url'].fillna(''), team_adds_df['payload.repository.url'].fillna('')], ignore_index=True)
        team_adds_df['repository'] = team_adds_df.apply(lambda x: empty_string.join(list(set([x['repository.url'], x['repo.url'], x['payload.repository.url']]))), 1)
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
        print colored('Reading the ' + member_adds_csv_filename + ' file.. it may take a while...', 'red')
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
        member_adds_df['username'] = member_adds_df.apply(lambda x: empty_string.join(list(set([x['payload.member.login'], x['payload.member']]))), 1)
        print colored('End normalizing username', 'green')
        assert '' not in member_adds_df['username']
        print 'Are there any nulls in repository column?: ' + str(pd.isnull(member_adds_df['username']).any())
        print colored('End verifiying usernames', 'green')
        print colored('Starting normalizing repository', 'green')
        #member_adds_df['repository'] = pd.concat([member_adds_df['repository.url'].fillna(''), member_adds_df['payload.repository.url'].fillna(''), member_adds_df['repo.url'].fillna('')], ignore_index=True)
        member_adds_df['repository'] = member_adds_df.apply(lambda x: empty_string.join(list(set([x['repository.url'], x['payload.repository.url'], x['repo.url']]))), 1)
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
        created_df['repository'] = created_df.apply(lambda x: empty_string.join(list(set([x['repository.url'] if x['repository.url'] != 'nan' else '', x['repo.url'] if x['repo.url'] != 'nan' else '']))), 1)
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
        # assign creation dates to repos in MemberAdd table
        member_adds_df = member_adds_df.join(created_df[created_df['object']=='repository'], on='repository', how='inner', lsuffix="_memb", rsuffix="_cr")
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
        pulls_df['head.repo'] = pulls_df['head.repo'].astype(str)
        pulls_df['repo.url'] = pulls_df['repo.url'].astype(str)
        print pulls_df.dtypes  # can verify
        print pulls_df.head(20)
        print pulls_df.tail(20)
        print colored('Starting normalizing actor username', 'green')
        #created_df['object'] = pd.concat([created_df['payload.object'].fillna(''), created_df['payload.ref_type'].fillna('')], ignore_index=True)
        pulls_df['username'] = pulls_df.apply(lambda x: empty_string.join(list(set([x['payload.object'], x['payload.ref_type']]))), 1)
        print colored('End normalizing payload username', 'green')
        assert '' not in pulls_df['username']
        print 'Are there any nulls in the `username` column?: ' + str(pd.isnull(pulls_df['username']).any())
        print colored('End verifiying payload username', 'green')
        print colored('Starting normalizing repository', 'green')
        #created_df['repository'] = pd.concat([created_df['repository.url'].fillna(''), created_df['repo.url'].fillna('')], ignore_index=True)
        pulls_df['repository'] = pulls_df.apply(lambda x: empty_string.join(list(set([x['head.repo'], x['repo.url']]))), 1)
        print colored('End normalizing repository', 'green')
        assert '' not in pulls_df['repository']
        print 'Are there any nulls in the `repository` column?: ' + str(pd.isnull(pulls_df['repository']).any())
        print colored('End verifiying repository', 'green')
        print colored('Droping useless before-concat columns', 'red')
        pulls_df = pulls_df.drop('actor.login', axis=1)
        pulls_df = pulls_df.drop('payload.actor', axis=1)
        pulls_df = pulls_df.drop('head.repo', axis=1)
        pulls_df = pulls_df.drop('repo.url', axis=1)
        print colored('Drop of 4 columns complete', 'red')
        print pulls_df.dtypes  # can verify
        print pulls_df.head(20)
        print pulls_df.tail(20)
        print colored('Writing normalized CSV..', 'blue')
        pulls_df.to_csv(ultimate_path + 'normalized_' + pushes_csv_filename, mode='wb', sep=';', encoding='UTF-8')

        print colored('-------------------- AGGREGATION --------------', 'red')
        # assign creation dates to repos in MemberAdd table
        pulls_df = pulls_df.join(created_df[created_df['object']=='repository'], on='repository', how='inner', lsuffix="_pull", rsuffix="_cr")
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
        follows_df = pd.read_csv(ultimate_path + pulls_csv_filename, header=0,
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
        print follows_df.dtypes  # can verify
        print follows_df.head(20)
        print follows_df.tail(20)
        print colored('Starting normalizing actor username and target username', 'green')
        follows_df['actor'] = follows_df['actor'].apply(lambda x: json.loads(x)['login'] if ',' in str(x) else x)
        follows_df['username'] = follows_df.apply(lambda x: empty_string.join(list(set([x['actor'], x['actor.login'], x['actor_attributes.login']]))), 1)
        follows_df['target'] = follows_df.apply(lambda x: empty_string.join(list(set([x['payload.target.login'], x['target.login']]))), 1)
        print colored('End normalizing payload username and target username', 'green')
        assert '' not in follows_df['username']
        print 'Are there any nulls in the `username` column?: ' + str(pd.isnull(follows_df['username']).any())
        print 'Are there any nulls in the `target` column?: ' + str(pd.isnull(follows_df['target']).any())
        print colored('End verifiying payload username', 'green')
        print colored('Starting normalizing countables', 'green')
        #created_df['repository'] = pd.concat([created_df['repository.url'].fillna(''), created_df['repo.url'].fillna('')], ignore_index=True)
        follows_df['target-followers'] = follows_df.apply(lambda x: empty_string.join(list(set([x['target.followers'], x['payload.target.followers']]))), 1)
        follows_df['target-repos'] = follows_df.apply(lambda x: empty_string.join(list(set([x['target.repos'], x['payload.target.repos']]))), 1)
        print colored('End normalizing countables', 'green')
        assert '' not in follows_df['repository']
        print 'Are there any nulls in the `repository` column?: ' + str(pd.isnull(follows_df['repository']).any())
        print colored('End verifiying repository', 'green')
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

        if WAIT_FOR_USER:
            raw_input("Press Enter to quit program...")
