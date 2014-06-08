import pandas as pd
import dateutil.parser
from termcolor import colored
import json

WAIT_FOR_USER = True

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
print team_adds_df.dtypes  # can verify
print team_adds_df.head(20)
print team_adds_df.tail(20)
print colored('Starting normalizing username', 'green')
team_adds_df['username'] = pd.concat([team_adds_df['actor'], team_adds_df['actor.login'], team_adds_df['actor_attributes.login']], ignore_index=True)
print colored('End normalizing username', 'green')
assert '' not in team_adds_df['username']
print 'Are there any nulls in username column?: ' + str(pd.isnull(team_adds_df['username']).any())
print colored('End verifiying usernames', 'green')
print colored('Starting normalizing repository', 'green')
team_adds_df['repository'] = pd.concat([team_adds_df['repository.url'].fillna(''), team_adds_df['repo.url'].fillna(''), team_adds_df['payload.repository.url'].fillna('')], ignore_index=True)
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
print member_adds_df.dtypes  # can verify
print member_adds_df.head(20)
print member_adds_df.tail(20)
print colored('Starting normalizing username', 'green')
member_adds_df['payload.member'] = member_adds_df['payload.member'].apply(lambda x: json.loads(x)['login'] if ',' in str(x) else x)
member_adds_df['username'] = member_adds_df.apply(lambda x: x['payload.member.login'] if ((not pd.isnull(x['payload.member.login'])) and (x['payload.member.login'] != '')) else x['payload.member'], 1)
print colored('End normalizing username', 'green')
assert '' not in member_adds_df['username']
print 'Are there any nulls in repository column?: ' + str(pd.isnull(member_adds_df['username']).any())
print colored('End verifiying usernames', 'green')
print colored('Starting normalizing repository', 'green')
member_adds_df['repository'] = pd.concat([member_adds_df['repository.url'].fillna(''), member_adds_df['payload.repository.url'].fillna(''), member_adds_df['repo.url']].fillna(''), ignore_index=True)
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

created_df = pandas.read_csv(ultimate_path + created_at_filename, header=0,
                             sep=',', na_values=['', None], error_bad_lines=False, parse_dates=[5], quotechar='"')
print 'Reading created_data_frame done.'
print created_df.dtypes
print created_df.head()
print created_df.tail()
print 'Parsing IsoDate Zulu time to proper datetime object'
team_adds_df['created_at'] = team_adds_df['created_at'].apply(lambda x: dateutil.parser.parse(x))
print created_df.dtypes  # can verify
print created_df.head()
print created_df.tail()

if WAIT_FOR_USER:
    raw_input("Press Enter to continue to pulls events...")

pulls_df = pandas.read_csv(ultimate_path + pulls_csv_filename, header=0,
                           sep=',', na_values=['', None], error_bad_lines=False, parse_dates=[1], quotechar='"')
print 'Reading pulls_csv_filename done..'
print pulls_df.dtypes
print pulls_df.head()
print pulls_df.tail()
print 'Parsing IsoDate Zulu time to proper datetime object'
team_adds_df['created_at'] = team_adds_df['created_at'].apply(lambda x: dateutil.parser.parse(x))
print pulls_df.dtypes  # can verify
print pulls_df.head()
print pulls_df.tail()

if WAIT_FOR_USER:
    raw_input("Press Enter to continue to follows events...")

follows_df = pandas.read_csv(ultimate_path + follows_csv_filename, header=0,
                             sep=',', na_values=['', None], error_bad_lines=False, parse_dates=[1], quotechar='"')
print 'Reading follows_csv_filename done...'
print follows_df.dtypes
print follows_df.head()
print follows_df.tail()
print 'Parsing IsoDate Zulu time to proper datetime object'
team_adds_df['created_at'] = team_adds_df['created_at'].apply(lambda x: dateutil.parser.parse(x))
print follows_df.dtypes  # can verify
print follows_df.head()
print follows_df.tail()

if WAIT_FOR_USER:
    raw_input("Press Enter to continue to issues events...")

issues_df = pandas.read_csv(ultimate_path + issues_csv_filename, header=0,
                            sep=',', na_values=['', None], error_bad_lines=False, parse_dates=[1], quotechar='"')
print 'Reading issues_csv_filename done....'
print issues_df.dtypes
print issues_df.head()
print issues_df.tail()
print 'Parsing IsoDate Zulu time to proper datetime object'
team_adds_df['created_at'] = team_adds_df['created_at'].apply(lambda x: dateutil.parser.parse(x))
print issues_df.dtypes  # can verify
print issues_df.head()
print issues_df.tail()

if WAIT_FOR_USER:
    raw_input("Press Enter to quit program...")
