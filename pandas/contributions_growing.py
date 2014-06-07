import pandas as pd
import dateutil.parser

WAIT_FOR_USER = True

created_at_filename = 'created.csv'

pushes_csv_filename = 'pushes.csv'
pulls_csv_filename = 'pulls.csv'
follows_csv_filename = 'follows.csv'
issues_csv_filename = 'issues.csv'
team_adds_csv_filename = 'team-adds.csv'

ultimate_path = '../'

team_adds_df = pd.read_csv(ultimate_path + team_adds_csv_filename, header=0,
                           sep=',', na_values=['', None], error_bad_lines=False, quotechar='"')
print 'Reading team_adds_csv_filename done.'
print 'Index of team_adds_df is:'
print team_adds_df.index
print team_adds_df.dtypes
print team_adds_df.head()
print team_adds_df.tail()
print 'Parsing IsoDate Zulu time to proper datetime object'
team_adds_df['created_at'] = team_adds_df['created_at'].apply(lambda x: dateutil.parser.parse(x))
print team_adds_df.dtypes  # can verify
print team_adds_df.head()
print team_adds_df.tail()
print 'Starting normalizing username'
team_adds_df['username'] = pd.concat([team_adds_df['actor'], team_adds_df['actor.login'], team_adds_df['actor_attributes.login']], ignore_index=True)
print 'End normalizing username'
assert '' not in team_adds_df['username']
print pd.isnull(team_adds_df['username']).any()
print 'End verifiying usernames'
print 'Starting normalizing repository'
team_adds_df['repository'] = pd.concat([team_adds_df['repository.url'], team_adds_df['repo.url'], team_adds_df['payload.repository.url']], ignore_index=True)
print 'End normalizing repository'
assert '' not in team_adds_df['repository']
print pd.isnull(team_adds_df['repository']).any()
print 'End verifiying repository'
print 'Droping useless before-concat columns'
team_adds_df = team_adds_df.drop('actor', axis=1)
team_adds_df = team_adds_df.drop('actor.login', axis=1)
team_adds_df = team_adds_df.drop('actor_attributes.login', axis=1)
team_adds_df = team_adds_df.drop('repository.url', axis=1)
team_adds_df = team_adds_df.drop('repo.url', axis=1)
team_adds_df = team_adds_df.drop('payload.repository.url', axis=1)
print 'Drop complete'
print team_adds_df.dtypes  # can verify
print team_adds_df.head()
print team_adds_df.tail()

if WAIT_FOR_USER:
    raw_input("Press Enter to continue to creation events...")

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
