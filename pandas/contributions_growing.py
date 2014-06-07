import pandas

created_at_filename = 'created.csv'

pushes_csv_filename = 'pushes.csv'
pulls_csv_filename = 'pulls.csv'
follows_csv_filename = 'follows.csv'
issues_csv_filename = 'issues.csv'
team_adds_csv_filename = 'team_adds.csv'

ultimate_path = '../'

team_adds_df = pandas.read_csv(ultimate_path + team_adds_csv_filename, header=0,
                             sep=',', na_values=['', None], error_bad_lines=False, parse_dates=[1], quotechar='"')
print 'Reading team_adds_csv_filename done.'
print team_adds_df.dtypes
print team_adds_df.head()
print team_adds_df.tail()

created_df = pandas.read_csv(ultimate_path + created_at_filename, header=0,
                             sep=',', na_values=['', None], error_bad_lines=False, parse_dates=[5], quotechar='"')
print 'Reading created_data_frame done.'
print created_df.dtypes
print created_df.head()
print created_df.tail()

pulls_df = pandas.read_csv(ultimate_path + pulls_csv_filename, header=0,
                           sep=',', na_values=['', None], error_bad_lines=False, parse_dates=[1], quotechar='"')
print 'Reading pulls_csv_filename done..'

follows_df = pandas.read_csv(ultimate_path + follows_csv_filename, header=0,
                             sep=',', na_values=['', None], error_bad_lines=False, parse_dates=[1], quotechar='"')
print 'Reading follows_csv_filename done...'

issues_df = pandas.read_csv(ultimate_path + issues_csv_filename, header=0,
                            sep=',', na_values=['', None], error_bad_lines=False, parse_dates=[1], quotechar='"')
print 'Reading issues_csv_filename done....'
