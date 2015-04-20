import scream
from github import Github, UnknownObjectException, GithubException
import sys
import time
import argparse
# import ElementTree based on the python version
try:
    import elementtree.ElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
try:
    import MySQLdb as MSQL
except ImportError:
    import _mysql as MSQL

version_name = 'version 1.0 codename: Poliglot'
auth_with_tokens = True

github_clients = list()
github_clients_ids = list()

safe_margin = 100
timeout = 50
sleepy_head_time = 25
force_raise = False
show_trace = False

record_count = None
IP_ADDRESS = "10.4.4.3"  # Be sure to update this to your needs


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", help="verbose messaging ? [True/False]", action="store_true")
    args = parser.parse_args()
    if args.verbose:
        scream.intelliTag_verbose = True
        scream.say("verbosity turned on")

    scream.say('Start main execution')
    scream.say('Welcome to WikiTeams.pl GitHub repo analyzer!')
    scream.say(version_name)

    secrets = []

    credential_list = []
    # reading the secrets, the Github factory objects will be created in next paragraph
    with open('pass.txt', 'r') as passfile:
        line__id = 0
        for line in passfile:
            line__id += 1
            secrets.append(line)
            if line__id % 4 == 0:
                login_or_token__ = str(secrets[0]).strip()
                pass_string = str(secrets[1]).strip()
                client_id__ = str(secrets[2]).strip()
                client_secret__ = str(secrets[3]).strip()
                credential_list.append({'login': login_or_token__, 'pass': pass_string, 'client_id': client_id__, 'client_secret': client_secret__})
                del secrets[:]

    scream.say(str(len(credential_list)) + ' full credentials successfully loaded')

    # with the credential_list list we create a list of Github objects, github_clients holds ready Github objects
    for credential in credential_list:
        if auth_with_tokens:
            local_gh = Github(login_or_token=credential['pass'], client_id=credential['client_id'],
                              client_secret=credential['client_secret'], user_agent=credential['login'],
                              timeout=timeout)
            github_clients.append(local_gh)
            github_clients_ids.append(credential['login'])
            scream.say(local_gh.rate_limiting)
        else:
            local_gh = Github(credential['login'], credential['pass'])
            github_clients.append(local_gh)
            scream.say(local_gh.rate_limiting)

    scream.cout('How many Github objects in github_clients: ' + str(len(github_clients)))
    scream.cout('Assigning current github client to the first object in a list')

    github_client = github_clients[0]
    lapis = local_gh.get_api_status()
    scream.say('Current status of GitHub API...: ' + lapis.status + ' (last update: ' + str(lapis.last_updated) + ')')

    # init connection to database
    first_conn = MSQL.connect(host=IP_ADDRESS, port=3306, user=open('mysqlu.dat', 'r').read(),
                              passwd=open('mysqlp.dat', 'r').read(),
                              db="github", connect_timeout=50000000,
                              charset='utf8', init_command='SET NAMES UTF8',
                              use_unicode=True)
    print 'Testing MySql connection...'
    print 'Pinging database: ' + (str(first_conn.ping(True)) if first_conn.ping(True) is not None else 'NaN')
    cursor = first_conn.cursor()
    cursor.execute(r'SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = "%s"' % 'github')
    rows = cursor.fetchall()
    print 'There are: ' + str(rows[0][0]) + ' table objects in the local GHtorrent copy'
    cursor.execute(r'SELECT table_name FROM information_schema.tables WHERE table_schema = "%s"' % 'github')
    rows = cursor.fetchall()
    if (u'users', ) and (u'projects', ) in rows:
        print 'All neccesary tables are there.'
    else:
        print 'Your database does not fit a typical description of a GitHub Torrent copy..'
        sys.exit(0)

    sample_tb_name = raw_input("Please enter table/view name (of chosen data sample): ")
    output_tb_name = raw_input("Please enter output table/view name (where we'll put results): ")
    cursor.execute(r'select count(distinct repo_id) from ' + str(sample_tb_name))
    rows = cursor.fetchall()
    record_count = rows[0][0]
    cursor.close()

    scream.say("Database seems to be working. Move on to getting list of repositories.")

    # populate list of repositories to memory
    cursor = first_conn.cursor()
    print 'Querying all names from the observations set.. This can take around 25-30 sec.'
    cursor.execute(r'select r.id as id, r.url as url from ' + str(sample_tb_name) + ' as s join projects r on s.repo_id = r.id')
    # if you are interested in how this table was created, you will probably need to read our paper and contact us as well
    # because we have some more tables with aggregated data compared to standard GitHub Torrent collection
    row = cursor.fetchone()
    iterator = 1.0

    while row is not None:
        repoid = str(row[0])
        url = str(row[1])
        iterator += 1
        print "[Progress]: " + str((iterator / record_count) * 100) + "% ----------- "  # [names] size: " + str(len(names))

        repo_full_name = url.split("/")[-2] + "/" + url.split("/")[-1]
        print repo_full_name

        row = cursor.fetchone()

        repository = github_client.get_repo(repo_full_name)

        languages = repository.get_languages()

        print languages
