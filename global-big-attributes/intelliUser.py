class GitUser():

    key = None

    def __init__(self):
        self.data = []

    def __init__(self, login):
        self.data = []
        self.key = login
        self.login = login

    login = None
    fullname = None
    followers = set()
    followers_date = None
    following = set()
    following_date = None

    followers_count = 0
    repositories_count = 0

    repositories = set()
    repositories_date = None

    his_repos_how_many_developers = 0
    contributing_to = set()

    def setKey(self, key):
        self.key = key

    def getKey(self):
        return self.key

    def setLogin(self, login):
        self.login = login

    def getLogin(self):
        return self.login

    def setFullname(self, fullname):
        self.fullname = fullname

    def getFullname(self):
        return self.fullname

    def setFollowers(self, followers):
        self.followers = followers

    def addFollower(self, follower):
        self.follower.add(follower)

    def setFollowersDate(self, followers_date):
        self.followers_date = followers_date

    def getFollowers(self):
        return self.followers

    def setFollowing(self, following):
        self.following = following

    def addFollowing(self, followed):
        self.following.add(followed)

    def setFollowingDate(self, following_date):
        self.following_date = following_date

    def getFollowing(self):
        return self.following

    def setRepositories(self, repositories):
        self.repositories = repositories

    def setRepositoriesgDate(self, repositories_date):
        self.repositories_date = repositories_date

    def getRepositories(self):
        return self.repositories

    def setFollowersCount(self, followers_count):
        self.followers_count = followers_count

    def setRepositoriesCount(self, repositories_count):
        self.repositories_count = repositories_count

    def getHisReposHowManyDevelopers(self):
        return self.his_repos_how_many_developers

    def setHisReposHowManyDevelopers(self, his_repos_how_many_developers):
        self.his_repos_how_many_developers = his_repos_how_many_developers
