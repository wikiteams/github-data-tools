class GitUser():

    key = None

    def __init__(self):
        self.data = []

    login = None
    fullname = None
    followers = None
    followers_date = None
    following = None
    following_date = None
    repositories = None
    repositories_date = None

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

    def setFollowersDate(self, followers_date):
        self.followers_date = followers_date

    def getFollowers(self):
        return self.followers

    def setFollowing(self, following):
        self.following = following

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
