class GitUser():

    key = None

    def __init__(self):
        self.data = []

    login = None
    fullname = None
    followers = None
    following = None
    repositories = None

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

    def getFollowers(self):
        return self.followers

    def setFollowing(self, following):
        self.following = following

    def getFollowing(self):
        return self.following

    def setRepositories(self, repositories):
        self.repositories = repositories

    def getRepositories(self):
        return self.repositories
