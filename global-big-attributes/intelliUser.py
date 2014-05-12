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
