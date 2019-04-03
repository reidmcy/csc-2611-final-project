import requests
#import fake_useragent
import json
import time
import urllib
import os
import os.path
import traceback
import datetime
import gzip
import sys

import cfscrape



headers = {'user-agent':'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.14 (KHTML, like Gecko) Chrome/24.0.1292.0 Safari/537.14'}


popularUsers = 'https://gab.ai/popular/users'

userURL = "https://gab.ai/users/{}"
followersURL = "https://gab.ai/users/{}/followers"
followingURL = "https://gab.ai/users/{}/following"
postsURL = "https://gab.ai/api/feed/{}"

#Not sure what the conversation_parent does, but I'm keeping it
commentsURL = "https://gab.ai/api/feed/{}/comments?includes=post.conversation_parent"

class PrivateAccount(Exception):
    pass

class LoadIssues(Exception):
    pass

class LockError(Exception):
    pass

class TimeoutError(Exception):
    pass

class GabSession(object):

    def __init__(self, username = None, password = None, login = True):
        #self.ua = fake_useragent.UserAgent()
        self.headers = None
        self.session = cfscrape.create_scraper()
        self.login = login
        if username is None:
            self.username = os.environ['GAB_USERNAME']
        else:
            self.username = username
        if password is None:
            self.password = os.environ['GAB_PASSWORD']
        else:
            self.password = password
        self.setupSession()

    def setupSession(self):
        self.headers = headers#{'user-agent' : self.ua.random}
        f = self.session.get('https://gab.ai/auth/login', headers=self.headers)
        #self.session = f.cookies
        if self.login:
            token = f.text.split('"_token" value="')[1].split('"')[0]
            self.session.post('https://gab.ai/auth/login', headers=self.headers, cookies = self.session.cookies, data={'_token':token, 'password': self.password, 'username': self.username})

    def get(self, url, timeout = 0, jsonRet = True, ignorePrivate = False):
        if timeout > 8:
            raise TimeoutError(f"Fetching {url} took too long")
            #self.setupSession()
        r = self.session.get(url, timeout = 10)
        if r.ok or (ignorePrivate and r.status_code == 400):
            if jsonRet:
                return r.json()
            else:
                return r.content
        elif r.status_code == 429 or r.status_code == 502 or r.status_code == 500:
            #print(url, timeout)
            #Looks like 20 seconds is the timeout, so doing 21 to be safe
            time.sleep(20 + 2**timeout)
            return self.get(url, timeout = timeout + 1)
        elif r.status_code == 400:
            raise PrivateAccount(f"{url} is private")
        else:
            raise RuntimeError(f"Bad URL: {url}, {r}")

    def getUserInfo(self, username):
        return self.get(userURL.format(username), ignorePrivate = True)

    def downloadImage(self, url, saveDir):

        fileName = os.path.basename(url)

        saveDirPath = os.path.join(saveDir, os.path.basename(os.path.dirname(url)), fileName[:2], fileName[2:4])

        saveName = os.path.join(saveDirPath, fileName)

        if not os.path.isdir(saveDirPath):
            os.makedirs(saveDirPath, exist_ok = True)
        elif os.path.isfile(saveName):
            return False

        r = self.get(url, jsonRet = False)

        with open(saveName, 'wb') as f:
            f.write(r)

        return True

    def simplePaginatedFetch(self, url):
        dat = []
        pageDat = self.get(url)
        dat += pageDat['data']
        count = pageDat['count']

        remaining = pageDat['count'] - len(dat)
        index = 0
        while remaining > 0:
            index += 30
            pageDat = self.get(url + f"?before={index}")
            dat += pageDat['data']
            remaining -= len(pageDat['data'])

        return dat

    def getPosts(self, username):
        posts = []
        postBatch = self.get(postsURL.format(username))

        posts += postBatch['data']

        while len(postBatch['data']) > 0:
            lastDate = urllib.parse.quote_plus(postBatch['data'][-1]['published_at'])
            postBatch = self.get(postsURL.format(username) + f'?before={lastDate}')
            posts += postBatch['data']
        return posts

    def getComments(self, username):
        comments = []
        commentsBatch = self.get(commentsURL.format(username))

        comments += commentsBatch['data']

        index = 0

        while len(commentsBatch['data']) > 29:
            index += 30
            commentsBatch = self.get(commentsURL.format(username) + f'+before={index}')
            comments += commentsBatch['data']
        return comments

    def fetchUser(self, username):
        tStart = time.time()
        s = f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} : Fetching {username}'
        userInfo = self.get(userURL.format(username))

        print("{} {} followers, {} following, {} posts".format(s, userInfo['follower_count'], userInfo['following_count'], userInfo['post_count']), end = '\n', flush = True)
        try:
            following = self.simplePaginatedFetch(followingURL.format(username))
            #print(" .", end = '')
            followers = self.simplePaginatedFetch(followersURL.format(username))
            #print(".", end = '')
            posts = self.getPosts(username)
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {username} downloaded in {time.time() - tStart :.0f}s", end = '\n', flush = True)
        except PrivateAccount:
            #print(f" {username} is private")
            followers = []
            following = []
            posts = []
        #comments = self.getComments(username)
        #print(".", end = '\n')

        return {"user_info" : userInfo,
                "followers": followers,
                "following" : following,
                "posts" : posts,
                #"comments" : comments,
                }


class UserFetcher(object):
    def __init__(self, username, outputDir):
        self.username = username
        self.outputDir = outputDir
        self.lockFile = f"{outputDir}/{username}.lock"
        self.outputFile = f"{outputDir}/{username}.json.gz"
        self.lock = None
        self.lockFileHandle = None

    def __enter__(self):
        if os.path.isfile(self.outputFile):
            self.lock = False
            #raise LockError(f"{self.username} already downloaded/or being download")
        else:
            try:
                self.lockFileHandle = open(self.lockFile, 'x')
            except FileExistsError:
                self.lock = False
            else:
                self.lock = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.lock:
            self.lockFileHandle.close()
            os.remove(self.lockFile)

    def download(self, session, raiseExceptions = False):
        if not self.lock:
            print(f"{self.username} already downloaded")
            return []
        try:
            userDict = session.fetchUser(self.username)
        except PrivateAccount:
            print(f"{self.username} is private")
            with gzip.open(self.outputFile, 'wt') as f:
                f.write("Private")
            return None
        except Exception as e:
            if raiseExceptions:
                raise
            else:
                print(e, traceback.format_exc())
                return None
        except TimeoutError:
            if raiseExceptions:
                raise
            else:
                print(f"{self.username} is timed out")
                return None
        else:
            with gzip.open(self.outputFile, 'wt') as f:
                f.write(json.dumps(userDict))
            return list(set([e['username'] for e in userDict['following']] + [e['username'] for e in userDict['followers']]))
