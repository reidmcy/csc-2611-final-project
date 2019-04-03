import gzip
import json
import collections.abc

saveDir =  'users_info'

class BadFileError(Exception):
    pass

class GabAccount(collections.abc.Mapping):
    def __init__(self, path):
        self.path = path
        try:
            with gzip.open(path, 'rt') as f:
                j = json.load(f)
        except json.decoder.JSONDecodeError:
            raise BadFileError(f"{path} is not decoding correctly")
        self.dat = j

    def __getitem__(self, key):
        return self.dat[key]

    def __len__(self):
        return len(self.dat)

    def __iter__(self):
        for k in self.dat:
            yield k

    def items(self):
        return self.dat.items()

    @property
    def name(self):
        return self['user_info']['username']

    def __repr__(self):
        return f"<GabAccount for {self.name}>"

    @property
    def followers(self):
        return [e['username'] for e in self['followers']]

    @property
    def following(self):
        return [e['username'] for e in self['following']]

    def posts(self):
        return [cleanPost(p) for p in self['posts']]

    def reposts(self):
        return [p for p in self.posts() if p['type'] == 'repost']

    def userInfo(self):
        return cleanUserInfo(self['user_info'])

    def getRepostCounts(self):
        reposts = {}
        for p in self.reposts():
            try:
                reposts[p['orginal_auth']] += 1
            except KeyError:
                reposts[p['orginal_auth']] = 1
        return reposts

userValues = [
            'username',
            'name',
            'created_at_month_label'
            'follower_count',
            'following_count',
            'post_count',
            'is_pro',
            'is_donor',
            'is_investor',
            'is_premium',
            'is_tippable',
            'is_private',
            'is_accessible',
            'bio',
            'score',
            'video_count',
            ]
def cleanUserInfo(u):
    d = {}
    for v in userValues:
        if v == 'bio':
            d[v] = json.dumps(str(u.get(v)))
        else:
            d[v] = u.get(v)

    return d

postVals = [
        'created_at',
        'revised_at',
        'edited',
        'body',
        'like_count',
        'dislike_count',
        'reply_count',
        'repost_count',
        'is_quote',
        'is_reply',
        'category',
        'nsfw',
        'language',
]

def cleanPost(p):
    d = {
        'id' : p['post']['id'],
        'published_at' : p['published_at'],
        'type': p['type'],
        'creator' : p['actuser']['username']
    }
    for v in postVals:
        if v == 'body':
            d[v] = json.dumps(str(p['post'].get(v)))
        else:
            d[v] = p['post'].get(v)

    d['orginal_auth'] = p['post']['user']['username']
    d['attachment_type'] = p['post']['attachment']['type']
    d['has_embed'] = not p['post']['embed']['html'] is None
    d['has_parent'] = bool(p['post'].get('conversation_parent', False))
    return d
