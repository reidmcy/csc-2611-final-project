import collections.abc
import json
import gzip
import re

from .hateSpeech import isHate, antiSemticRegex

hashTagRE = re.compile(r'(?=^|\s)#[1-9A-Za-z]+')
mentionsRE = re.compile(r'(?=^|\s)@[1-9A-Za-z]+')
urlRE = re.compile(r'(?:(?:https?|ftp):\/\/)?[\w/\-?=%.]+\.[a-zA-Z]{2,}(([/:])\S*)?')

newLineRe = re.compile(r'(?<=\w)\s*\n')
whiteSpaceRE = re.compile(r"[^\w#@.!'?’]+")
punctuationSpacingRE = re.compile(r'(?<=\w)([.!?])')

def cleanMessage(s):
    s = json.loads(s)
    s = urlRE.sub('', s)
    #s = newLineRe.sub('. ', s)
    s = whiteSpaceRE.sub(' ', s).strip()
    s = punctuationSpacingRE.sub(lambda x : ' {}'.format(x.group(1)), s)
    return json.dumps(s)

def getHashTags(s):
    return hashTagRE.findall(s)

def getMentions(s):
    return mentionsRE.findall(s)

def getURLs(s):
    return urlRE.findall(s)

def getHate(s):
    ret, probs = isHate(s, get_probs=True)
    return ret, json.dumps(probs)

def antisemticCount(s):
    return len(antiSemticRegex.findall(s))

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
        self._posts = None

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
        if self._posts is None:
            self._posts = [cleanPost(p) for p in self['posts']]
        return [cleanPost(p) for p in self['posts']]

    def reposts(self):
        return [p for p in self.posts() if p['type'] == 'repost']

    def userInfo(self):
        return cleanUserInfo(self['user_info'], self.posts())

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
            'created_at_month_label',
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
            'video_count'
            ]
def cleanUserInfo(u, posts):
    d = {}
    for v in userValues:
        if v == 'bio':
            d[v] = json.dumps(str(u.get(v)))
        else:
            d[v] = u.get(v)

    d['hash_tags'] = getHashTags(u.get('bio', ''))
    d['mentions'] = getMentions(u.get('bio', ''))
    d['urls'] = getURLs(u.get('bio', ''))
    d['hate_type'], d['hate_probs'] = getHate(u.get('bio', ''))
    d['antisemtic_bio_count'] = antisemticCount(u.get('bio', ''))

    d['reposts'] = list(set([p['orginal_auth'] for p in posts if p['type'] == 'repost']))
    d['has_offensive_language'] = False
    d['has_hate_speech'] = False
    for p in posts:
        if p['hate_type'] == 'offensive_language':
            d['has_offensive_language'] = True
        elif p['hate_type'] == 'hate_speech':
            d['has_hate_speech'] = True
        if d['has_hate_speech'] and d['has_offensive_language']:
            break

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
            d[v] = str(p['post'].get(v))
        else:
            d[v] = p['post'].get(v)

    d['orginal_auth'] = p['post']['user']['username']
    d['attachment_type'] = p['post']['attachment']['type']
    d['has_embed'] = not p['post']['embed']['html'] is None
    d['has_parent'] = bool(p['post'].get('conversation_parent', False))

    d['hash_tags'] = getHashTags(p['post'].get('body', ''))
    d['mentions'] = getMentions(p['post'].get('body', ''))
    d['urls'] = getURLs(p['post'].get('body', ''))
    d['hate_type'], d['hate_probs'] = getHate(p['post'].get('body', ''))
    d['antisemtic_count'] = antisemticCount(p['post'].get('body', ''))

    return d
